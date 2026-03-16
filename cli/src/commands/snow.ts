import chalk from "chalk";
import { spawn, spawnSync } from "node:child_process";
import { createConnection, createServer } from "node:net";
import { existsSync, readFileSync, realpathSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { configuration } from "@/configuration";
import { authAndSetupMachineIfNeeded } from "@/ui/auth";
import { initializeToken } from "@/ui/tokenInit";
import { maybeAutoStartServer } from "@/utils/autoStartServer";
import { getUnknownErrorMessage } from "@/utils/errorUtils";
import { registerSnowAgent } from "@/agent/runners/snow";
import { runAgentSession } from "@/agent/runners/runAgentSession";
import type { CommandDefinition } from "./types";

const DEFAULT_SNOW_SSE_HOST = "127.0.0.1";
const SNOW_STARTUP_TIMEOUT_MS = 10000;
const POLL_INTERVAL_MS = 250;
const PORT_CHECK_TIMEOUT_MS = 800;
const SNOW_CLIENT_TRACKER_FILE = join(
  configuration.happyHomeDir,
  "snow-sse-clients.json"
);
const SNOW_SESSION_LINKS_FILE = join(
  configuration.happyHomeDir,
  "snow-session-links.json"
);
const SNOW_SSE_INSTANCES_FILE = join(
  configuration.happyHomeDir,
  "snow-sse-instances.json"
);

type SnowClientTrackerState = {
  instances: Array<{
    baseUrl: string;
    pids: number[];
  }>;
};

type CliSessionResponse = {
  session?: {
    metadata?: {
      flavor?: string | null;
      snowSessionId?: string;
      snowSseUrl?: string;
    } | null;
  } | null;
  error?: string;
};

type SnowSessionLink = {
  snowSessionId: string;
  hapiSessionId: string;
  snowSseUrl?: string;
  updatedAt: number;
};

type SnowSessionLinksStore = {
  links: SnowSessionLink[];
};

type SnowSseInstanceRecord = {
  projectDir: string;
  baseUrl: string;
  port: number;
  host: string;
  updatedAt: number;
};

type SnowSseInstancesStore = {
  instances: SnowSseInstanceRecord[];
};

function parseBooleanEnv(name: string): boolean {
  return ["1", "true", "yes"].includes(
    (process.env[name] ?? "").trim().toLowerCase()
  );
}

function readClientTrackerState(): SnowClientTrackerState {
  if (!existsSync(SNOW_CLIENT_TRACKER_FILE)) {
    return { instances: [] };
  }
  try {
    const raw = JSON.parse(
      readFileSync(SNOW_CLIENT_TRACKER_FILE, "utf8")
    ) as Partial<SnowClientTrackerState>;
    const instances = Array.isArray(raw.instances)
      ? raw.instances
          .map((item) => {
            if (!item || typeof item !== "object") return null;
            const record = item as Partial<
              SnowClientTrackerState["instances"][number]
            >;
            if (typeof record.baseUrl !== "string") return null;
            const pids = Array.isArray(record.pids)
              ? record.pids.filter(
                  (pid): pid is number => Number.isInteger(pid) && pid > 0
                )
              : [];
            return { baseUrl: record.baseUrl, pids };
          })
          .filter(
            (item): item is SnowClientTrackerState["instances"][number] =>
              item !== null
          )
      : [];
    return { instances };
  } catch {
    return { instances: [] };
  }
}

function writeClientTrackerState(state: SnowClientTrackerState): void {
  writeFileSync(SNOW_CLIENT_TRACKER_FILE, JSON.stringify(state), "utf8");
}

function readSnowSessionLinks(): SnowSessionLinksStore {
  if (!existsSync(SNOW_SESSION_LINKS_FILE)) {
    return { links: [] };
  }
  try {
    const raw = JSON.parse(
      readFileSync(SNOW_SESSION_LINKS_FILE, "utf8")
    ) as Partial<SnowSessionLinksStore>;
    if (!Array.isArray(raw.links)) {
      return { links: [] };
    }
    const links = raw.links.filter((item): item is SnowSessionLink => {
      if (!item || typeof item !== "object") return false;
      const record = item as Partial<SnowSessionLink>;
      return (
        typeof record.snowSessionId === "string" &&
        typeof record.hapiSessionId === "string" &&
        typeof record.updatedAt === "number" &&
        record.snowSessionId.length > 0 &&
        record.hapiSessionId.length > 0
      );
    });
    return { links };
  } catch {
    return { links: [] };
  }
}

function resolveSnowLinkFromSnowSessionId(
  snowSessionId: string
): { hapiSessionId: string; snowSseUrl?: string } | null {
  const matches = readSnowSessionLinks()
    .links.filter((item) => item.snowSessionId === snowSessionId)
    .sort((a, b) => b.updatedAt - a.updatedAt);
  if (matches.length === 0) {
    return null;
  }
  return {
    hapiSessionId: matches[0].hapiSessionId,
    snowSseUrl: matches[0].snowSseUrl,
  };
}

function isPidAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function normalizeHostForConnection(hostname: string): string {
  if (
    hostname === "localhost" ||
    hostname === "::1" ||
    hostname === "0.0.0.0" ||
    hostname === "::"
  ) {
    return "127.0.0.1";
  }
  return hostname;
}

function shouldAutoStopSnowSse(): boolean {
  return parseBooleanEnv("HAPI_SNOW_SSE_AUTO_STOP_WHEN_IDLE");
}

function stopSnowSseByPort(port: number): void {
  spawnSync("snow", ["--sse-stop", "--sse-port", String(port)], {
    stdio: "ignore",
  });
}

function registerSnowClientTracker(baseUrl: string): () => void {
  if (!shouldAutoStopSnowSse()) {
    return () => {};
  }
  const parsedUrl = parseSnowUrl(baseUrl);
  if (!isLocalHost(parsedUrl.hostname)) {
    return () => {};
  }
  const currentPid = process.pid;
  const port = getSnowPort(parsedUrl);

  const state = readClientTrackerState();
  const nextInstances = state.instances.filter(
    (item) => item.baseUrl !== baseUrl
  );
  const existing = state.instances.find((item) => item.baseUrl === baseUrl);
  const alive = (existing?.pids ?? []).filter(
    (pid) => pid !== currentPid && isPidAlive(pid)
  );
  alive.push(currentPid);
  nextInstances.push({ baseUrl, pids: Array.from(new Set(alive)) });
  writeClientTrackerState({ instances: nextInstances });

  let cleaned = false;
  return () => {
    if (cleaned) {
      return;
    }
    cleaned = true;
    const latest = readClientTrackerState();
    const persisted = latest.instances.find((item) => item.baseUrl === baseUrl);
    const remaining = (persisted?.pids ?? []).filter(
      (pid) => pid !== currentPid && isPidAlive(pid)
    );
    const survivors = latest.instances.filter(
      (item) => item.baseUrl !== baseUrl
    );
    if (remaining.length > 0) {
      survivors.push({ baseUrl, pids: remaining });
    }
    writeClientTrackerState({ instances: survivors });
    if (remaining.length === 0) {
      stopSnowSseByPort(port);
    }
  };
}

function normalizeProjectDir(input: string): string {
  const resolved = resolve(input);
  try {
    return realpathSync(resolved);
  } catch {
    return resolved;
  }
}

function resolveSnowProjectDir(): string {
  const raw = process.env.HAPI_SNOW_SSE_WORK_DIR?.trim();
  return normalizeProjectDir(raw && raw.length > 0 ? raw : process.cwd());
}

function readSnowSseInstances(): SnowSseInstancesStore {
  if (!existsSync(SNOW_SSE_INSTANCES_FILE)) {
    return { instances: [] };
  }
  try {
    const raw = JSON.parse(
      readFileSync(SNOW_SSE_INSTANCES_FILE, "utf8")
    ) as Partial<SnowSseInstancesStore>;
    if (!Array.isArray(raw.instances)) {
      return { instances: [] };
    }
    const instances = raw.instances.filter(
      (item): item is SnowSseInstanceRecord => {
        if (!item || typeof item !== "object") return false;
        const record = item as Partial<SnowSseInstanceRecord>;
        return (
          typeof record.projectDir === "string" &&
          typeof record.baseUrl === "string" &&
          typeof record.port === "number" &&
          typeof record.host === "string" &&
          typeof record.updatedAt === "number" &&
          record.projectDir.length > 0 &&
          record.baseUrl.length > 0 &&
          record.port > 0 &&
          record.port <= 65535
        );
      }
    );
    return { instances };
  } catch {
    return { instances: [] };
  }
}

function writeSnowSseInstances(store: SnowSseInstancesStore): void {
  writeFileSync(SNOW_SSE_INSTANCES_FILE, JSON.stringify(store), "utf8");
}

function upsertSnowSseInstance(
  record: Omit<SnowSseInstanceRecord, "updatedAt">
): void {
  const store = readSnowSseInstances();
  const now = Date.now();
  const next = store.instances.filter(
    (item) =>
      item.projectDir !== record.projectDir && item.baseUrl !== record.baseUrl
  );
  next.push({ ...record, updatedAt: now });
  next.sort((a, b) => b.updatedAt - a.updatedAt);
  writeSnowSseInstances({ instances: next.slice(0, 200) });
}

function resolveSnowBaseUrl(sseUrl?: string): string | null {
  const value = (sseUrl ?? process.env.HAPI_SNOW_SSE_URL ?? "").trim();
  if (!value) {
    return null;
  }
  return value.replace(/\/+$/, "");
}

function buildLocalSnowBaseUrl(host: string, port: number): string {
  const hostForUrl =
    host.includes(":") && !host.startsWith("[") ? `[${host}]` : host;
  return `http://${hostForUrl}:${port}`;
}

async function resolveSnowSessionFromHapiSession(
  hapiSessionId: string
): Promise<{ snowSessionId: string; snowSseUrl?: string }> {
  const token = configuration.cliApiToken.trim();
  if (!token) {
    throw new Error("CLI_API_TOKEN is missing");
  }

  const response = await fetch(
    `${configuration.apiUrl}/cli/sessions/${encodeURIComponent(hapiSessionId)}`,
    {
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
      },
      signal: AbortSignal.timeout(5000),
    }
  );

  const payload = (await response
    .json()
    .catch(() => ({}))) as CliSessionResponse;
  if (!response.ok) {
    throw new Error(
      payload.error || `Failed to load HAPI session ${hapiSessionId}`
    );
  }

  const metadata = payload.session?.metadata;
  if (!metadata) {
    throw new Error(`HAPI session ${hapiSessionId} has no metadata`);
  }
  if (metadata.flavor && metadata.flavor !== "snow") {
    throw new Error(`HAPI session ${hapiSessionId} is not a snow session`);
  }
  if (!metadata.snowSessionId || !metadata.snowSessionId.trim()) {
    throw new Error(`HAPI session ${hapiSessionId} has no snowSessionId`);
  }
  return {
    snowSessionId: metadata.snowSessionId.trim(),
    snowSseUrl:
      typeof metadata.snowSseUrl === "string" && metadata.snowSseUrl.trim()
        ? metadata.snowSseUrl.trim().replace(/\/+$/, "")
        : undefined,
  };
}

async function resolveExistingHapiSessionId(
  hapiSessionId?: string
): Promise<string | undefined> {
  const candidate = hapiSessionId?.trim();
  if (!candidate) {
    return undefined;
  }
  const token = configuration.cliApiToken.trim();
  if (!token) {
    return undefined;
  }
  const response = await fetch(
    `${configuration.apiUrl}/cli/sessions/${encodeURIComponent(candidate)}`,
    {
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
      },
      signal: AbortSignal.timeout(5000),
    }
  );
  if (!response.ok) {
    return undefined;
  }
  return candidate;
}

function parseSnowUrl(baseUrl: string): URL {
  try {
    return new URL(baseUrl);
  } catch {
    throw new Error(`Invalid Snow SSE URL: ${baseUrl}`);
  }
}

function getSnowPort(url: URL): number {
  if (url.port) {
    return Number(url.port);
  }
  return url.protocol === "https:" ? 443 : 80;
}

function isLocalHost(hostname: string): boolean {
  return (
    hostname === "127.0.0.1" ||
    hostname === "localhost" ||
    hostname === "::1" ||
    hostname === "0.0.0.0" ||
    hostname === "::"
  );
}

async function checkPortListening(
  port: number,
  host: string
): Promise<boolean> {
  return new Promise((resolve) => {
    const socket = createConnection({ port, host });
    const cleanup = () => {
      socket.removeAllListeners();
      socket.destroy();
    };

    socket.setTimeout(PORT_CHECK_TIMEOUT_MS);
    socket.on("connect", () => {
      cleanup();
      resolve(true);
    });
    socket.on("error", () => {
      cleanup();
      resolve(false);
    });
    socket.on("timeout", () => {
      cleanup();
      resolve(false);
    });
  });
}

async function checkSnowHealth(baseUrl: string): Promise<boolean> {
  try {
    const response = await fetch(`${baseUrl}/health`, {
      signal: AbortSignal.timeout(1000),
    });
    return response.ok;
  } catch {
    return false;
  }
}

async function waitForSnowReady(baseUrl: string): Promise<boolean> {
  const start = Date.now();
  while (Date.now() - start < SNOW_STARTUP_TIMEOUT_MS) {
    if (await checkSnowHealth(baseUrl)) {
      return true;
    }
    await new Promise((resolve) => setTimeout(resolve, POLL_INTERVAL_MS));
  }
  return false;
}

async function findFreePort(host: string): Promise<number> {
  return await new Promise<number>((resolvePort, reject) => {
    const server = createServer();
    server.unref();
    server.once("error", reject);
    server.listen(0, host, () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        server.close(() => reject(new Error("Failed to allocate free port")));
        return;
      }
      const { port } = address;
      server.close((error) => {
        if (error) {
          reject(error);
          return;
        }
        resolvePort(port);
      });
    });
  });
}

async function startSnowDaemon(port: number, workDir: string): Promise<void> {
  const args = ["--sse-daemon", "--sse-port", String(port)];
  args.push("--work-dir", workDir);
  await new Promise<void>((resolve, reject) => {
    const child = spawn("snow", args, {
      stdio: "ignore",
    });

    child.once("error", (error) => {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") {
        reject(new Error("Cannot find `snow` command on PATH"));
        return;
      }
      reject(error);
    });

    child.once("exit", (code) => {
      if (code === 0) {
        resolve();
        return;
      }
      reject(new Error(`snow --sse-daemon exited with code ${code ?? "null"}`));
    });
  });
}

async function ensureSnowSseReady(
  baseUrl: string,
  workDir: string
): Promise<void> {
  if (await checkSnowHealth(baseUrl)) {
    return;
  }

  const parsedUrl = parseSnowUrl(baseUrl);
  if (!isLocalHost(parsedUrl.hostname)) {
    throw new Error(`Unable to connect Snow SSE at ${baseUrl}`);
  }

  const port = getSnowPort(parsedUrl);
  const host = normalizeHostForConnection(parsedUrl.hostname);
  const listening = await checkPortListening(port, host);
  if (!listening) {
    console.log(
      chalk.gray(
        `Starting Snow SSE daemon on ${host}:${port} for ${workDir}...`
      )
    );
    await startSnowDaemon(port, workDir);
  }

  const ready = await waitForSnowReady(baseUrl);
  if (!ready) {
    throw new Error(
      `Snow SSE is not ready at ${baseUrl}. Please run \`snow --sse --sse-port ${port}\` manually.`
    );
  }
}

async function ensureProjectSnowInstance(projectDir: string): Promise<string> {
  const host = DEFAULT_SNOW_SSE_HOST;
  const normalizedProjectDir = normalizeProjectDir(projectDir);
  const store = readSnowSseInstances();
  const existing = store.instances
    .filter((item) => item.projectDir === normalizedProjectDir)
    .sort((a, b) => b.updatedAt - a.updatedAt)[0];

  if (existing) {
    if (await checkSnowHealth(existing.baseUrl)) {
      upsertSnowSseInstance({
        projectDir: normalizedProjectDir,
        baseUrl: existing.baseUrl,
        host: existing.host,
        port: existing.port,
      });
      return existing.baseUrl;
    }
    const listening = await checkPortListening(
      existing.port,
      normalizeHostForConnection(existing.host)
    );
    if (!listening) {
      await ensureSnowSseReady(existing.baseUrl, normalizedProjectDir);
      upsertSnowSseInstance({
        projectDir: normalizedProjectDir,
        baseUrl: existing.baseUrl,
        host: existing.host,
        port: existing.port,
      });
      return existing.baseUrl;
    }
  }

  const port = await findFreePort(host);
  const baseUrl = buildLocalSnowBaseUrl(host, port);
  await ensureSnowSseReady(baseUrl, normalizedProjectDir);
  upsertSnowSseInstance({
    projectDir: normalizedProjectDir,
    baseUrl,
    host,
    port,
  });
  return baseUrl;
}

export const snowCommand: CommandDefinition = {
  name: "snow",
  requiresRuntimeAssets: true,
  run: async ({ commandArgs }) => {
    try {
      const options: {
        startedBy?: "runner" | "terminal";
        yolo?: boolean;
        sseUrl?: string;
        resumeSessionId?: string;
        resumeFromHapiSessionId?: string;
        hapiSessionId?: string;
        projectDir?: string;
      } = {};

      for (let i = 0; i < commandArgs.length; i++) {
        const arg = commandArgs[i];
        if (arg === "--started-by") {
          options.startedBy = commandArgs[++i] as "runner" | "terminal";
        } else if (arg === "--yolo") {
          options.yolo = true;
        } else if (arg === "--sse-url") {
          const value = commandArgs[++i];
          if (!value) {
            throw new Error("Missing --sse-url value");
          }
          options.sseUrl = value;
        } else if (arg === "--resume" || arg === "--resume-snow") {
          const value = commandArgs[++i];
          if (!value) {
            throw new Error(`Missing ${arg} value`);
          }
          options.resumeSessionId = value;
        } else if (arg === "--resume-hapi") {
          const value = commandArgs[++i];
          if (!value) {
            throw new Error("Missing --resume-hapi value");
          }
          options.resumeFromHapiSessionId = value;
        }
      }

      if (options.resumeSessionId && options.resumeFromHapiSessionId) {
        throw new Error("Use only one of --resume or --resume-hapi");
      }

      await initializeToken();
      await maybeAutoStartServer();
      await authAndSetupMachineIfNeeded();
      options.projectDir = resolveSnowProjectDir();
      if (options.resumeFromHapiSessionId) {
        const resolved = await resolveSnowSessionFromHapiSession(
          options.resumeFromHapiSessionId
        );
        options.resumeSessionId = resolved.snowSessionId;
        options.hapiSessionId = options.resumeFromHapiSessionId;
        options.sseUrl = options.sseUrl ?? resolved.snowSseUrl;
      } else if (options.resumeSessionId) {
        const linked = resolveSnowLinkFromSnowSessionId(
          options.resumeSessionId
        );
        options.hapiSessionId = await resolveExistingHapiSessionId(
          linked?.hapiSessionId
        );
        options.sseUrl = options.sseUrl ?? linked?.snowSseUrl;
        if (!options.hapiSessionId) {
          console.log(
            chalk.gray(
              `No linked HAPI session found for Snow session ${options.resumeSessionId}, creating new HAPI session.`
            )
          );
        }
      }
      let snowBaseUrl = resolveSnowBaseUrl(options.sseUrl);
      if (!snowBaseUrl) {
        snowBaseUrl = await ensureProjectSnowInstance(options.projectDir);
      } else {
        await ensureSnowSseReady(snowBaseUrl, options.projectDir);
      }
      const cleanupClientTracker = registerSnowClientTracker(snowBaseUrl);
      process.once("exit", cleanupClientTracker);
      registerSnowAgent({
        baseUrl: snowBaseUrl,
        yolo: options.yolo === true,
      });

      await runAgentSession({
        agentType: "snow",
        startedBy: options.startedBy,
        resumeSessionId: options.resumeSessionId,
        hapiSessionId: options.hapiSessionId,
        snowBaseUrl,
        workingDirectory: options.projectDir,
        yolo: options.yolo === true,
      });
    } catch (error) {
      console.error(chalk.red("Error:"), getUnknownErrorMessage(error));
      if (process.env.DEBUG) {
        console.error(error);
      }
      process.exit(1);
    }
  },
};
