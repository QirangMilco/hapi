import chalk from 'chalk';
import { spawn, spawnSync } from 'node:child_process';
import { createConnection } from 'node:net';
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { configuration } from '@/configuration';
import { authAndSetupMachineIfNeeded } from '@/ui/auth';
import { initializeToken } from '@/ui/tokenInit';
import { maybeAutoStartServer } from '@/utils/autoStartServer';
import type { CommandDefinition } from './types';

const DEFAULT_SNOW_SSE_URL = 'http://127.0.0.1:3000';
const SNOW_STARTUP_TIMEOUT_MS = 10000;
const POLL_INTERVAL_MS = 250;
const PORT_CHECK_TIMEOUT_MS = 800;
const SNOW_CLIENT_TRACKER_FILE = join(configuration.happyHomeDir, 'snow-sse-clients.json');
const SNOW_SESSION_LINKS_FILE = join(configuration.happyHomeDir, 'snow-session-links.json');

type SnowClientTrackerState = {
    pids: number[];
};

type CliSessionResponse = {
    session?: {
        metadata?: {
            flavor?: string | null;
            snowSessionId?: string;
        } | null;
    } | null;
    error?: string;
};

type SnowSessionLink = {
    snowSessionId: string;
    hapiSessionId: string;
    updatedAt: number;
};

type SnowSessionLinksStore = {
    links: SnowSessionLink[];
};

function parseBooleanEnv(name: string): boolean {
    return ['1', 'true', 'yes'].includes((process.env[name] ?? '').trim().toLowerCase());
}

function readClientTrackerState(): SnowClientTrackerState {
    if (!existsSync(SNOW_CLIENT_TRACKER_FILE)) {
        return { pids: [] };
    }
    try {
        const raw = JSON.parse(readFileSync(SNOW_CLIENT_TRACKER_FILE, 'utf8')) as Partial<SnowClientTrackerState>;
        const pids = Array.isArray(raw.pids)
            ? raw.pids.filter((pid): pid is number => Number.isInteger(pid) && pid > 0)
            : [];
        return { pids };
    } catch {
        return { pids: [] };
    }
}

function writeClientTrackerState(state: SnowClientTrackerState): void {
    writeFileSync(SNOW_CLIENT_TRACKER_FILE, JSON.stringify(state), 'utf8');
}

function readSnowSessionLinks(): SnowSessionLinksStore {
    if (!existsSync(SNOW_SESSION_LINKS_FILE)) {
        return { links: [] };
    }
    try {
        const raw = JSON.parse(readFileSync(SNOW_SESSION_LINKS_FILE, 'utf8')) as Partial<SnowSessionLinksStore>;
        if (!Array.isArray(raw.links)) {
            return { links: [] };
        }
        const links = raw.links.filter((item): item is SnowSessionLink => {
            if (!item || typeof item !== 'object') return false;
            const record = item as Partial<SnowSessionLink>;
            return typeof record.snowSessionId === 'string'
                && typeof record.hapiSessionId === 'string'
                && typeof record.updatedAt === 'number'
                && record.snowSessionId.length > 0
                && record.hapiSessionId.length > 0;
        });
        return { links };
    } catch {
        return { links: [] };
    }
}

function resolveHapiSessionIdFromSnowSessionId(snowSessionId: string): string | null {
    const matches = readSnowSessionLinks().links
        .filter((item) => item.snowSessionId === snowSessionId)
        .sort((a, b) => b.updatedAt - a.updatedAt);
    if (matches.length === 0) {
        return null;
    }
    return matches[0].hapiSessionId;
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
    if (hostname === 'localhost' || hostname === '::1' || hostname === '0.0.0.0' || hostname === '::') {
        return '127.0.0.1';
    }
    return hostname;
}

function shouldAutoStopSnowSse(): boolean {
    return parseBooleanEnv('HAPI_SNOW_SSE_AUTO_STOP_WHEN_IDLE');
}

function stopSnowSseByPort(port: number): void {
    spawnSync('snow', ['--sse-stop', '--sse-port', String(port)], { stdio: 'ignore' });
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
    const alive = state.pids.filter((pid) => pid !== currentPid && isPidAlive(pid));
    alive.push(currentPid);
    writeClientTrackerState({ pids: Array.from(new Set(alive)) });

    let cleaned = false;
    return () => {
        if (cleaned) {
            return;
        }
        cleaned = true;
        const latest = readClientTrackerState();
        const remaining = latest.pids.filter((pid) => pid !== currentPid && isPidAlive(pid));
        writeClientTrackerState({ pids: remaining });
        if (remaining.length === 0) {
            stopSnowSseByPort(port);
        }
    };
}

function buildSnowUrlFromListenEnv(): string {
    const host = (process.env.HAPI_SNOW_SSE_LISTEN_HOST ?? '127.0.0.1').trim();
    const portRaw = (process.env.HAPI_SNOW_SSE_LISTEN_PORT ?? '3000').trim();
    const port = Number(portRaw);
    if (!Number.isInteger(port) || port <= 0 || port > 65535) {
        throw new Error(`Invalid HAPI_SNOW_SSE_LISTEN_PORT: ${portRaw}`);
    }
    const normalizedHost = host === '0.0.0.0' || host === '::' ? '127.0.0.1' : host;
    const hostForUrl = normalizedHost.includes(':') && !normalizedHost.startsWith('[') ? `[${normalizedHost}]` : normalizedHost;
    return `http://${hostForUrl}:${port}`;
}

function resolveSnowBaseUrl(sseUrl?: string): string {
    const value = (sseUrl ?? process.env.HAPI_SNOW_SSE_URL ?? buildSnowUrlFromListenEnv() ?? DEFAULT_SNOW_SSE_URL).trim();
    return value.replace(/\/+$/, '');
}

async function resolveSnowSessionIdFromHapiSession(hapiSessionId: string): Promise<string> {
    const token = configuration.cliApiToken.trim();
    if (!token) {
        throw new Error('CLI_API_TOKEN is missing');
    }

    const response = await fetch(`${configuration.apiUrl}/cli/sessions/${encodeURIComponent(hapiSessionId)}`, {
        headers: {
            Authorization: `Bearer ${token}`,
            'Content-Type': 'application/json'
        },
        signal: AbortSignal.timeout(5000)
    });

    const payload = await response.json().catch(() => ({})) as CliSessionResponse;
    if (!response.ok) {
        throw new Error(payload.error || `Failed to load HAPI session ${hapiSessionId}`);
    }

    const metadata = payload.session?.metadata;
    if (!metadata) {
        throw new Error(`HAPI session ${hapiSessionId} has no metadata`);
    }
    if (metadata.flavor && metadata.flavor !== 'snow') {
        throw new Error(`HAPI session ${hapiSessionId} is not a snow session`);
    }
    if (!metadata.snowSessionId || !metadata.snowSessionId.trim()) {
        throw new Error(`HAPI session ${hapiSessionId} has no snowSessionId`);
    }
    return metadata.snowSessionId.trim();
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
    return url.protocol === 'https:' ? 443 : 80;
}

function isLocalHost(hostname: string): boolean {
    return hostname === '127.0.0.1' || hostname === 'localhost' || hostname === '::1' || hostname === '0.0.0.0' || hostname === '::';
}

async function checkPortListening(port: number, host: string): Promise<boolean> {
    return new Promise((resolve) => {
        const socket = createConnection({ port, host });
        const cleanup = () => {
            socket.removeAllListeners();
            socket.destroy();
        };

        socket.setTimeout(PORT_CHECK_TIMEOUT_MS);
        socket.on('connect', () => {
            cleanup();
            resolve(true);
        });
        socket.on('error', () => {
            cleanup();
            resolve(false);
        });
        socket.on('timeout', () => {
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
        await new Promise(resolve => setTimeout(resolve, POLL_INTERVAL_MS));
    }
    return false;
}

async function startSnowDaemon(port: number): Promise<void> {
    const args = ['--sse-daemon', '--sse-port', String(port)];
    const workDir = process.env.HAPI_SNOW_SSE_WORK_DIR?.trim();
    if (workDir) {
        args.push('--work-dir', workDir);
    }
    await new Promise<void>((resolve, reject) => {
        const child = spawn('snow', args, {
            stdio: 'ignore',
        });

        child.once('error', (error) => {
            if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
                reject(new Error('Cannot find `snow` command on PATH'));
                return;
            }
            reject(error);
        });

        child.once('exit', (code) => {
            if (code === 0) {
                resolve();
                return;
            }
            reject(new Error(`snow --sse-daemon exited with code ${code ?? 'null'}`));
        });
    });
}

async function ensureSnowSseReady(baseUrl: string): Promise<void> {
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
        console.log(chalk.gray(`Starting Snow SSE daemon on ${host}:${port}...`));
        await startSnowDaemon(port);
    }

    const ready = await waitForSnowReady(baseUrl);
    if (!ready) {
        throw new Error(`Snow SSE is not ready at ${baseUrl}. Please run \`snow --sse --sse-port ${port}\` manually.`);
    }
}

export const snowCommand: CommandDefinition = {
    name: 'snow',
    requiresRuntimeAssets: true,
    run: async ({ commandArgs }) => {
        try {
            const options: {
                startedBy?: 'runner' | 'terminal';
                yolo?: boolean;
                sseUrl?: string;
                resumeSessionId?: string;
                resumeFromHapiSessionId?: string;
                hapiSessionId?: string;
            } = {};

            for (let i = 0; i < commandArgs.length; i++) {
                const arg = commandArgs[i];
                if (arg === '--started-by') {
                    options.startedBy = commandArgs[++i] as 'runner' | 'terminal';
                } else if (arg === '--yolo') {
                    options.yolo = true;
                } else if (arg === '--sse-url') {
                    const value = commandArgs[++i];
                    if (!value) {
                        throw new Error('Missing --sse-url value');
                    }
                    options.sseUrl = value;
                } else if (arg === '--resume' || arg === '--resume-snow') {
                    const value = commandArgs[++i];
                    if (!value) {
                        throw new Error(`Missing ${arg} value`);
                    }
                    options.resumeSessionId = value;
                } else if (arg === '--resume-hapi') {
                    const value = commandArgs[++i];
                    if (!value) {
                        throw new Error('Missing --resume-hapi value');
                    }
                    options.resumeFromHapiSessionId = value;
                }
            }

            if (options.resumeSessionId && options.resumeFromHapiSessionId) {
                throw new Error('Use only one of --resume or --resume-hapi');
            }

            await initializeToken();
            await maybeAutoStartServer();
            await authAndSetupMachineIfNeeded();
            if (options.resumeFromHapiSessionId) {
                options.resumeSessionId = await resolveSnowSessionIdFromHapiSession(options.resumeFromHapiSessionId);
                options.hapiSessionId = options.resumeFromHapiSessionId;
            } else if (options.resumeSessionId) {
                options.hapiSessionId = resolveHapiSessionIdFromSnowSessionId(options.resumeSessionId) ?? undefined;
                if (!options.hapiSessionId) {
                    console.log(chalk.gray(`No linked HAPI session found for Snow session ${options.resumeSessionId}, creating new HAPI session.`));
                }
            }
            const snowBaseUrl = resolveSnowBaseUrl(options.sseUrl);
            await ensureSnowSseReady(snowBaseUrl);
            const cleanupClientTracker = registerSnowClientTracker(snowBaseUrl);
            process.once('exit', cleanupClientTracker);

            const { registerSnowAgent } = await import('@/agent/runners/snow');
            const { runAgentSession } = await import('@/agent/runners/runAgentSession');
            registerSnowAgent({
                baseUrl: snowBaseUrl,
                yolo: options.yolo === true
            });
            await runAgentSession({
                agentType: 'snow',
                startedBy: options.startedBy,
                resumeSessionId: options.resumeSessionId,
                hapiSessionId: options.hapiSessionId
            });
        } catch (error) {
            console.error(chalk.red('Error:'), error instanceof Error ? error.message : 'Unknown error');
            if (process.env.DEBUG) {
                console.error(error);
            }
            process.exit(1);
        }
    }
};
