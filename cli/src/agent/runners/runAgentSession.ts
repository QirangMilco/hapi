import type { AgentState } from '@/api/types';
import { logger } from '@/ui/logger';
import { MessageQueue2 } from '@/utils/MessageQueue2';
import { hashObject } from '@/utils/deterministicJson';
import { AgentRegistry } from '@/agent/AgentRegistry';
import { convertAgentMessage } from '@/agent/messageConverter';
import { PermissionAdapter } from '@/agent/permissionAdapter';
import type { AgentBackend, PromptContent } from '@/agent/types';
import { startHappyServer } from '@/claude/utils/startHappyServer';
import { getHappyCliCommand } from '@/utils/spawnHappyCLI';
import { registerKillSessionHandler } from '@/claude/registerKillSessionHandler';
import { bootstrapSession } from '@/agent/sessionFactory';
import { formatMessageWithAttachments } from '@/utils/attachmentFormatter';
import type { SessionPermissionMode } from '@/api/types';
import { existsSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { configuration } from '@/configuration';

type SnowSessionLink = {
    snowSessionId: string;
    hapiSessionId: string;
    snowSseUrl?: string;
    updatedAt: number;
};

type SnowSessionLinksStore = {
    links: SnowSessionLink[];
};

const SNOW_SESSION_LINKS_FILE = join(configuration.happyHomeDir, 'snow-session-links.json');

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

function writeSnowSessionLinks(store: SnowSessionLinksStore): void {
    writeFileSync(SNOW_SESSION_LINKS_FILE, JSON.stringify(store), 'utf8');
}

function upsertSnowSessionLink(snowSessionId: string, hapiSessionId: string, snowSseUrl?: string): void {
    const store = readSnowSessionLinks();
    const now = Date.now();
    const next = store.links.filter((item) => item.snowSessionId !== snowSessionId);
    next.push({
        snowSessionId,
        hapiSessionId,
        snowSseUrl,
        updatedAt: now
    });
    next.sort((a, b) => b.updatedAt - a.updatedAt);
    writeSnowSessionLinks({ links: next.slice(0, 500) });
}

function emitReadyIfIdle(props: {
    queueSize: () => number;
    shouldExit: boolean;
    thinking: boolean;
    sendReady: () => void;
}): void {
    if (props.shouldExit) return;
    if (props.thinking) return;
    if (props.queueSize() > 0) return;
    props.sendReady();
}

function resolveSnowPermissionMode(value: unknown, fallback: SessionPermissionMode): SessionPermissionMode {
    if (value === 'default' || value === 'yolo') {
        return value;
    }
    return fallback;
}

type SnowStoredSession = {
    messages?: Array<{
        role?: string;
        content?: unknown;
        subAgentInternal?: boolean;
    }>;
};

function findSnowSessionFile(sessionId: string): string | null {
    const root = join(homedir(), '.snow', 'sessions');
    if (!existsSync(root)) {
        return null;
    }
    const target = `${sessionId}.json`;
    const stack: string[] = [root];
    while (stack.length > 0) {
        const dir = stack.pop();
        if (!dir) {
            continue;
        }
        let entries: Array<{ name: string; isDirectory: () => boolean; isFile: () => boolean }>;
        try {
            entries = readdirSync(dir, { withFileTypes: true, encoding: 'utf8' }) as Array<{ name: string; isDirectory: () => boolean; isFile: () => boolean }>;
        } catch {
            continue;
        }
        for (const entry of entries) {
            const fullPath = join(dir, entry.name);
            if (entry.isDirectory()) {
                stack.push(fullPath);
                continue;
            }
            if (entry.isFile() && entry.name === target) {
                return fullPath;
            }
        }
    }
    return null;
}

function restoreSnowHistoryToHapiSession(
    session: { sendUserMessage: (text: string) => void; sendCodexMessage: (body: unknown) => void },
    snowSessionId: string
): number {
    const filePath = findSnowSessionFile(snowSessionId);
    if (!filePath) {
        return 0;
    }
    let parsed: SnowStoredSession;
    try {
        parsed = JSON.parse(readFileSync(filePath, 'utf8')) as SnowStoredSession;
    } catch {
        return 0;
    }
    if (!Array.isArray(parsed.messages)) {
        return 0;
    }
    let restored = 0;
    for (const msg of parsed.messages) {
        if (!msg || typeof msg !== 'object' || msg.subAgentInternal === true) {
            continue;
        }
        if (msg.role === 'user' && typeof msg.content === 'string' && msg.content.trim()) {
            session.sendUserMessage(msg.content);
            restored++;
            continue;
        }
        if (msg.role === 'assistant' && typeof msg.content === 'string' && msg.content.trim()) {
            session.sendCodexMessage({
                type: 'message',
                message: msg.content
            });
            restored++;
        }
    }
    return restored;
}

export async function runAgentSession(opts: {
    agentType: string;
    startedBy?: 'runner' | 'terminal';
    resumeSessionId?: string;
    hapiSessionId?: string;
    snowBaseUrl?: string;
    workingDirectory?: string;
    yolo?: boolean;
}): Promise<void> {
    const workingDirectory = opts.workingDirectory ?? process.cwd();
    let snowPermissionMode: SessionPermissionMode = opts.yolo === true ? 'yolo' : 'default';
    const initialState: AgentState = {
        controlledByUser: false
    };
    const { session } = await bootstrapSession({
        flavor: opts.agentType,
        startedBy: opts.startedBy ?? 'terminal',
        workingDirectory,
        agentState: initialState,
        existingSessionId: opts.hapiSessionId
    });

    session.updateAgentState((currentState) => ({
        ...currentState,
        controlledByUser: false
    }));

    const messageQueue = new MessageQueue2<Record<string, never>>(() => hashObject({}));

    session.onUserMessage((message) => {
        const formattedText = formatMessageWithAttachments(message.content.text, message.content.attachments);
        messageQueue.push(formattedText, {});
    });

    const backend: AgentBackend = AgentRegistry.create(opts.agentType);
    await backend.initialize();
    const snowBackend = opts.agentType === 'snow'
        ? backend as AgentBackend & { setYoloMode?: (enabled: boolean) => void }
        : null;
    snowBackend?.setYoloMode?.(snowPermissionMode === 'yolo');

    const permissionAdapter = new PermissionAdapter(session, backend);

    const happyServer = await startHappyServer(session);
    const bridgeCommand = getHappyCliCommand(['mcp', '--url', happyServer.url]);
    const mcpServers = [
        {
            name: 'happy',
            command: bridgeCommand.command,
            args: bridgeCommand.args,
            env: []
        }
    ];

    const agentSessionId = await backend.newSession({
        cwd: workingDirectory,
        mcpServers,
        resumeSessionId: opts.resumeSessionId
    });

    if (opts.agentType === 'snow') {
        session.keepAlive(false, 'remote', { permissionMode: snowPermissionMode });
        upsertSnowSessionLink(agentSessionId, session.sessionId, opts.snowBaseUrl);
        session.updateMetadata((metadata) => ({
            ...metadata,
            snowSessionId: agentSessionId,
            snowSseUrl: opts.snowBaseUrl ?? metadata.snowSseUrl
        }));
        if (opts.resumeSessionId && !opts.hapiSessionId) {
            const restored = restoreSnowHistoryToHapiSession(session, opts.resumeSessionId);
            if (restored > 0) {
                logger.debug(`[SNOW] Restored ${restored} historical messages from ~/.snow for ${opts.resumeSessionId}`);
            }
        }
    }

    let thinking = false;
    let shouldExit = false;
    let waitAbortController: AbortController | null = null;

    session.keepAlive(thinking, 'remote', opts.agentType === 'snow' ? { permissionMode: snowPermissionMode } : undefined);
    const keepAliveInterval = setInterval(() => {
        session.keepAlive(thinking, 'remote', opts.agentType === 'snow' ? { permissionMode: snowPermissionMode } : undefined);
    }, 2000);

    const sendReady = () => {
        session.sendSessionEvent({ type: 'ready' });
    };

    const handleAbort = async () => {
        logger.debug('[ACP] Abort requested');
        await backend.cancelPrompt(agentSessionId);
        await permissionAdapter.cancelAll('User aborted');
        thinking = false;
        session.keepAlive(thinking, 'remote', opts.agentType === 'snow' ? { permissionMode: snowPermissionMode } : undefined);
        sendReady();
        if (waitAbortController) {
            waitAbortController.abort();
        }
    };

    session.rpcHandlerManager.registerHandler('abort', async () => {
        await handleAbort();
    });

    if (opts.agentType === 'snow') {
        session.rpcHandlerManager.registerHandler('set-session-config', async (payload: unknown) => {
            if (!payload || typeof payload !== 'object') {
                throw new Error('Invalid session config payload');
            }
            const config = payload as { permissionMode?: unknown };
            if (config.permissionMode !== undefined) {
                snowPermissionMode = resolveSnowPermissionMode(config.permissionMode, snowPermissionMode);
                snowBackend?.setYoloMode?.(snowPermissionMode === 'yolo');
                session.sendSessionEvent({
                    type: 'permission-mode-changed',
                    mode: snowPermissionMode
                });
                session.keepAlive(thinking, 'remote', { permissionMode: snowPermissionMode });
            }
            return { applied: { permissionMode: snowPermissionMode } };
        });
    }

    const handleKillSession = async () => {
        if (shouldExit) return;
        shouldExit = true;
        await permissionAdapter.cancelAll('Session killed');
        if (waitAbortController) {
            waitAbortController.abort();
        }
    };

    registerKillSessionHandler(session.rpcHandlerManager, handleKillSession);

    try {
        while (!shouldExit) {
            waitAbortController = new AbortController();
            const batch = await messageQueue.waitForMessagesAndGetAsString(waitAbortController.signal);
            waitAbortController = null;
            if (!batch) {
                if (shouldExit) {
                    break;
                }
                continue;
            }

            const promptContent: PromptContent[] = [{
                type: 'text',
                text: batch.message
            }];

            thinking = true;
            session.keepAlive(thinking, 'remote', opts.agentType === 'snow' ? { permissionMode: snowPermissionMode } : undefined);

            try {
                await backend.prompt(agentSessionId, promptContent, (message) => {
                    const converted = convertAgentMessage(message);
                    if (converted) {
                        session.sendCodexMessage(converted);
                    }
                });
            } catch (error) {
                logger.warn('[ACP] Prompt failed', error);
                session.sendSessionEvent({
                    type: 'message',
                    message: 'Agent prompt failed. Check logs for details.'
                });
            } finally {
                thinking = false;
                session.keepAlive(thinking, 'remote', opts.agentType === 'snow' ? { permissionMode: snowPermissionMode } : undefined);
                await permissionAdapter.cancelAll('Prompt finished');
                emitReadyIfIdle({
                    queueSize: () => messageQueue.size(),
                    shouldExit,
                    thinking,
                    sendReady
                });
            }
        }
    } finally {
        clearInterval(keepAliveInterval);
        await permissionAdapter.cancelAll('Session ended');
        session.sendSessionDeath();
        await session.flush();
        session.close();
        await backend.disconnect();
        happyServer.stop();
    }
}
