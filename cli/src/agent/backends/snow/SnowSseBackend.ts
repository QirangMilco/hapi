import { randomUUID } from 'node:crypto';
import type {
    AgentBackend,
    AgentMessage,
    AgentSessionConfig,
    PermissionOption,
    PermissionRequest,
    PermissionResponse,
    PromptContent
} from '@/agent/types';

type SnowSseEvent = {
    type: string;
    data?: any;
    timestamp?: string;
    requestId?: string;
};

type PendingPrompt = {
    sessionId: string;
    onUpdate: (msg: AgentMessage) => void;
    resolve: () => void;
    reject: (error: Error) => void;
};

type PendingPermission = {
    optionMap: Map<string, any>;
};

export class SnowSseBackend implements AgentBackend {
    private readonly baseUrl: string;
    private yoloMode: boolean;
    private permissionHandler: ((request: PermissionRequest) => void) | null = null;
    private streamAbortController: AbortController | null = null;
    private streamReadyPromise: Promise<void> | null = null;
    private streamReadyResolve: (() => void) | null = null;
    private streamReadyReject: ((error: Error) => void) | null = null;
    private pendingPrompt: PendingPrompt | null = null;
    private pendingPermissions = new Map<string, PendingPermission>();
    private latestSessionId: string | null = null;
    private lastToolCallId: string | null = null;

    constructor(options?: { baseUrl?: string; yoloMode?: boolean }) {
        this.baseUrl = (options?.baseUrl ?? process.env.HAPI_SNOW_SSE_URL ?? 'http://127.0.0.1:3000').replace(/\/+$/, '');
        this.yoloMode = options?.yoloMode === true;
    }

    setYoloMode(enabled: boolean): void {
        this.yoloMode = enabled;
    }

    async initialize(): Promise<void> {
        if (this.streamAbortController) {
            return;
        }

        this.streamAbortController = new AbortController();
        this.streamReadyPromise = new Promise<void>((resolve, reject) => {
            this.streamReadyResolve = resolve;
            this.streamReadyReject = reject;
        });

        void this.startEventStream(this.streamAbortController.signal);
        await this.streamReadyPromise;
    }

    async newSession(config: AgentSessionConfig): Promise<string> {
        const payload = config.resumeSessionId
            ? await this.postJson('/session/load', { sessionId: config.resumeSessionId })
            : await this.postJson('/session/create', {});
        const sessionId = payload?.session?.id;
        if (typeof sessionId !== 'string' || sessionId.length === 0) {
            throw new Error(config.resumeSessionId ? 'Snow SSE /session/load 返回的 sessionId 无效' : 'Snow SSE /session/create 返回的 sessionId 无效');
        }
        this.latestSessionId = sessionId;
        return sessionId;
    }

    async prompt(
        sessionId: string,
        content: PromptContent[],
        onUpdate: (msg: AgentMessage) => void
    ): Promise<void> {
        if (!this.streamAbortController) {
            throw new Error('Snow SSE 未初始化');
        }
        if (this.pendingPrompt) {
            throw new Error('Snow SSE 当前已有执行中的请求');
        }

        const text = content
            .filter((item) => item.type === 'text')
            .map((item) => item.text)
            .join('\n')
            .trim();

        if (!text) {
            return;
        }

        await new Promise<void>((resolve, reject) => {
            this.pendingPrompt = {
                sessionId,
                onUpdate,
                resolve,
                reject
            };
            void this.postJson('/message', {
                type: 'chat',
                content: text,
                sessionId,
                yoloMode: this.yoloMode
            }).catch((error) => {
                const pending = this.pendingPrompt;
                this.pendingPrompt = null;
                const normalized = error instanceof Error ? error : new Error(String(error));
                pending?.reject(normalized);
            });
        });
    }

    async cancelPrompt(sessionId: string): Promise<void> {
        await this.postJson('/message', {
            type: 'abort',
            sessionId
        });
    }

    async respondToPermission(
        _sessionId: string,
        request: PermissionRequest,
        response: PermissionResponse
    ): Promise<void> {
        const pending = this.pendingPermissions.get(request.id);
        if (!pending) {
            return;
        }

        this.pendingPermissions.delete(request.id);
        const decision = this.resolvePermissionDecision(pending.optionMap, response);
        await this.postJson('/message', {
            type: 'tool_confirmation_response',
            requestId: request.id,
            response: decision
        });
    }

    onPermissionRequest(handler: (request: PermissionRequest) => void): void {
        this.permissionHandler = handler;
    }

    async disconnect(): Promise<void> {
        if (this.pendingPrompt) {
            this.pendingPrompt.reject(new Error('Snow SSE 连接已断开'));
            this.pendingPrompt = null;
        }
        this.pendingPermissions.clear();
        this.lastToolCallId = null;

        const controller = this.streamAbortController;
        this.streamAbortController = null;
        this.streamReadyPromise = null;
        this.streamReadyResolve = null;
        this.streamReadyReject = null;
        if (controller) {
            controller.abort();
        }
    }

    private async startEventStream(signal: AbortSignal): Promise<void> {
        try {
            const response = await fetch(`${this.baseUrl}/events`, {
                method: 'GET',
                headers: { Accept: 'text/event-stream' },
                signal
            });
            if (!response.ok || !response.body) {
                throw new Error(`Snow SSE 连接失败: HTTP ${response.status}`);
            }

            const decoder = new TextDecoder();
            const reader = response.body.getReader();
            let buffer = '';

            while (true) {
                const { done, value } = await reader.read();
                if (done) {
                    break;
                }
                buffer += decoder.decode(value, { stream: true });

                while (true) {
                    const delimiterIndex = buffer.indexOf('\n\n');
                    if (delimiterIndex === -1) {
                        break;
                    }
                    const rawEvent = buffer.slice(0, delimiterIndex);
                    buffer = buffer.slice(delimiterIndex + 2);
                    const parsed = this.parseSseData(rawEvent);
                    if (parsed) {
                        this.handleSseEvent(parsed);
                    }
                }
            }
        } catch (error) {
            if (signal.aborted) {
                return;
            }
            const normalized = error instanceof Error ? error : new Error(String(error));
            if (this.streamReadyReject) {
                this.streamReadyReject(normalized);
                this.streamReadyReject = null;
                this.streamReadyResolve = null;
            }
            if (this.pendingPrompt) {
                this.pendingPrompt.reject(normalized);
                this.pendingPrompt = null;
            }
        }
    }

    private parseSseData(rawEvent: string): SnowSseEvent | null {
        const lines = rawEvent.split('\n');
        const dataLines: string[] = [];
        for (const line of lines) {
            if (line.startsWith('data:')) {
                dataLines.push(line.slice(5).trimStart());
            }
        }
        if (dataLines.length === 0) {
            return null;
        }
        const payload = dataLines.join('\n');
        try {
            return JSON.parse(payload) as SnowSseEvent;
        } catch {
            return null;
        }
    }

    private handleSseEvent(event: SnowSseEvent): void {
        if (event.type === 'connected') {
            if (this.streamReadyResolve) {
                this.streamReadyResolve();
                this.streamReadyResolve = null;
                this.streamReadyReject = null;
            }
            return;
        }

        if (event.type === 'error') {
            const message = typeof event.data?.message === 'string' ? event.data.message : 'Snow SSE 执行出错';
            const pending = this.pendingPrompt;
            if (pending) {
                pending.onUpdate({ type: 'error', message });
                pending.reject(new Error(message));
                this.pendingPrompt = null;
            }
            return;
        }

        if (event.type === 'tool_confirmation_request') {
            this.handleToolConfirmationRequest(event);
            return;
        }

        if (event.type === 'user_question_request') {
            void this.handleUserQuestionRequest(event);
            return;
        }

        const pending = this.pendingPrompt;
        if (!pending) {
            return;
        }

        if (event.type === 'message') {
            const text = typeof event.data?.content === 'string' ? event.data.content : '';
            if (text) {
                pending.onUpdate({ type: 'text', text });
            }
            return;
        }

        if (event.type === 'tool_call') {
            const toolCall = event.data ?? {};
            const toolId = this.extractToolCallId(toolCall);
            const toolName = this.extractToolName(toolCall);
            const toolInput = this.extractToolInput(toolCall);
            this.lastToolCallId = toolId;
            pending.onUpdate({
                type: 'tool_call',
                id: toolId,
                name: toolName,
                input: toolInput,
                status: 'pending'
            });
            return;
        }

        if (event.type === 'tool_result') {
            const toolId = this.extractToolResultId(event.data);
            const success = event.data?.status !== 'error' && event.data?.status !== 'failed';
            pending.onUpdate({
                type: 'tool_result',
                id: toolId,
                output: event.data?.content ?? event.data,
                status: success ? 'completed' : 'failed'
            });
            return;
        }

        if (event.type === 'complete') {
            const completed = this.pendingPrompt;
            if (!completed) {
                return;
            }
            const nextSessionId = typeof event.data?.sessionId === 'string' ? event.data.sessionId : null;
            if (nextSessionId) {
                this.latestSessionId = nextSessionId;
            }
            const stopReason = event.data?.cancelled ? 'cancelled' : 'completed';
            completed.onUpdate({ type: 'turn_complete', stopReason });
            completed.resolve();
            this.pendingPrompt = null;
        }
    }

    private handleToolConfirmationRequest(event: SnowSseEvent): void {
        const requestId = event.requestId;
        if (!requestId || !this.permissionHandler) {
            return;
        }

        const toolCall = event.data?.toolCall ?? {};
        const toolName = this.extractToolName(toolCall);
        const toolInput = this.extractToolInput(toolCall);
        const options = this.mapPermissionOptions(event.data?.availableOptions);
        const optionMap = new Map<string, any>();
        for (const option of options) {
            optionMap.set(option.optionId, option.optionId);
        }
        this.pendingPermissions.set(requestId, { optionMap });

        const permissionRequest: PermissionRequest = {
            id: requestId,
            sessionId: this.pendingPrompt?.sessionId ?? this.latestSessionId ?? requestId,
            toolCallId: requestId,
            title: toolName,
            kind: toolName,
            rawInput: toolInput,
            options
        };
        this.permissionHandler(permissionRequest);
    }

    private async handleUserQuestionRequest(event: SnowSseEvent): Promise<void> {
        const requestId = event.requestId;
        if (!requestId) {
            return;
        }
        const options = Array.isArray(event.data?.options) ? event.data.options.filter((item: unknown) => typeof item === 'string') : [];
        const selected = options.length > 0 ? options[0] : '';
        await this.postJson('/message', {
            type: 'user_question_response',
            requestId,
            response: {
                selected,
                customInput: ''
            }
        });
    }

    private mapPermissionOptions(rawOptions: unknown): PermissionOption[] {
        if (!Array.isArray(rawOptions)) {
            return [];
        }
        return rawOptions
            .map((raw) => {
                const value = raw?.value;
                const label = raw?.label;
                if (typeof value !== 'string') {
                    return null;
                }
                const kind = this.mapOptionKind(value);
                return {
                    optionId: value,
                    name: typeof label === 'string' ? label : value,
                    kind
                } satisfies PermissionOption;
            })
            .filter((item): item is PermissionOption => item !== null);
    }

    private mapOptionKind(value: string): string {
        if (value === 'approve') return 'allow_once';
        if (value === 'approve_always') return 'allow_always';
        if (value === 'reject') return 'reject_once';
        if (value === 'reject_with_reply') return 'reject_with_reply';
        return value;
    }

    private resolvePermissionDecision(
        optionMap: Map<string, any>,
        response: PermissionResponse
    ): any {
        if (response.outcome === 'cancelled') {
            return 'reject';
        }
        if (optionMap.has(response.optionId)) {
            return optionMap.get(response.optionId);
        }
        return response.optionId;
    }

    private extractToolName(toolCall: any): string {
        if (typeof toolCall?.name === 'string' && toolCall.name.length > 0) {
            return toolCall.name;
        }
        if (typeof toolCall?.function?.name === 'string' && toolCall.function.name.length > 0) {
            return toolCall.function.name;
        }
        return 'snow-tool';
    }

    private extractToolInput(toolCall: any): unknown {
        const input = toolCall?.arguments ?? toolCall?.function?.arguments;
        if (typeof input === 'string') {
            try {
                return JSON.parse(input);
            } catch {
                return input;
            }
        }
        return input;
    }

    private extractToolCallId(toolCall: any): string {
        const id = toolCall?.id ?? toolCall?.toolCallId ?? toolCall?.callId;
        if (typeof id === 'string' && id.length > 0) {
            return id;
        }
        return randomUUID();
    }

    private extractToolResultId(data: any): string {
        const id = data?.toolCallId ?? data?.callId ?? data?.id;
        if (typeof id === 'string' && id.length > 0) {
            return id;
        }
        if (this.lastToolCallId) {
            return this.lastToolCallId;
        }
        return randomUUID();
    }

    private async postJson(path: string, payload: unknown): Promise<any> {
        const response = await fetch(`${this.baseUrl}${path}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        if (!response.ok) {
            const bodyText = await response.text().catch(() => '');
            throw new Error(`Snow SSE 请求失败: ${response.status} ${response.statusText}${bodyText ? ` - ${bodyText}` : ''}`);
        }
        return await response.json().catch(() => ({}));
    }
}
