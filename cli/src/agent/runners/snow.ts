import { AgentRegistry } from '@/agent/AgentRegistry';
import { SnowSseBackend } from '@/agent/backends/snow';

export function registerSnowAgent(options?: { baseUrl?: string; yolo?: boolean }): void {
    AgentRegistry.register('snow', () => new SnowSseBackend({
        baseUrl: options?.baseUrl,
        yoloMode: options?.yolo
    }));
}
