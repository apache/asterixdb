/*
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, lastValueFrom } from 'rxjs';
import { MCPClientService, MCPTool } from './mcp-client.service';
import { ChatMessage } from '../reducers/mcp.reducer';
import {
    LLMCredentials,
    ProviderAdapter,
    ToolExecResult,
    adapterFor
} from './llm-providers';

/*
* Events streamed out of a single assistant turn so the panel can render the
* tool-call trace inline (the Antigravity pattern) instead of hiding it in prose.
*/
export type ChatEvent =
    | { kind: 'assistant_text'; text: string }
    | { kind: 'tool_call'; id: string; name: string; args: any }
    | { kind: 'tool_result'; id: string; result: any; isError: boolean }
    | { kind: 'done' };

const MAX_TOOL_ITERATIONS = 8;

/* Cap on a tool result folded into a re-seeded cross-provider transcript. */
const MAX_TOOL_NOTE_CHARS = 2000;

/*
* Drives the natural-language assistant: a provider-agnostic tool-calling loop
* where every tool the model may call is a gateway tool. The model never gets a
* raw SQL++ channel or cluster credentials — the gateway stays the single
* authority, exactly as the manual panels do.
*
* The provider, model, base URL, and API key are supplied at runtime
* (configure()) and live only in memory; nothing is compiled into the served
* bundle. Provider-specific wire format lives in the adapters (llm-providers.ts);
* this loop is shared across Anthropic, the OpenAI family, GLM, and Gemini.
*/
@Injectable()
export class LLMService {
    private creds: LLMCredentials | null = null;
    private adapter: ProviderAdapter | null = null;
    private history: any[] = [];

    constructor(private http: HttpClient, private mcp: MCPClientService) {}

    get ready(): boolean {
        return !!this.creds && !!this.adapter;
    }

    /* The active provider id, or '' when unconfigured. Used to decide whether a
       saved conversation's history can be replayed or must be reset. */
    get provider(): string {
        return this.creds ? this.creds.provider : '';
    }

    configure(creds: LLMCredentials): void {
        this.creds = {
            provider: creds.provider,
            baseUrl: creds.baseUrl.trim(),
            model: creds.model.trim(),
            apiKey: creds.apiKey.trim()
        };
        this.adapter = adapterFor(creds.provider);
        this.history = [];
    }

    reset(): void {
        this.history = [];
    }

    /* The opaque provider-shaped turn list, persisted so an old conversation
       can be reloaded and continued with full model context. */
    getHistory(): any[] {
        return this.history;
    }

    setHistory(history: any[]): void {
        this.history = Array.isArray(history) ? history : [];
    }

    /*
    * Rebuilds the LLM context from a rendered transcript as alternating plain-
    * text turns, so a conversation saved under one provider can be continued
    * under another. Tool-call cards collapse to a short text note (structure is
    * lost, the data the model saw is kept). Consecutive same-role turns merge so
    * the result never violates a provider's strict user/assistant alternation,
    * and any leading assistant turns are dropped (providers expect user first).
    */
    restoreFromTranscript(messages: ChatMessage[]): void {
        const adapter = this.adapter;
        if (!adapter) {
            this.history = [];
            return;
        }
        const turns: { side: 'user' | 'assistant'; text: string }[] = [];
        const push = (side: 'user' | 'assistant', text: string) => {
            if (!text) {
                return;
            }
            const last = turns[turns.length - 1];
            if (last && last.side === side) {
                last.text += '\n\n' + text;
            } else {
                turns.push({ side, text });
            }
        };
        for (const message of messages || []) {
            if (message.role === 'user') {
                push('user', message.text || '');
            } else if (message.role === 'assistant') {
                push('assistant', message.text || '');
            } else if (message.role === 'tool' && message.tool) {
                push('assistant', this.toolNote(message.tool));
            }
        }
        while (turns.length && turns[0].side === 'assistant') {
            turns.shift();
        }
        this.history = turns.map(turn => turn.side === 'user'
            ? adapter.userMessage(turn.text)
            : adapter.assistantMessage(turn.text));
    }

    private toolNote(tool: { name: string; result?: any }): string {
        const raw = typeof tool.result === 'string' ? tool.result : JSON.stringify(tool.result || '');
        const result = raw.length > MAX_TOOL_NOTE_CHARS ? raw.slice(0, MAX_TOOL_NOTE_CHARS) + '…' : raw;
        return `[called ${tool.name} → ${result}]`;
    }

    /*
    * Runs one user turn: append the message, then loop LLM -> tool calls ->
    * results -> LLM until the model stops requesting tools. Emits a ChatEvent
    * for every assistant text block and every tool call/result so the UI can
    * render the trace as it happens.
    */
    sendMessage(text: string, tools: MCPTool[]): Observable<ChatEvent> {
        return new Observable<ChatEvent>(observer => {
            let cancelled = false;
            const adapter = this.adapter;
            const creds = this.creds;
            if (!adapter || !creds) {
                observer.error('No LLM provider configured.');
                return;
            }
            const mappedTools = adapter.mapTools(tools);

            const run = async () => {
                this.history.push(adapter.userMessage(text));
                for (let i = 0; i < MAX_TOOL_ITERATIONS && !cancelled; i++) {
                    const req = adapter.request(creds, this.history, mappedTools);
                    const response: any = await lastValueFrom(
                        this.http.post(req.url, req.body, { headers: req.headers })
                    );
                    const parsed = adapter.parse(response);
                    for (const text of parsed.texts) {
                        observer.next({ kind: 'assistant_text', text });
                    }
                    for (const call of parsed.toolCalls) {
                        observer.next({ kind: 'tool_call', id: call.id, name: call.name, args: call.args });
                    }
                    adapter.appendAssistant(this.history, parsed);

                    if (!parsed.needsTools || parsed.toolCalls.length === 0) {
                        break;
                    }
                    const results = await this.runToolCalls(parsed.toolCalls, observer, () => cancelled);
                    adapter.appendToolResults(this.history, results);
                }
                if (!cancelled) {
                    observer.next({ kind: 'done' });
                    observer.complete();
                }
            };

            run().catch(error => observer.error(this.message(error)));
            return () => { cancelled = true; };
        });
    }

    /* Executes each requested tool against the gateway and formats results. */
    private async runToolCalls(toolCalls: any[], observer: any, cancelled: () => boolean): Promise<ToolExecResult[]> {
        const results: ToolExecResult[] = [];
        for (const call of toolCalls) {
            if (cancelled()) {
                break;
            }
            try {
                const result = await lastValueFrom(this.mcp.callTool(call.name, call.args));
                const isError = !!(result && result.isError);
                observer.next({ kind: 'tool_result', id: call.id, result, isError });
                results.push({ id: call.id, name: call.name, content: JSON.stringify(result), isError });
            } catch (error) {
                const message = this.message(error);
                observer.next({ kind: 'tool_result', id: call.id, result: message, isError: true });
                results.push({ id: call.id, name: call.name, content: message, isError: true });
            }
        }
        return results;
    }

    private message(error: any): string {
        if (typeof error === 'string') {
            return error;
        }
        if (error && error.error && error.error.error && error.error.error.message) {
            return error.error.error.message;
        }
        return (error && error.message) || 'LLM request failed';
    }
}
