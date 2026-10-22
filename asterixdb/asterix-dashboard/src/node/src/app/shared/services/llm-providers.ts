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
import { MCPTool } from './mcp-client.service';

/*
* LLM provider abstraction. The assistant's tool-calling loop is provider-
* agnostic; each adapter only shapes the request, parses the response, and
* appends turns in that provider's native message format. Anthropic, the OpenAI
* function-calling family (OpenAI/GLM/DeepSeek/Groq/local), and Gemini differ
* enough in wire format to warrant one adapter each — but the loop is shared.
*/
export type ProviderId = 'anthropic' | 'openai' | 'gemini' | 'glm' | 'custom';

export interface LLMCredentials {
    provider: ProviderId;
    baseUrl: string;
    model: string;
    apiKey: string;
}

export interface ToolExecResult {
    id: string;
    name: string;
    content: string;
    isError: boolean;
}

/* Provider-neutral view of one model response. */
export interface ParsedTurn {
    texts: string[];
    toolCalls: { id: string; name: string; args: any }[];
    needsTools: boolean;
    assistant: any;   // native assistant turn to append back into history
}

export interface ProviderAdapter {
    mapTools(tools: MCPTool[]): any;
    userMessage(text: string): any;
    assistantMessage(text: string): any;
    request(creds: LLMCredentials, history: any[], tools: any): { url: string; headers: any; body: any };
    parse(response: any): ParsedTurn;
    appendAssistant(history: any[], parsed: ParsedTurn): void;
    appendToolResults(history: any[], results: ToolExecResult[]): void;
}

const MAX_TOKENS = 1024;

const SYSTEM_PROMPT =
    'You are an assistant embedded in the Apache AsterixDB web console. Answer questions ' +
    'about the database by calling the provided tools. The tools are read-only and already ' +
    'bound for safety — never ask for raw SQL access or credentials. Prefer a tool call over ' +
    'guessing. Keep answers concise and cite the data the tools returned.';

/*
* Anthropic Messages API.
*/
class AnthropicAdapter implements ProviderAdapter {
    mapTools(tools: MCPTool[]): any {
        return tools.map(tool => ({
            name: tool.name,
            description: tool.description || '',
            input_schema: tool.inputSchema || { type: 'object', properties: {} }
        }));
    }

    userMessage(text: string): any {
        return { role: 'user', content: text };
    }

    assistantMessage(text: string): any {
        return { role: 'assistant', content: text };
    }

    request(creds: LLMCredentials, history: any[], tools: any) {
        return {
            url: creds.baseUrl,
            headers: {
                'x-api-key': creds.apiKey,
                'anthropic-version': '2023-06-01',
                'anthropic-dangerous-direct-browser-access': 'true',
                'content-type': 'application/json'
            },
            body: { model: creds.model, max_tokens: MAX_TOKENS, system: SYSTEM_PROMPT, tools, messages: history }
        };
    }

    parse(response: any): ParsedTurn {
        const texts: string[] = [];
        const toolCalls: any[] = [];
        for (const block of (response && response.content) || []) {
            if (block.type === 'text') {
                texts.push(block.text);
            } else if (block.type === 'tool_use') {
                toolCalls.push({ id: block.id, name: block.name, args: block.input });
            }
        }
        return { texts, toolCalls, needsTools: response.stop_reason === 'tool_use', assistant: response.content };
    }

    appendAssistant(history: any[], parsed: ParsedTurn): void {
        history.push({ role: 'assistant', content: parsed.assistant });
    }

    appendToolResults(history: any[], results: ToolExecResult[]): void {
        history.push({
            role: 'user',
            content: results.map(r => ({
                type: 'tool_result', tool_use_id: r.id, content: r.content, is_error: r.isError
            }))
        });
    }
}

/*
* OpenAI-compatible chat/completions (OpenAI, GLM/Zhipu, DeepSeek, Groq, local,
* OpenRouter…). One adapter; only the base URL and key differ per vendor.
*/
class OpenAIAdapter implements ProviderAdapter {
    mapTools(tools: MCPTool[]): any {
        return tools.map(tool => ({
            type: 'function',
            function: {
                name: tool.name,
                description: tool.description || '',
                parameters: tool.inputSchema || { type: 'object', properties: {} }
            }
        }));
    }

    userMessage(text: string): any {
        return { role: 'user', content: text };
    }

    assistantMessage(text: string): any {
        return { role: 'assistant', content: text };
    }

    request(creds: LLMCredentials, history: any[], tools: any) {
        const messages = [{ role: 'system', content: SYSTEM_PROMPT }, ...history];
        return {
            url: creds.baseUrl,
            headers: { 'Authorization': `Bearer ${creds.apiKey}`, 'content-type': 'application/json' },
            body: { model: creds.model, messages, tools, tool_choice: 'auto' }
        };
    }

    parse(response: any): ParsedTurn {
        const message = response.choices && response.choices[0] && response.choices[0].message;
        const texts: string[] = message && message.content ? [message.content] : [];
        const toolCalls = ((message && message.tool_calls) || []).map((call: any) => ({
            id: call.id,
            name: call.function.name,
            args: this.safeParse(call.function.arguments)
        }));
        return { texts, toolCalls, needsTools: toolCalls.length > 0, assistant: message };
    }

    appendAssistant(history: any[], parsed: ParsedTurn): void {
        history.push(parsed.assistant);
    }

    appendToolResults(history: any[], results: ToolExecResult[]): void {
        for (const r of results) {
            history.push({ role: 'tool', tool_call_id: r.id, content: r.content });
        }
    }

    private safeParse(text: any): any {
        try {
            return typeof text === 'string' ? JSON.parse(text || '{}') : (text || {});
        } catch {
            return {};
        }
    }
}

/*
* Google Gemini generateContent. No tool-call ids — synthesized and matched by
* name on the way back.
*/
class GeminiAdapter implements ProviderAdapter {
    // JSON Schema keywords Gemini's function-declaration subset rejects.
    private static readonly UNSUPPORTED_KEYS = new Set<string>([
        'additionalProperties', '$schema', '$id', '$ref', '$defs', 'definitions',
        'patternProperties', 'unevaluatedProperties', 'propertyNames', 'examples'
    ]);

    mapTools(tools: MCPTool[]): any {
        return [{
            functionDeclarations: tools.map(tool => ({
                name: tool.name,
                description: tool.description || '',
                parameters: this.sanitizeSchema(tool.inputSchema) || { type: 'object', properties: {} }
            }))
        }];
    }

    /* Strip JSON Schema keywords Gemini does not accept (e.g. additionalProperties). */
    private sanitizeSchema(schema: any): any {
        if (Array.isArray(schema)) {
            return schema.map(item => this.sanitizeSchema(item));
        }
        if (schema && typeof schema === 'object') {
            const out: any = {};
            for (const [key, value] of Object.entries(schema)) {
                if (GeminiAdapter.UNSUPPORTED_KEYS.has(key)) {
                    continue;
                }
                out[key] = this.sanitizeSchema(value);
            }
            return out;
        }
        return schema;
    }

    userMessage(text: string): any {
        return { role: 'user', parts: [{ text }] };
    }

    assistantMessage(text: string): any {
        return { role: 'model', parts: [{ text }] };
    }

    request(creds: LLMCredentials, history: any[], tools: any) {
        return {
            url: `${creds.baseUrl}/${creds.model}:generateContent?key=${creds.apiKey}`,
            headers: { 'content-type': 'application/json' },
            body: {
                systemInstruction: { parts: [{ text: SYSTEM_PROMPT }] },
                contents: history,
                tools
            }
        };
    }

    parse(response: any): ParsedTurn {
        const content = response.candidates && response.candidates[0] && response.candidates[0].content;
        const parts = (content && content.parts) || [];
        const texts: string[] = [];
        const toolCalls: any[] = [];
        parts.forEach((part: any, index: number) => {
            if (part.text) {
                texts.push(part.text);
            } else if (part.functionCall) {
                toolCalls.push({ id: `call_${index}`, name: part.functionCall.name, args: part.functionCall.args || {} });
            }
        });
        return { texts, toolCalls, needsTools: toolCalls.length > 0, assistant: { role: 'model', parts } };
    }

    appendAssistant(history: any[], parsed: ParsedTurn): void {
        history.push(parsed.assistant);
    }

    appendToolResults(history: any[], results: ToolExecResult[]): void {
        history.push({
            role: 'user',
            parts: results.map(r => ({
                functionResponse: { name: r.name, response: { result: r.content } }
            }))
        });
    }
}

const ANTHROPIC = new AnthropicAdapter();
const OPENAI = new OpenAIAdapter();
const GEMINI = new GeminiAdapter();

export function adapterFor(provider: ProviderId): ProviderAdapter {
    switch (provider) {
        case 'anthropic':
            return ANTHROPIC;
        case 'gemini':
            return GEMINI;
        default:
            // openai, glm, custom all speak the OpenAI function-calling format.
            return OPENAI;
    }
}

/* UI presets: friendly defaults so the user only pastes a key. */
export interface ProviderPreset {
    id: ProviderId;
    label: string;
    baseUrl: string;
    model: string;
}

export const PROVIDER_PRESETS: ProviderPreset[] = [
    { id: 'anthropic', label: 'Anthropic (Claude)', baseUrl: 'https://api.anthropic.com/v1/messages', model: 'claude-sonnet-4-6' },
    { id: 'openai', label: 'OpenAI (ChatGPT)', baseUrl: 'https://api.openai.com/v1/chat/completions', model: 'gpt-4o' },
    { id: 'gemini', label: 'Google Gemini', baseUrl: 'https://generativelanguage.googleapis.com/v1beta/models', model: 'gemini-2.0-flash' },
    { id: 'glm', label: 'Zhipu GLM', baseUrl: 'https://open.bigmodel.cn/api/paas/v4/chat/completions', model: 'glm-4-plus' },
    { id: 'custom', label: 'Custom (OpenAI-compatible)', baseUrl: '', model: '' }
];
