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
import * as mcpActions from '../actions/mcp.actions';
import { MCPTool, MCPResource, MCPResourceTemplate, MCPPrompt } from '../services/mcp-client.service';
import { ChatEvent } from '../services/llm.service';
import { ConversationMeta } from '../services/chat-history.service';

const DEFAULT_DATAVERSE = 'Default';

function newId(): string {
    return (typeof crypto !== 'undefined' && (crypto as any).randomUUID)
        ? (crypto as any).randomUUID()
        : 'c-' + Date.now() + '-' + Math.random().toString(36).slice(2, 8);
}

export type Action = mcpActions.All;

/*
* One rendered entry in the assistant transcript. A 'tool' entry carries the
* inline trace card (name, args, result, live status).
*/
export interface ChatMessage {
    role: 'user' | 'assistant' | 'tool';
    text?: string;
    tool?: {
        id: string;
        name: string;
        args: any;
        result?: any;
        status: 'running' | 'ok' | 'error';
    };
}

/*
* MCP panel state. Holds the open/closed flag, connection status, the tool
* registry returned by the gateway, the currently selected tool, the in-flight
* call flag, and the last result/error. The gateway stays the single source of
* truth; this slice only caches what the panel needs to render.
*/
export interface State {
    open: boolean;
    view: 'assistant' | 'tools' | 'resources' | 'prompts';
    connecting: boolean;
    connected: boolean;
    tools: MCPTool[];
    selectedTool: MCPTool | null;
    calling: boolean;
    lastResult: any;
    error: any;
    resources: MCPResource[];
    resourceTemplates: MCPResourceTemplate[];
    selectedResource: MCPResource | null;
    resourceContent: any;
    readingResource: boolean;
    resourceError: any;
    prompts: MCPPrompt[];
    selectedPrompt: MCPPrompt | null;
    promptResult: any;
    gettingPrompt: boolean;
    promptError: any;
    llmReady: boolean;
    model: string;
    chatting: boolean;
    messages: ChatMessage[];
    chatError: any;
    conversations: ConversationMeta[];
    activeConversationId: string | null;
    activeDataverse: string;
    dataverses: string[];
}

const initialState: State = {
    open: false,
    view: 'assistant',
    connecting: false,
    connected: false,
    tools: [],
    selectedTool: null,
    calling: false,
    lastResult: null,
    error: null,
    resources: [],
    resourceTemplates: [],
    selectedResource: null,
    resourceContent: null,
    readingResource: false,
    resourceError: null,
    prompts: [],
    selectedPrompt: null,
    promptResult: null,
    gettingPrompt: false,
    promptError: null,
    llmReady: false,
    model: '',
    chatting: false,
    messages: [],
    chatError: null,
    conversations: [],
    activeConversationId: null,
    activeDataverse: DEFAULT_DATAVERSE,
    dataverses: [DEFAULT_DATAVERSE]
};

export function mcpReducer(state = initialState, action: Action): State {
    switch (action.type) {
        case mcpActions.TOGGLE_PANEL: {
            const open = action.payload === undefined ? !state.open : !!action.payload;
            return { ...state, open };
        }

        case mcpActions.CONNECT:
            return { ...state, connecting: true, connected: false, error: null };

        case mcpActions.CONNECT_SUCCESS:
            return { ...state, connecting: false, connected: true, error: null };

        case mcpActions.CONNECT_FAIL:
            return { ...state, connecting: false, connected: false, error: action.payload };

        case mcpActions.DISCONNECT:
            return {
                ...initialState,
                open: state.open,
                conversations: state.conversations,
                dataverses: state.dataverses,
                activeDataverse: state.activeDataverse
            };

        case mcpActions.LIST_TOOLS_SUCCESS:
            return { ...state, tools: action.payload, error: null };

        case mcpActions.LIST_TOOLS_FAIL:
            return { ...state, error: action.payload };

        case mcpActions.SELECT_TOOL:
            return { ...state, selectedTool: action.payload, lastResult: null, error: null };

        case mcpActions.CALL_TOOL:
            return { ...state, calling: true, lastResult: null, error: null };

        case mcpActions.CALL_TOOL_SUCCESS:
            return { ...state, calling: false, lastResult: action.payload, error: null };

        case mcpActions.CALL_TOOL_FAIL:
            return { ...state, calling: false, error: action.payload };

        case mcpActions.LIST_RESOURCES_SUCCESS:
            return {
                ...state,
                resources: action.payload.resources,
                resourceTemplates: action.payload.templates,
                resourceError: null
            };

        case mcpActions.LIST_RESOURCES_FAIL:
            return { ...state, resourceError: action.payload };

        case mcpActions.SELECT_RESOURCE:
            return { ...state, selectedResource: action.payload, resourceContent: null, resourceError: null };

        case mcpActions.READ_RESOURCE:
            return { ...state, readingResource: true, resourceContent: null, resourceError: null };

        case mcpActions.READ_RESOURCE_SUCCESS:
            return { ...state, readingResource: false, resourceContent: action.payload, resourceError: null };

        case mcpActions.READ_RESOURCE_FAIL:
            return { ...state, readingResource: false, resourceError: action.payload };

        case mcpActions.LIST_PROMPTS_SUCCESS:
            return { ...state, prompts: action.payload, promptError: null };

        case mcpActions.LIST_PROMPTS_FAIL:
            return { ...state, promptError: action.payload };

        case mcpActions.SELECT_PROMPT:
            return { ...state, selectedPrompt: action.payload, promptResult: null, promptError: null };

        case mcpActions.GET_PROMPT:
            return { ...state, gettingPrompt: true, promptResult: null, promptError: null };

        case mcpActions.GET_PROMPT_SUCCESS:
            return { ...state, gettingPrompt: false, promptResult: action.payload, promptError: null };

        case mcpActions.GET_PROMPT_FAIL:
            return { ...state, gettingPrompt: false, promptError: action.payload };

        case mcpActions.SET_VIEW:
            return { ...state, view: action.payload };

        case mcpActions.CONFIGURE_LLM:
            return { ...state, llmReady: true, model: action.payload.model };

        case mcpActions.SEND_MESSAGE:
            return {
                ...state,
                chatting: true,
                chatError: null,
                // First message of a fresh thread mints the conversation id.
                activeConversationId: state.activeConversationId || newId(),
                messages: [...state.messages, { role: 'user', text: action.payload }]
            };

        case mcpActions.CHAT_EVENT:
            return { ...state, messages: foldEvent(state.messages, action.payload) };

        case mcpActions.CHAT_DONE:
            return { ...state, chatting: false };

        case mcpActions.CHAT_FAIL:
            return { ...state, chatting: false, chatError: action.payload };

        case mcpActions.CLEAR_CHAT:
            return { ...state, messages: [], chatError: null };

        case mcpActions.LOAD_HISTORY_SUCCESS:
            return { ...state, conversations: action.payload };

        case mcpActions.SET_DATAVERSE_SCOPE:
            return { ...state, activeDataverse: action.payload };

        case mcpActions.SET_DATAVERSES: {
            const list = action.payload && action.payload.length ? action.payload : [DEFAULT_DATAVERSE];
            const scope = list.indexOf(state.activeDataverse) !== -1 ? state.activeDataverse : list[0];
            return { ...state, dataverses: list, activeDataverse: scope };
        }

        case mcpActions.NEW_CONVERSATION:
            return { ...state, activeConversationId: null, messages: [], chatError: null };

        case mcpActions.SELECT_CONVERSATION_SUCCESS:
            return {
                ...state,
                activeConversationId: action.payload.id,
                activeDataverse: action.payload.dataverse,
                messages: action.payload.messages,
                chatError: null
            };

        case mcpActions.DELETE_CONVERSATION_SUCCESS: {
            const conversations = state.conversations.filter(c => c.id !== action.payload);
            const wasActive = state.activeConversationId === action.payload;
            return {
                ...state,
                conversations,
                activeConversationId: wasActive ? null : state.activeConversationId,
                messages: wasActive ? [] : state.messages
            };
        }

        case mcpActions.CONVERSATION_SAVED: {
            const others = state.conversations.filter(c => c.id !== action.payload.id);
            const conversations = [action.payload, ...others].sort((a, b) => b.updatedAt - a.updatedAt);
            return { ...state, conversations };
        }

        default:
            return state;
    }
}

/*
* Folds a streamed assistant event into the transcript. Text appends a bubble;
* a tool_call appends a running trace card; a tool_result resolves the matching
* card by id. Always returns a new array — never mutates the existing one.
*/
function foldEvent(messages: ChatMessage[], event: ChatEvent): ChatMessage[] {
    switch (event.kind) {
        case 'assistant_text':
            return [...messages, { role: 'assistant', text: event.text }];

        case 'tool_call':
            return [...messages, {
                role: 'tool',
                tool: { id: event.id, name: event.name, args: event.args, status: 'running' }
            }];

        case 'tool_result':
            return messages.map(message => {
                if (message.role !== 'tool' || !message.tool || message.tool.id !== event.id) {
                    return message;
                }
                return {
                    ...message,
                    tool: {
                        ...message.tool,
                        result: event.result,
                        status: event.isError ? 'error' : 'ok'
                    }
                };
            });

        default:
            return messages;
    }
}
