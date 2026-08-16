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
import { Action } from '@ngrx/store';
import {
    MCPConnection,
    MCPTool,
    MCPResource,
    MCPResourceTemplate,
    MCPPrompt
} from '../services/mcp-client.service';
import { ChatEvent } from '../services/llm.service';
import { LLMCredentials } from '../services/llm-providers';
import { ConversationMeta } from '../services/chat-history.service';
import { ChatMessage } from '../reducers/mcp.reducer';

/*
* Definition of MCP panel actions. The panel is an MCP client of the gateway;
* every call flows through these actions into mcp.effects, mirroring the
* existing dataset/query effect pattern.
*/
export const TOGGLE_PANEL        = '[MCP] Toggle Panel';
export const CONNECT             = '[MCP] Connect';
export const CONNECT_SUCCESS     = '[MCP] Connect Success';
export const CONNECT_FAIL        = '[MCP] Connect Fail';
export const DISCONNECT          = '[MCP] Disconnect';
export const LIST_TOOLS          = '[MCP] List Tools';
export const LIST_TOOLS_SUCCESS  = '[MCP] List Tools Success';
export const LIST_TOOLS_FAIL     = '[MCP] List Tools Fail';
export const SELECT_TOOL         = '[MCP] Select Tool';
export const CALL_TOOL           = '[MCP] Call Tool';
export const CALL_TOOL_SUCCESS   = '[MCP] Call Tool Success';
export const CALL_TOOL_FAIL      = '[MCP] Call Tool Fail';
export const LIST_RESOURCES         = '[MCP] List Resources';
export const LIST_RESOURCES_SUCCESS = '[MCP] List Resources Success';
export const LIST_RESOURCES_FAIL    = '[MCP] List Resources Fail';
export const SELECT_RESOURCE        = '[MCP] Select Resource';
export const READ_RESOURCE          = '[MCP] Read Resource';
export const READ_RESOURCE_SUCCESS  = '[MCP] Read Resource Success';
export const READ_RESOURCE_FAIL     = '[MCP] Read Resource Fail';
export const LIST_PROMPTS           = '[MCP] List Prompts';
export const LIST_PROMPTS_SUCCESS   = '[MCP] List Prompts Success';
export const LIST_PROMPTS_FAIL      = '[MCP] List Prompts Fail';
export const SELECT_PROMPT          = '[MCP] Select Prompt';
export const GET_PROMPT             = '[MCP] Get Prompt';
export const GET_PROMPT_SUCCESS     = '[MCP] Get Prompt Success';
export const GET_PROMPT_FAIL        = '[MCP] Get Prompt Fail';
export const SET_VIEW            = '[MCP] Set View';
export const CONFIGURE_LLM       = '[MCP] Configure LLM';
export const SEND_MESSAGE        = '[MCP] Send Message';
export const CHAT_EVENT          = '[MCP] Chat Event';
export const CHAT_DONE           = '[MCP] Chat Done';
export const CHAT_FAIL           = '[MCP] Chat Fail';
export const CLEAR_CHAT          = '[MCP] Clear Chat';
export const LOAD_HISTORY            = '[MCP] Load History';
export const LOAD_HISTORY_SUCCESS    = '[MCP] Load History Success';
export const SET_DATAVERSE_SCOPE     = '[MCP] Set Dataverse Scope';
export const SET_DATAVERSES          = '[MCP] Set Dataverses';
export const NEW_CONVERSATION        = '[MCP] New Conversation';
export const SELECT_CONVERSATION         = '[MCP] Select Conversation';
export const SELECT_CONVERSATION_SUCCESS = '[MCP] Select Conversation Success';
export const DELETE_CONVERSATION         = '[MCP] Delete Conversation';
export const DELETE_CONVERSATION_SUCCESS = '[MCP] Delete Conversation Success';
export const CONVERSATION_SAVED      = '[MCP] Conversation Saved';

export class TogglePanel implements Action {
    readonly type = TOGGLE_PANEL;
    constructor(public payload?: boolean) {}
}

export class Connect implements Action {
    readonly type = CONNECT;
    constructor(public payload: MCPConnection) {}
}

export class ConnectSuccess implements Action {
    readonly type = CONNECT_SUCCESS;
    constructor(public payload: any) {}
}

export class ConnectFail implements Action {
    readonly type = CONNECT_FAIL;
    constructor(public payload: any) {}
}

export class Disconnect implements Action {
    readonly type = DISCONNECT;
    constructor(public payload?: any) {}
}

export class ListTools implements Action {
    readonly type = LIST_TOOLS;
    constructor(public payload?: any) {}
}

export class ListToolsSuccess implements Action {
    readonly type = LIST_TOOLS_SUCCESS;
    constructor(public payload: MCPTool[]) {}
}

export class ListToolsFail implements Action {
    readonly type = LIST_TOOLS_FAIL;
    constructor(public payload: any) {}
}

export class SelectTool implements Action {
    readonly type = SELECT_TOOL;
    constructor(public payload: MCPTool) {}
}

export class CallTool implements Action {
    readonly type = CALL_TOOL;
    constructor(public payload: { name: string; args: any }) {}
}

export class CallToolSuccess implements Action {
    readonly type = CALL_TOOL_SUCCESS;
    constructor(public payload: any) {}
}

export class CallToolFail implements Action {
    readonly type = CALL_TOOL_FAIL;
    constructor(public payload: any) {}
}

export class ListResources implements Action {
    readonly type = LIST_RESOURCES;
    constructor(public payload?: any) {}
}

export class ListResourcesSuccess implements Action {
    readonly type = LIST_RESOURCES_SUCCESS;
    constructor(public payload: { resources: MCPResource[]; templates: MCPResourceTemplate[] }) {}
}

export class ListResourcesFail implements Action {
    readonly type = LIST_RESOURCES_FAIL;
    constructor(public payload: any) {}
}

export class SelectResource implements Action {
    readonly type = SELECT_RESOURCE;
    constructor(public payload: MCPResource) {}
}

export class ReadResource implements Action {
    readonly type = READ_RESOURCE;
    constructor(public payload: string) {}
}

export class ReadResourceSuccess implements Action {
    readonly type = READ_RESOURCE_SUCCESS;
    constructor(public payload: any) {}
}

export class ReadResourceFail implements Action {
    readonly type = READ_RESOURCE_FAIL;
    constructor(public payload: any) {}
}

export class ListPrompts implements Action {
    readonly type = LIST_PROMPTS;
    constructor(public payload?: any) {}
}

export class ListPromptsSuccess implements Action {
    readonly type = LIST_PROMPTS_SUCCESS;
    constructor(public payload: MCPPrompt[]) {}
}

export class ListPromptsFail implements Action {
    readonly type = LIST_PROMPTS_FAIL;
    constructor(public payload: any) {}
}

export class SelectPrompt implements Action {
    readonly type = SELECT_PROMPT;
    constructor(public payload: MCPPrompt) {}
}

export class GetPrompt implements Action {
    readonly type = GET_PROMPT;
    constructor(public payload: { name: string; args: any }) {}
}

export class GetPromptSuccess implements Action {
    readonly type = GET_PROMPT_SUCCESS;
    constructor(public payload: any) {}
}

export class GetPromptFail implements Action {
    readonly type = GET_PROMPT_FAIL;
    constructor(public payload: any) {}
}

export class SetView implements Action {
    readonly type = SET_VIEW;
    constructor(public payload: 'assistant' | 'tools' | 'resources' | 'prompts') {}
}

export class ConfigureLLM implements Action {
    readonly type = CONFIGURE_LLM;
    constructor(public payload: LLMCredentials) {}
}

export class SendMessage implements Action {
    readonly type = SEND_MESSAGE;
    constructor(public payload: string) {}
}

export class ChatEventAction implements Action {
    readonly type = CHAT_EVENT;
    constructor(public payload: ChatEvent) {}
}

export class ChatDone implements Action {
    readonly type = CHAT_DONE;
    constructor(public payload?: any) {}
}

export class ChatFail implements Action {
    readonly type = CHAT_FAIL;
    constructor(public payload: any) {}
}

export class ClearChat implements Action {
    readonly type = CLEAR_CHAT;
    constructor(public payload?: any) {}
}

export class LoadHistory implements Action {
    readonly type = LOAD_HISTORY;
    constructor(public payload?: any) {}
}

export class LoadHistorySuccess implements Action {
    readonly type = LOAD_HISTORY_SUCCESS;
    constructor(public payload: ConversationMeta[]) {}
}

export class SetDataverseScope implements Action {
    readonly type = SET_DATAVERSE_SCOPE;
    constructor(public payload: string) {}
}

export class SetDataverses implements Action {
    readonly type = SET_DATAVERSES;
    constructor(public payload: string[]) {}
}

export class NewConversation implements Action {
    readonly type = NEW_CONVERSATION;
    constructor(public payload?: any) {}
}

export class SelectConversation implements Action {
    readonly type = SELECT_CONVERSATION;
    constructor(public payload: string) {}
}

export class SelectConversationSuccess implements Action {
    readonly type = SELECT_CONVERSATION_SUCCESS;
    constructor(public payload: { id: string; dataverse: string; messages: ChatMessage[] }) {}
}

export class DeleteConversation implements Action {
    readonly type = DELETE_CONVERSATION;
    constructor(public payload: string) {}
}

export class DeleteConversationSuccess implements Action {
    readonly type = DELETE_CONVERSATION_SUCCESS;
    constructor(public payload: string) {}
}

export class ConversationSaved implements Action {
    readonly type = CONVERSATION_SAVED;
    constructor(public payload: ConversationMeta) {}
}

/*
* Exports of MCP actions
*/
export type All = TogglePanel |
    Connect |
    ConnectSuccess |
    ConnectFail |
    Disconnect |
    ListTools |
    ListToolsSuccess |
    ListToolsFail |
    SelectTool |
    CallTool |
    CallToolSuccess |
    CallToolFail |
    ListResources |
    ListResourcesSuccess |
    ListResourcesFail |
    SelectResource |
    ReadResource |
    ReadResourceSuccess |
    ReadResourceFail |
    ListPrompts |
    ListPromptsSuccess |
    ListPromptsFail |
    SelectPrompt |
    GetPrompt |
    GetPromptSuccess |
    GetPromptFail |
    SetView |
    ConfigureLLM |
    SendMessage |
    ChatEventAction |
    ChatDone |
    ChatFail |
    ClearChat |
    LoadHistory |
    LoadHistorySuccess |
    SetDataverseScope |
    SetDataverses |
    NewConversation |
    SelectConversation |
    SelectConversationSuccess |
    DeleteConversation |
    DeleteConversationSuccess |
    ConversationSaved;
