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
import { Store } from '@ngrx/store';
import { Actions, createEffect, ofType } from '@ngrx/effects';
import { of, forkJoin, from } from 'rxjs';
import { map, switchMap, catchError, tap, withLatestFrom, filter } from 'rxjs/operators';
import { MCPClientService } from '../services/mcp-client.service';
import { LLMService } from '../services/llm.service';
import { ChatHistoryService, Conversation } from '../services/chat-history.service';
import { ChatMessage } from '../reducers/mcp.reducer';
import * as mcpActions from '../actions/mcp.actions';

const TITLE_MAX = 48;

function titleFor(messages: ChatMessage[]): string {
    const firstUser = messages.find(m => m.role === 'user' && !!m.text);
    const text = (firstUser && firstUser.text) ? firstUser.text.trim() : 'New conversation';
    return text.length > TITLE_MAX ? text.slice(0, TITLE_MAX) + '…' : text;
}

/* Defensively pulls dataverse names out of the asterixdb://dataverses resource. */
function parseDataverses(result: any): string[] {
    try {
        const contents = (result && result.contents) || [];
        const text = contents.map((c: any) => c && c.text).filter((t: string) => !!t).join('');
        const data = JSON.parse(text);
        const rows = Array.isArray(data) ? data : (data.dataverses || data.results || []);
        const names = rows
            .map((row: any) => typeof row === 'string' ? row : (row.DataverseName || row.dataverse || row.name))
            .filter((n: any) => typeof n === 'string');
        return Array.from(new Set(['Default', ...names]));
    } catch {
        return ['Default'];
    }
}

/*
* Runs MCP client calls as side effects, exactly as dataset.effects /
* query.effects run their CC calls. A view dispatches an action; the matching
* effect invokes the MCP client and dispatches the result back into the store.
*/
@Injectable()
export class MCPEffects {
    constructor(private actions: Actions,
                private store: Store<any>,
                private mcpClient: MCPClientService,
                private llm: LLMService,
                private history: ChatHistoryService) {
    }

    connect$ = createEffect(() => this.actions.pipe(
        ofType(mcpActions.CONNECT),
        switchMap(action => {
            return this.mcpClient.connect((action as mcpActions.Connect).payload).pipe(
                map(result => new mcpActions.ConnectSuccess(result)),
                catchError(error => of(new mcpActions.ConnectFail(error)))
            );
        })
    ));

    // A successful connect immediately loads the tool registry, the resource
    // catalog, and the prompt catalog (the three MCP primitives).
    afterConnect$ = createEffect(() => this.actions.pipe(
        ofType(mcpActions.CONNECT_SUCCESS),
        map(() => new mcpActions.ListTools())
    ));

    afterConnectResources$ = createEffect(() => this.actions.pipe(
        ofType(mcpActions.CONNECT_SUCCESS),
        map(() => new mcpActions.ListResources())
    ));

    afterConnectPrompts$ = createEffect(() => this.actions.pipe(
        ofType(mcpActions.CONNECT_SUCCESS),
        map(() => new mcpActions.ListPrompts())
    ));

    // Load the device-local conversation list on connect.
    afterConnectHistory$ = createEffect(() => this.actions.pipe(
        ofType(mcpActions.CONNECT_SUCCESS),
        map(() => new mcpActions.LoadHistory())
    ));

    // Populate the dataverse ("project") picker from the cluster.
    loadDataverses$ = createEffect(() => this.actions.pipe(
        ofType(mcpActions.CONNECT_SUCCESS),
        switchMap(() => this.mcpClient.readResource('asterixdb://dataverses').pipe(
            map(result => new mcpActions.SetDataverses(parseDataverses(result))),
            catchError(() => of(new mcpActions.SetDataverses(['Default'])))
        ))
    ));

    loadHistory$ = createEffect(() => this.actions.pipe(
        ofType(mcpActions.LOAD_HISTORY),
        switchMap(() => from(this.history.list()).pipe(
            map(list => new mcpActions.LoadHistorySuccess(list)),
            catchError(() => of(new mcpActions.LoadHistorySuccess([])))
        ))
    ));

    // Starting a new conversation clears the in-memory LLM context.
    newConversation$ = createEffect(() => this.actions.pipe(
        ofType(mcpActions.NEW_CONVERSATION),
        tap(() => this.llm.reset())
    ), { dispatch: false });

    // Persist the active conversation at the end of every assistant turn.
    persistConversation$ = createEffect(() => this.actions.pipe(
        ofType(mcpActions.CHAT_DONE),
        withLatestFrom(this.store.select((state: any) => state.mcp)),
        filter(([, mcp]) => !!mcp.activeConversationId && mcp.messages.length > 0),
        switchMap(([, mcp]) => {
            const now = Date.now();
            const conversation: Conversation = {
                id: mcp.activeConversationId,
                title: titleFor(mcp.messages),
                dataverse: mcp.activeDataverse,
                messages: mcp.messages,
                provider: this.llm.provider,
                providerHistory: this.llm.getHistory(),
                createdAt: now,
                updatedAt: now
            };
            return from(this.history.put(conversation)).pipe(
                map(() => new mcpActions.ConversationSaved({
                    id: conversation.id,
                    title: conversation.title,
                    dataverse: conversation.dataverse,
                    updatedAt: conversation.updatedAt
                })),
                catchError(() => of({ type: '[MCP] Persist Noop' } as any))
            );
        })
    ));

    // Open an old conversation: load its transcript and restore LLM context.
    selectConversation$ = createEffect(() => this.actions.pipe(
        ofType(mcpActions.SELECT_CONVERSATION),
        switchMap(action => {
            const id = (action as mcpActions.SelectConversation).payload;
            return from(this.history.get(id)).pipe(
                filter(conversation => !!conversation),
                tap(conversation => {
                    // Replay the saved native history when it was shaped for the
                    // provider in use now; otherwise re-seed from the transcript
                    // as plain text so the new provider still carries the prior
                    // conversation forward instead of starting blank.
                    if (conversation!.provider && conversation!.provider === this.llm.provider) {
                        this.llm.setHistory(conversation!.providerHistory);
                    } else {
                        this.llm.restoreFromTranscript(conversation!.messages);
                    }
                }),
                map(conversation => new mcpActions.SelectConversationSuccess({
                    id: conversation!.id,
                    dataverse: conversation!.dataverse,
                    messages: conversation!.messages
                })),
                catchError(() => of({ type: '[MCP] Select Noop' } as any))
            );
        })
    ));

    deleteConversation$ = createEffect(() => this.actions.pipe(
        ofType(mcpActions.DELETE_CONVERSATION),
        switchMap(action => {
            const id = (action as mcpActions.DeleteConversation).payload;
            return from(this.history.delete(id)).pipe(
                map(() => new mcpActions.DeleteConversationSuccess(id)),
                catchError(() => of({ type: '[MCP] Delete Noop' } as any))
            );
        })
    ));

    listTools$ = createEffect(() => this.actions.pipe(
        ofType(mcpActions.LIST_TOOLS),
        switchMap(() => {
            return this.mcpClient.listTools().pipe(
                map(tools => new mcpActions.ListToolsSuccess(tools)),
                catchError(error => of(new mcpActions.ListToolsFail(error)))
            );
        })
    ));

    callTool$ = createEffect(() => this.actions.pipe(
        ofType(mcpActions.CALL_TOOL),
        switchMap(action => {
            const payload = (action as mcpActions.CallTool).payload;
            return this.mcpClient.callTool(payload.name, payload.args).pipe(
                map(result => new mcpActions.CallToolSuccess(result)),
                catchError(error => of(new mcpActions.CallToolFail(error)))
            );
        })
    ));

    // Resources: list the static catalog and the URI templates together, so a
    // single success action carries both. Templates are optional — an empty
    // list is fine when the gateway advertises none.
    listResources$ = createEffect(() => this.actions.pipe(
        ofType(mcpActions.LIST_RESOURCES),
        switchMap(() => {
            return forkJoin({
                resources: this.mcpClient.listResources().pipe(catchError(() => of([]))),
                templates: this.mcpClient.listResourceTemplates().pipe(catchError(() => of([])))
            }).pipe(
                map(({ resources, templates }) => new mcpActions.ListResourcesSuccess({ resources, templates })),
                catchError(error => of(new mcpActions.ListResourcesFail(error)))
            );
        })
    ));

    readResource$ = createEffect(() => this.actions.pipe(
        ofType(mcpActions.READ_RESOURCE),
        switchMap(action => {
            const uri = (action as mcpActions.ReadResource).payload;
            return this.mcpClient.readResource(uri).pipe(
                map(result => new mcpActions.ReadResourceSuccess(result)),
                catchError(error => of(new mcpActions.ReadResourceFail(error)))
            );
        })
    ));

    listPrompts$ = createEffect(() => this.actions.pipe(
        ofType(mcpActions.LIST_PROMPTS),
        switchMap(() => {
            return this.mcpClient.listPrompts().pipe(
                map(prompts => new mcpActions.ListPromptsSuccess(prompts)),
                catchError(error => of(new mcpActions.ListPromptsFail(error)))
            );
        })
    ));

    getPrompt$ = createEffect(() => this.actions.pipe(
        ofType(mcpActions.GET_PROMPT),
        switchMap(action => {
            const payload = (action as mcpActions.GetPrompt).payload;
            return this.mcpClient.getPrompt(payload.name, payload.args).pipe(
                map(result => new mcpActions.GetPromptSuccess(result)),
                catchError(error => of(new mcpActions.GetPromptFail(error)))
            );
        })
    ));

    // Push the runtime LLM credentials into the (memory-only) service.
    configureLLM$ = createEffect(() => this.actions.pipe(
        ofType(mcpActions.CONFIGURE_LLM),
        tap(action => {
            this.llm.configure((action as mcpActions.ConfigureLLM).payload);
        })
    ), { dispatch: false });

    // Drop the in-memory conversation when the chat is cleared or the session ends.
    resetChat$ = createEffect(() => this.actions.pipe(
        ofType(mcpActions.CLEAR_CHAT, mcpActions.DISCONNECT),
        tap(() => this.llm.reset())
    ), { dispatch: false });

    // Run one assistant turn; stream each trace event back into the store.
    sendMessage$ = createEffect(() => this.actions.pipe(
        ofType(mcpActions.SEND_MESSAGE),
        withLatestFrom(this.store.select((state: any) => state.mcp.tools)),
        switchMap(([action, tools]) => {
            const text = (action as mcpActions.SendMessage).payload;
            return this.llm.sendMessage(text, tools || []).pipe(
                map(event => event.kind === 'done'
                    ? new mcpActions.ChatDone()
                    : new mcpActions.ChatEventAction(event)),
                catchError(error => of(new mcpActions.ChatFail(error)))
            );
        })
    ));
}
