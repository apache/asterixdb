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
import { Component, Input } from '@angular/core';
import { Store } from '@ngrx/store';
import * as mcpActions from '../../shared/actions/mcp.actions';
import { ChatMessage, State as MCPState } from '../../shared/reducers/mcp.reducer';
import { PROVIDER_PRESETS, ProviderId } from '../../shared/services/llm-providers';
import { MCPClientService, MCPResource, MCPPrompt } from '../../shared/services/mcp-client.service';

/* A resource the user pulled into the next turn via @-mention. */
interface ResourceAttachment {
    uri: string;
    name: string;
    text: string;
}

/* One row in the / or @ autocomplete popup. */
interface MenuItem {
    label: string;
    detail: string;
    resource?: MCPResource;
    prompt?: MCPPrompt;
}

/* Cap on how much resource text we fold into a single turn's context. */
const MAX_ATTACHMENT_CHARS = 20000;

/*
* Natural-language assistant: a chat thread where the LLM drives the gateway's
* tools. Every tool call renders inline as an inspectable trace card (tool name,
* arguments, result) — the Antigravity pattern — so nothing is hidden in prose.
* The LLM only ever calls gateway tools; it never gets raw SQL or credentials.
*
* The provider is pluggable (Anthropic, OpenAI/ChatGPT, Gemini, GLM, or any
* OpenAI-compatible endpoint). Credentials are entered at runtime and never
* compiled into the bundle.
*/
@Component({
    standalone: false,
    selector: 'awc-mcp-assistant',
    templateUrl: 'mcp-assistant.component.html',
    styleUrls: ['mcp-assistant.component.scss']
})
export class MCPAssistantComponent {
    @Input() mcp!: MCPState;

    presets = PROVIDER_PRESETS;
    provider: ProviderId = 'anthropic';
    baseUrl = PROVIDER_PRESETS[0].baseUrl;
    model = PROVIDER_PRESETS[0].model;
    apiKey = '';

    /* Current message being typed. */
    draft = '';

    /* Resources @-mentioned for the next turn (their contents ride along). */
    attachments: ResourceAttachment[] = [];

    /* Conversation-history drawer toggle. */
    showHistory = false;

    /* / (prompts) or @ (resources) autocomplete state. */
    menuKind: 'prompts' | 'resources' | null = null;
    menuItems: MenuItem[] = [];
    menuActive = 0;
    menuBusy = false;
    private menuReplaceStart = 0;

    constructor(private store: Store<any>, private mcpClient: MCPClientService) {}

    /* Fill base URL + model from the chosen provider's preset. */
    onProviderChange(): void {
        const preset = this.presets.find(p => p.id === this.provider);
        if (preset) {
            this.baseUrl = preset.baseUrl;
            this.model = preset.model;
        }
    }

    configure(): void {
        if (!this.apiKey.trim() || !this.baseUrl.trim() || !this.model.trim()) {
            return;
        }
        this.store.dispatch(new mcpActions.ConfigureLLM({
            provider: this.provider,
            baseUrl: this.baseUrl,
            model: this.model,
            apiKey: this.apiKey
        }));
    }

    send(): void {
        const text = this.composeTurn();
        if (!text || this.mcp.chatting) {
            return;
        }
        this.draft = '';
        this.attachments = [];
        this.closeMenu();
        this.store.dispatch(new mcpActions.SendMessage(text));
    }

    /* True when there is something to send: typed text or an attached resource. */
    get canSend(): boolean {
        return !this.mcp.chatting && (!!this.draft.trim() || this.attachments.length > 0);
    }

    /*
    * Builds the turn the model sees: each @-mentioned resource is folded in as a
    * fenced context block ahead of the user's typed text. Resources carry data,
    * not actions — the model still decides which tools to call.
    */
    private composeTurn(): string {
        const body = this.draft.trim();
        if (this.attachments.length === 0) {
            return body;
        }
        const blocks = this.attachments
            .map(a => `<resource uri="${a.uri}">\n${a.text}\n</resource>`)
            .join('\n\n');
        return body ? `${blocks}\n\n${body}` : blocks;
    }

    removeAttachment(index: number): void {
        this.attachments = this.attachments.filter((_, i) => i !== index);
    }

    clear(): void {
        this.store.dispatch(new mcpActions.ClearChat());
    }

    /* Conversations in the currently selected dataverse ("project"). */
    get scopedConversations() {
        return (this.mcp.conversations || []).filter(c => c.dataverse === this.mcp.activeDataverse);
    }

    toggleHistory(): void {
        this.showHistory = !this.showHistory;
    }

    newChat(): void {
        this.draft = '';
        this.attachments = [];
        this.closeMenu();
        this.showHistory = false;
        this.store.dispatch(new mcpActions.NewConversation());
    }

    openConversation(id: string): void {
        this.showHistory = false;
        this.store.dispatch(new mcpActions.SelectConversation(id));
    }

    deleteConversation(id: string, event: Event): void {
        event.stopPropagation();
        this.store.dispatch(new mcpActions.DeleteConversation(id));
    }

    onDataverseChange(dataverse: string): void {
        this.store.dispatch(new mcpActions.SetDataverseScope(dataverse));
    }

    /*
    * Recomputes the autocomplete menu from the token under the caret: a "/"
    * token opens the prompt picker, an "@" token the resource picker. Both are
    * fed from the catalogs already in store — no extra round trip to list them.
    */
    onDraftChange(): void {
        const match = this.draft.match(/(^|\s)([/@])([^\s]*)$/);
        if (!match) {
            this.closeMenu();
            return;
        }
        const trigger = match[2];
        const query = match[3].toLowerCase();
        this.menuReplaceStart = (match.index || 0) + match[1].length;
        this.menuActive = 0;
        this.menuKind = trigger === '/' ? 'prompts' : 'resources';
        this.menuItems = this.menuKind === 'prompts'
            ? this.promptItems(query)
            : this.resourceItems(query);
        if (this.menuItems.length === 0) {
            this.closeMenu();
        }
    }

    onComposerKeydown(event: KeyboardEvent): void {
        if (this.menuKind) {
            if (event.key === 'ArrowDown') {
                event.preventDefault();
                this.moveActive(1);
                return;
            }
            if (event.key === 'ArrowUp') {
                event.preventDefault();
                this.moveActive(-1);
                return;
            }
            if (event.key === 'Escape') {
                event.preventDefault();
                this.closeMenu();
                return;
            }
            if (event.key === 'Enter') {
                event.preventDefault();
                this.chooseActive();
                return;
            }
        } else if (event.key === 'Enter' && !event.shiftKey) {
            event.preventDefault();
            this.send();
        }
    }

    chooseActive(): void {
        const item = this.menuItems[this.menuActive];
        if (item) {
            this.selectMenuItem(item);
        }
    }

    /*
    * Applies a menu pick. A prompt is expanded (prompts/get) and its text drops
    * into the composer; a resource is read (resources/read) and attached as a
    * context chip. Either way the trigger token is stripped from the draft.
    */
    selectMenuItem(item: MenuItem): void {
        if (this.menuBusy) {
            return;
        }
        const prefix = this.draft.slice(0, this.menuReplaceStart);
        if (item.prompt) {
            this.menuBusy = true;
            this.mcpClient.getPrompt(item.prompt.name, {}).subscribe({
                next: result => {
                    this.draft = prefix + this.promptText(result);
                    this.menuBusy = false;
                    this.closeMenu();
                },
                error: () => {
                    this.menuBusy = false;
                    this.closeMenu();
                }
            });
            return;
        }
        if (item.resource) {
            const resource = item.resource;
            this.menuBusy = true;
            this.mcpClient.readResource(resource.uri).subscribe({
                next: result => {
                    this.attachments = [...this.attachments, {
                        uri: resource.uri,
                        name: resource.name || resource.uri,
                        text: this.resourceText(result)
                    }];
                    this.draft = prefix;
                    this.menuBusy = false;
                    this.closeMenu();
                },
                error: () => {
                    this.menuBusy = false;
                    this.closeMenu();
                }
            });
        }
    }

    private moveActive(delta: number): void {
        const count = this.menuItems.length;
        if (count === 0) {
            return;
        }
        this.menuActive = (this.menuActive + delta + count) % count;
    }

    private closeMenu(): void {
        this.menuKind = null;
        this.menuItems = [];
        this.menuActive = 0;
    }

    private promptItems(query: string): MenuItem[] {
        return (this.mcp.prompts || [])
            .filter(p => p.name.toLowerCase().includes(query)
                || (p.description || '').toLowerCase().includes(query))
            .map(p => ({ label: p.name, detail: p.description || '', prompt: p }));
    }

    private resourceItems(query: string): MenuItem[] {
        return (this.mcp.resources || [])
            .filter(r => (r.name || '').toLowerCase().includes(query)
                || r.uri.toLowerCase().includes(query))
            .map(r => ({ label: r.name || r.uri, detail: r.uri, resource: r }));
    }

    /* Flattens a prompts/get envelope into plain text. */
    private promptText(result: any): string {
        const messages = (result && result.messages) || [];
        return messages
            .map((message: any) => this.contentText(message && message.content))
            .filter((entry: string) => !!entry)
            .join('\n\n');
    }

    /* Flattens a resources/read envelope into plain text, capped for context. */
    private resourceText(result: any): string {
        const contents = (result && result.contents) || [];
        const text = contents
            .map((entry: any) => entry && (entry.text || ''))
            .filter((entry: string) => !!entry)
            .join('\n\n');
        return text.length > MAX_ATTACHMENT_CHARS
            ? text.slice(0, MAX_ATTACHMENT_CHARS) + '\n…[truncated]'
            : text;
    }

    private contentText(content: any): string {
        if (Array.isArray(content)) {
            return content.filter((part: any) => part && part.type === 'text')
                .map((part: any) => part.text).join('\n');
        }
        if (content && content.type === 'text') {
            return content.text;
        }
        return typeof content === 'string' ? content : '';
    }

    trackByIndex(index: number): number {
        return index;
    }

    isUser(message: ChatMessage): boolean {
        return message.role === 'user';
    }

    isAssistant(message: ChatMessage): boolean {
        return message.role === 'assistant';
    }

    isTool(message: ChatMessage): boolean {
        return message.role === 'tool';
    }
}
