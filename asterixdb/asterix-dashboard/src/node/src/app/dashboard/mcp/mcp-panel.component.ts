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
import { Component, OnInit, NgZone, ChangeDetectorRef } from '@angular/core';
import { Store } from '@ngrx/store';
import { Observable } from 'rxjs';
import * as mcpActions from '../../shared/actions/mcp.actions';
import { MCPTool, MCPResource, MCPPrompt } from '../../shared/services/mcp-client.service';
import { State as MCPState } from '../../shared/reducers/mcp.reducer';

/*
* The MCP side panel: a toggleable, right-docked client of the gateway. It
* lists the gateway's tools (tools/list), builds an argument editor from each
* tool's inputSchema, invokes them (tools/call), and renders the bounded JSON
* result. It is read-only presentation over the gateway — it inherits every
* safety boundary the gateway enforces and adds no authority of its own.
*/
const MIN_PANEL_WIDTH = 300;
const MAX_PANEL_WIDTH = 900;
const DEFAULT_PANEL_WIDTH = 420;

@Component({
    standalone: false,
    selector: 'awc-mcp-panel',
    templateUrl: 'mcp-panel.component.html',
    styleUrls: ['mcp-panel.component.scss']
})
export class MCPPanelComponent implements OnInit {
    mcp$: Observable<MCPState>;

    /* Connect form. The token is entered here, never baked into the bundle. */
    endpoint = '/mcp';
    token = '';

    /* Editable JSON arguments for the selected tool. */
    argsText = '{}';

    /* Editable JSON arguments for the selected prompt. */
    promptArgsText = '{}';

    /* Side-by-side panel width (px), adjusted by dragging the left edge. */
    panelWidth = DEFAULT_PANEL_WIDTH;
    resizing = false;
    private startX = 0;
    private startWidth = 0;
    private readonly moveHandler = (event: MouseEvent) => this.onResizeMove(event);
    private readonly upHandler = () => this.stopResize();

    /* Tracks open state so we only nudge on actual open/close transitions. */
    private lastOpen: boolean | undefined;

    constructor(private store: Store<any>, private zone: NgZone, private cdr: ChangeDetectorRef) {
        this.mcp$ = this.store.select((state: any) => state.mcp);
    }

    ngOnInit(): void {
        // Opening/closing the panel changes the layout width of the query/plan
        // columns. ngx-graph (plan viewer) only re-fits on window resize, so
        // fire one after the width animation settles to auto-rescale the graph.
        this.mcp$.subscribe(state => {
            const open = !!(state && state.open);
            if (open !== this.lastOpen) {
                this.lastOpen = open;
                setTimeout(() => window.dispatchEvent(new Event('resize')), 250);
            }
        });
    }

    startResize(event: MouseEvent): void {
        event.preventDefault();
        this.resizing = true;
        this.startX = event.clientX;
        this.startWidth = this.panelWidth;
        document.addEventListener('mousemove', this.moveHandler);
        document.addEventListener('mouseup', this.upHandler);
    }

    private onResizeMove(event: MouseEvent): void {
        if (!this.resizing) {
            return;
        }
        // Panel is docked on the right, so dragging left (smaller clientX) widens it.
        const delta = this.startX - event.clientX;
        const next = this.startWidth + delta;
        const width = Math.min(Math.max(next, MIN_PANEL_WIDTH), MAX_PANEL_WIDTH);
        // The mousemove fires outside Angular's change detection, so update the
        // width binding inside the zone and force a render to make the drag live.
        this.zone.run(() => {
            this.panelWidth = width;
            this.cdr.detectChanges();
        });
    }

    private stopResize(): void {
        this.resizing = false;
        document.removeEventListener('mousemove', this.moveHandler);
        document.removeEventListener('mouseup', this.upHandler);
        // Manual drag also changed the layout width — re-fit the plan graph.
        window.dispatchEvent(new Event('resize'));
    }

    close(): void {
        this.store.dispatch(new mcpActions.TogglePanel(false));
    }

    setView(view: 'assistant' | 'tools' | 'resources' | 'prompts'): void {
        this.store.dispatch(new mcpActions.SetView(view));
    }

    connect(): void {
        this.store.dispatch(new mcpActions.Connect({ endpoint: this.endpoint, token: this.token }));
    }

    disconnect(): void {
        this.store.dispatch(new mcpActions.Disconnect());
    }

    selectTool(tool: MCPTool): void {
        this.argsText = this.templateForSchema(tool.inputSchema);
        this.store.dispatch(new mcpActions.SelectTool(tool));
    }

    invoke(tool: MCPTool): void {
        let args: any;
        try {
            args = this.argsText.trim() ? JSON.parse(this.argsText) : {};
        } catch (error) {
            this.store.dispatch(new mcpActions.CallToolFail('Arguments are not valid JSON.'));
            return;
        }
        this.store.dispatch(new mcpActions.CallTool({ name: tool.name, args }));
    }

    /*
    * Selecting a resource immediately reads it (resources/read) so its contents
    * render in the detail pane.
    */
    selectResource(resource: MCPResource): void {
        this.store.dispatch(new mcpActions.SelectResource(resource));
        this.store.dispatch(new mcpActions.ReadResource(resource.uri));
    }

    /*
    * Selecting a prompt prefills its argument editor from the declared
    * arguments (required ones first) so the user fills values, not key names.
    */
    selectPrompt(prompt: MCPPrompt): void {
        this.promptArgsText = this.templateForPromptArgs(prompt);
        this.store.dispatch(new mcpActions.SelectPrompt(prompt));
    }

    getPrompt(prompt: MCPPrompt): void {
        let args: any;
        try {
            args = this.promptArgsText.trim() ? JSON.parse(this.promptArgsText) : {};
        } catch (error) {
            this.store.dispatch(new mcpActions.GetPromptFail('Arguments are not valid JSON.'));
            return;
        }
        this.store.dispatch(new mcpActions.GetPrompt({ name: prompt.name, args }));
    }

    /*
    * Two-step rule: a prompt only produces text. Send that text to the
    * assistant as the next user turn; the model still decides which tools to
    * call. Never auto-invokes anything.
    */
    sendPromptToAssistant(promptResult: any): void {
        const text = this.promptText(promptResult);
        if (!text.trim()) {
            return;
        }
        this.store.dispatch(new mcpActions.SetView('assistant'));
        this.store.dispatch(new mcpActions.SendMessage(text));
    }

    /*
    * Flattens a prompts/get envelope into plain text. Each message contributes
    * its text content; non-text parts are skipped.
    */
    promptText(promptResult: any): string {
        const messages = (promptResult && promptResult.messages) || [];
        return messages
            .map((message: any) => {
                const content = message && message.content;
                if (Array.isArray(content)) {
                    return content.filter((part: any) => part && part.type === 'text')
                        .map((part: any) => part.text).join('\n');
                }
                if (content && content.type === 'text') {
                    return content.text;
                }
                return typeof content === 'string' ? content : '';
            })
            .filter((entry: string) => !!entry)
            .join('\n\n');
    }

    private templateForPromptArgs(prompt: MCPPrompt): string {
        const args = (prompt && prompt.arguments) || [];
        if (args.length === 0) {
            return '{}';
        }
        const template: any = {};
        args.forEach(argument => {
            template[argument.name] = '';
        });
        return JSON.stringify(template, null, 2);
    }

    /*
    * Builds a starter arguments object from a tool's JSON Schema so the editor
    * is prefilled with the expected keys instead of an empty brace. A full
    * schema-driven form is the documented fast-follow.
    */
    private templateForSchema(schema: any): string {
        if (!schema || !schema.properties) {
            return '{}';
        }
        const required: string[] = schema.required || [];
        const template: any = {};
        Object.keys(schema.properties).forEach(key => {
            if (required.indexOf(key) !== -1) {
                template[key] = this.defaultForType(schema.properties[key]);
            }
        });
        return JSON.stringify(template, null, 2);
    }

    private defaultForType(property: any): any {
        switch (property && property.type) {
            case 'integer':
            case 'number':
                return 0;
            case 'boolean':
                return false;
            case 'array':
                return [];
            case 'object':
                return {};
            default:
                return '';
        }
    }
}
