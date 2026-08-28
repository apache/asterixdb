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
import { HttpClient, HttpHeaders, HttpResponse } from '@angular/common/http';
import { Observable, throwError } from 'rxjs';
import { map, switchMap, catchError } from 'rxjs/operators';

/*
* Connection parameters supplied by the user when they open the panel.
* The token is never compiled into the bundle; it is entered at connect time.
*/
export interface MCPConnection {
    endpoint: string;
    token: string;
}

/*
* A single MCP tool as returned by tools/list. inputSchema is JSON Schema and
* drives the generated form in the panel.
*/
export interface MCPTool {
    name: string;
    description?: string;
    inputSchema?: any;
}

/*
* A read-only resource exposed by the gateway (resources/list). The agent or
* user attaches it to context by URI; it carries data, never an action.
*/
export interface MCPResource {
    uri: string;
    name?: string;
    description?: string;
    mimeType?: string;
}

/*
* A parameterized resource URI (resources/templates/list). The {placeholders}
* are filled in by the client before the URI is read.
*/
export interface MCPResourceTemplate {
    uriTemplate: string;
    name?: string;
    description?: string;
    mimeType?: string;
}

/*
* One declared argument of a prompt (prompts/list). Drives the prompt's
* argument form in the panel.
*/
export interface MCPPromptArgument {
    name: string;
    description?: string;
    required?: boolean;
}

/*
* A prompt template exposed by the gateway (prompts/list). prompts/get expands
* it into messages the user reasons about — it never auto-executes a tool.
*/
export interface MCPPrompt {
    name: string;
    description?: string;
    arguments?: MCPPromptArgument[];
}

const PROTOCOL_VERSION = '2025-03-26';
const CLIENT_INFO = { name: 'asterixdb-web-console', version: '1.0.0' };
const SESSION_HEADER = 'Mcp-Session-Id';

/*
* Minimal MCP client over the Streamable HTTP transport. The browser cannot
* open a stdio pipe, so the panel talks JSON-RPC to the gateway's HTTP endpoint
* directly (design Option B). This service owns the session: connect, listTools,
* callTool. It adds no authority of its own — every call is bounded by whatever
* the gateway already enforces (read-only, egress ceilings, auth token).
*/
@Injectable()
export class MCPClientService {
    private endpoint = '';
    private token = '';
    private sessionId = '';
    private nextId = 1;

    constructor(private http: HttpClient) {}

    /*
    * Opens a session: initialize handshake, capture the session id, then send
    * the initialized notification. Resolves once the gateway is ready.
    */
    connect(connection: MCPConnection): Observable<any> {
        this.endpoint = connection.endpoint.trim();
        this.token = connection.token.trim();
        this.sessionId = '';
        this.nextId = 1;

        const params = {
            protocolVersion: PROTOCOL_VERSION,
            capabilities: {},
            clientInfo: CLIENT_INFO
        };
        return this.rpc('initialize', params).pipe(
            switchMap(result => this.notify('notifications/initialized').pipe(map(() => result)))
        );
    }

    /*
    * Lists the tools the gateway exposes. The panel renders one entry per tool
    * and builds its form from the returned inputSchema.
    */
    listTools(): Observable<MCPTool[]> {
        return this.rpc('tools/list', {}).pipe(map(result => (result && result.tools) || []));
    }

    /*
    * Invokes a tool by name with the given arguments and returns the raw MCP
    * result envelope (content + structuredContent + isError).
    */
    callTool(name: string, args: any): Observable<any> {
        return this.rpc('tools/call', { name, arguments: args || {} });
    }

    /*
    * Lists the read-only resources the gateway exposes. The panel renders one
    * entry per resource; reading a resource attaches its contents to context.
    */
    listResources(): Observable<MCPResource[]> {
        return this.rpc('resources/list', {}).pipe(map(result => (result && result.resources) || []));
    }

    /*
    * Lists the parameterized resource URIs (resources/templates/list). Returns
    * an empty list when the gateway advertises no templates.
    */
    listResourceTemplates(): Observable<MCPResourceTemplate[]> {
        return this.rpc('resources/templates/list', {}).pipe(
            map(result => (result && result.resourceTemplates) || [])
        );
    }

    /*
    * Reads a single resource by URI and returns its contents array (text or
    * blob entries, each tagged with its mimeType).
    */
    readResource(uri: string): Observable<any> {
        return this.rpc('resources/read', { uri });
    }

    /*
    * Lists the prompt templates the gateway exposes. The panel renders one
    * entry per prompt and builds an argument form from its declared arguments.
    */
    listPrompts(): Observable<MCPPrompt[]> {
        return this.rpc('prompts/list', {}).pipe(map(result => (result && result.prompts) || []));
    }

    /*
    * Expands a prompt by name with the given arguments and returns the messages
    * envelope (description + messages). The result is text for the user to
    * reason about — it never auto-invokes a tool (the two-step rule).
    */
    getPrompt(name: string, args: any): Observable<any> {
        return this.rpc('prompts/get', { name, arguments: args || {} });
    }

    /*
    * Sends a JSON-RPC request and resolves its result. The session id header
    * returned by initialize is echoed on every subsequent call.
    */
    private rpc(method: string, params: any): Observable<any> {
        const body = { jsonrpc: '2.0', id: this.nextId++, method, params };
        return this.http.post(this.endpoint, body, {
            headers: this.headers(),
            observe: 'response',
            responseType: 'text'
        }).pipe(
            map((response: HttpResponse<string>) => this.handle(response)),
            catchError((error: any) => this.handleError(error))
        );
    }

    /*
    * Fire-and-forget notification (no id, no result expected).
    */
    private notify(method: string): Observable<any> {
        const body = { jsonrpc: '2.0', method, params: {} };
        return this.http.post(this.endpoint, body, {
            headers: this.headers(),
            responseType: 'text'
        }).pipe(catchError(() => throwError(() => 'MCP notify failed')));
    }

    private headers(): HttpHeaders {
        let headers = new HttpHeaders({
            'Content-Type': 'application/json',
            'Accept': 'application/json, text/event-stream'
        });
        if (this.token) {
            headers = headers.append('Authorization', `Bearer ${this.token}`);
        }
        if (this.sessionId) {
            headers = headers.append(SESSION_HEADER, this.sessionId);
        }
        return headers;
    }

    /*
    * Captures the session id (set by initialize) and unwraps the JSON-RPC
    * envelope. The Streamable HTTP transport may answer as plain JSON or as a
    * single SSE event; parseEnvelope handles both.
    */
    private handle(response: HttpResponse<string>): any {
        const issued = response.headers.get(SESSION_HEADER);
        if (issued) {
            this.sessionId = issued;
        }
        const envelope = this.parseEnvelope(response.body || '');
        if (envelope && envelope.error) {
            throw envelope.error.message || 'MCP error';
        }
        return envelope ? envelope.result : null;
    }

    private parseEnvelope(text: string): any {
        const trimmed = text.trim();
        if (!trimmed) {
            return null;
        }
        if (trimmed.charAt(0) === '{') {
            return JSON.parse(trimmed);
        }
        // SSE framing: take the JSON after the last "data:" line.
        const dataLines = trimmed.split('\n').filter(line => line.startsWith('data:'));
        if (dataLines.length === 0) {
            return null;
        }
        return JSON.parse(dataLines[dataLines.length - 1].slice('data:'.length).trim());
    }

    private handleError(error: any): Observable<never> {
        console.log('mcpClientError:');
        console.log(error);
        const message = typeof error === 'string' ? error : (error && error.message) || 'MCP request failed';
        return throwError(() => message);
    }
}
