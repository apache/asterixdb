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
import { ChatMessage } from '../reducers/mcp.reducer';

/*
* A persisted conversation. messages drive the rendered transcript;
* providerHistory is the opaque provider-shaped turn list that lets the LLM
* continue an old conversation with full context. dataverse is the "project"
* key the sidebar groups by.
*/
export interface Conversation {
    id: string;
    title: string;
    dataverse: string;
    messages: ChatMessage[];
    /* The provider whose wire format providerHistory is shaped for. Reopening
       under a different provider must reset rather than replay this. */
    provider: string;
    providerHistory: any[];
    createdAt: number;
    updatedAt: number;
}

/* Lightweight conversation metadata for the sidebar (no transcript payload). */
export interface ConversationMeta {
    id: string;
    title: string;
    dataverse: string;
    updatedAt: number;
}

const DB_NAME = 'asterix-mcp-history';
const DB_VERSION = 1;
const STORE = 'conversations';
const INDEX_DATAVERSE = 'by_dataverse';
const INDEX_UPDATED = 'by_updated';

/*
* Device-local chat-history store backed by IndexedDB. Local-first and
* dataverse-scoped — the same model Claude Code and Antigravity use, where the
* project is the unit of organization (here the dataverse). Survives cluster
* restarts and page reloads; nothing leaves the browser. The read-only gateway
* boundary is untouched — history never round-trips through the cluster.
*/
@Injectable()
export class ChatHistoryService {
    private dbPromise: Promise<IDBDatabase> | null = null;

    /* Opens (and on first run, creates) the database. Memoized. */
    private open(): Promise<IDBDatabase> {
        if (this.dbPromise) {
            return this.dbPromise;
        }
        this.dbPromise = new Promise<IDBDatabase>((resolve, reject) => {
            const request = indexedDB.open(DB_NAME, DB_VERSION);
            request.onupgradeneeded = () => {
                const db = request.result;
                if (!db.objectStoreNames.contains(STORE)) {
                    const store = db.createObjectStore(STORE, { keyPath: 'id' });
                    store.createIndex(INDEX_DATAVERSE, 'dataverse', { unique: false });
                    store.createIndex(INDEX_UPDATED, 'updatedAt', { unique: false });
                }
            };
            request.onsuccess = () => resolve(request.result);
            request.onerror = () => reject(request.error);
        });
        return this.dbPromise;
    }

    /* Lists conversation metadata, newest first. */
    async list(): Promise<ConversationMeta[]> {
        const db = await this.open();
        return new Promise<ConversationMeta[]>((resolve, reject) => {
            const tx = db.transaction(STORE, 'readonly');
            const request = tx.objectStore(STORE).getAll();
            request.onsuccess = () => {
                const all = (request.result as Conversation[]) || [];
                const meta = all
                    .map(c => ({ id: c.id, title: c.title, dataverse: c.dataverse, updatedAt: c.updatedAt }))
                    .sort((a, b) => b.updatedAt - a.updatedAt);
                resolve(meta);
            };
            request.onerror = () => reject(request.error);
        });
    }

    /* Loads one full conversation, or null when it no longer exists. */
    async get(id: string): Promise<Conversation | null> {
        const db = await this.open();
        return new Promise<Conversation | null>((resolve, reject) => {
            const tx = db.transaction(STORE, 'readonly');
            const request = tx.objectStore(STORE).get(id);
            request.onsuccess = () => resolve((request.result as Conversation) || null);
            request.onerror = () => reject(request.error);
        });
    }

    /* Inserts or replaces a conversation. */
    async put(conversation: Conversation): Promise<void> {
        const db = await this.open();
        return new Promise<void>((resolve, reject) => {
            const tx = db.transaction(STORE, 'readwrite');
            tx.objectStore(STORE).put(conversation);
            tx.oncomplete = () => resolve();
            tx.onerror = () => reject(tx.error);
        });
    }

    async delete(id: string): Promise<void> {
        const db = await this.open();
        return new Promise<void>((resolve, reject) => {
            const tx = db.transaction(STORE, 'readwrite');
            tx.objectStore(STORE).delete(id);
            tx.oncomplete = () => resolve();
            tx.onerror = () => reject(tx.error);
        });
    }
}
