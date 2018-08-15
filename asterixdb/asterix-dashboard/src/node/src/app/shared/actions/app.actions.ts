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

/*
* Definition of App Actions
*/
export const APP_MODE_CHANGE = '[App State] App Mode Change';
export const APP_SIDE_MENU = '[App State] App Side Menu Mode Change';
export const APP_QUERY_INPUT_INDEX = '[App State] App Query Input Index';
export const APP_ACTIVE_DATAVERSE = '[App State] App Active Dataverse';

/*
* Guide Select Datasets for UI Helpers
*/
export class ChangeMode implements Action {
    readonly type = APP_MODE_CHANGE;
    constructor(public payload: string) {}
}

export class setEditorIndex implements Action {
    readonly type = APP_QUERY_INPUT_INDEX;
    constructor(public payload: string) {}
}

export class setSideMenuVisible implements Action {
    readonly type = APP_SIDE_MENU;
    constructor(public payload: boolean) {}
}

/*
* Exports of datasets actions
*/
export type All = ChangeMode |
    setEditorIndex |
    setSideMenuVisible;