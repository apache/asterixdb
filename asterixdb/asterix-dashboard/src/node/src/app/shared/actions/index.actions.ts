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
* Definition of Index Actions
*/
export const SELECT_INDEXES         = '[Index Collection] Select Indexes';
export const SELECT_INDEXES_SUCCESS = '[Index Collection] Select Indexes Success';
export const SELECT_INDEXES_FAIL    = '[Index Collection] Select Indexes Fail';
export const CREATE_INDEX           = '[Index Collection] Create Index';
export const CREATE_INDEX_SUCCESS   = '[Index Collection] Create Index Success';
export const CREATE_INDEX_FAIL      = '[Index Collection] Create Index Fail';
export const UPDATE_INDEX           = '[Index Collection] Update Index';
export const UPDATE_INDEX_SUCCESS   = '[Index Collection] Update Index Success';
export const UPDATE_INDEX_FAIL      = '[Index Collection] Update Index Fail';
export const DROP_INDEX             = '[Index Collection] Drop Indexes';
export const DROP_INDEX_SUCCESS     = '[Index Collection] Drop Indexes Success';
export const DROP_INDEX_FAIL        = '[Index Collection] Drop Indexes Fail';

/*
* Select Indexes
*/
export class SelectIndexes implements Action {
    readonly type = SELECT_INDEXES;
    constructor(public payload: string) {}
}

export class SelectIndexesSuccess implements Action {
    readonly type = SELECT_INDEXES_SUCCESS;
    constructor(public payload: any[]) {}
}

export class SelectIndexesFail implements Action {
    readonly type = SELECT_INDEXES_FAIL;
    constructor(public payload: any[]) {}
}

/*
* Create Index
*/
export class CreateIndex implements Action {
    readonly type = CREATE_INDEX;
    constructor(public payload: string) {}
}

export class CreateIndexSuccess implements Action {
    readonly type = CREATE_INDEX_SUCCESS;
    constructor(public payload: any[]) {}
}

export class CreateIndexFail implements Action {
    readonly type = CREATE_INDEX_FAIL;
    constructor(public payload: any) {}
}

/*
* Update Index
*/
export class UpdateIndex implements Action {
    readonly type = UPDATE_INDEX;
    constructor(public payload: any) {}
}

export class UpdateIndexSuccess implements Action {
    readonly type = UPDATE_INDEX_SUCCESS;
    constructor(public payload: any[]) {}
}

export class UpdateIndexFail implements Action {
    readonly type = UPDATE_INDEX_FAIL;
    constructor(public payload: any) {}
}

/*
* Remove Index
*/
export class DropIndex implements Action {
    readonly type = DROP_INDEX;
    constructor(public payload: string) {}
}

export class DropIndexSuccess implements Action {
    readonly type = DROP_INDEX_SUCCESS;
    constructor(public payload: any[]) {}
}

export class DropIndexFail implements Action {
    readonly type = DROP_INDEX_FAIL;
    constructor(public payload: any) {}
}

/*
* Exports of indexes actions
*/
export type All = SelectIndexes |
    SelectIndexesSuccess |
    SelectIndexesFail |
    CreateIndex |
    CreateIndexSuccess |
    CreateIndexFail |
    UpdateIndex |
    UpdateIndexSuccess |
    UpdateIndexFail |
    DropIndex |
    DropIndexSuccess |
    DropIndexFail;