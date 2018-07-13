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
* Definition of Datatypes Actions
*/
export const SELECT_DATATYPES         = '[Datatype Collection] Select Datatypes';
export const SELECT_DATATYPES_SUCCESS = '[Datatype Collection] Select Datatypes Success';
export const SELECT_DATATYPES_FAIL    = '[Datatype Collection] Select Datatypes Fail';
export const CREATE_DATATYPE          = '[Datatype Collection] Create Datatypes';
export const CREATE_DATATYPE_SUCCESS  = '[Datatype Collection] Create Datatypes Success';
export const CREATE_DATATYPE_FAIL     = '[Datatype Collection] Create Datatypes Fail';
export const UPDATE_DATATYPE          = '[Datatype Collection] Update Datatype';
export const UPDATE_DATATYPE_SUCCESS  = '[Datatype Collection] Update Datatype Success';
export const UPDATE_DATATYPE_FAIL     = '[Datatype Collection] Update Datatype Fail';
export const DROP_DATATYPE            = '[Datatype Collection] Drop Datatypes';
export const DROP_DATATYPE_SUCCESS    = '[Datatype Collection] Drop Datatypes Success';
export const DROP_DATATYPE_FAIL       = '[Datatype Collection] Drop Datatypes Fail';

/*
* Select Datatypes
*/
export class SelectDatatypes implements Action {
    readonly type = SELECT_DATATYPES;
    constructor(public payload: string) {}
}

export class SelectDatatypesSuccess implements Action {
    readonly type = SELECT_DATATYPES_SUCCESS;
    constructor(public payload: any[]) {}
}

export class SelectDatatypesFail implements Action {
    readonly type = SELECT_DATATYPES_FAIL;
    constructor(public payload: any[]) {}
}

/*
* Create Datatype
*/
export class CreateDatatype implements Action {
    readonly type = CREATE_DATATYPE;
    constructor(public payload: string) {}
}

export class CreateDatatypeSuccess implements Action {
    readonly type = CREATE_DATATYPE_SUCCESS;
    constructor(public payload: any[]) {}
}

export class CreateDatatypeFail implements Action {
    readonly type = CREATE_DATATYPE_FAIL;
    constructor(public payload: any) {}
}

/*
* Update Datatype
*/
export class UpdateDatatype implements Action {
    readonly type = UPDATE_DATATYPE;
    constructor(public payload: any) {}
}

export class UpdateDatatypeSuccess implements Action {
    readonly type = UPDATE_DATATYPE_SUCCESS;
    constructor(public payload: any[]) {}
}

export class UpdateDatatypeFail implements Action {
    readonly type = UPDATE_DATATYPE_FAIL;
    constructor(public payload: any) {}
}

/*
* Drop Datatype
*/
export class DropDatatype implements Action {
    readonly type = DROP_DATATYPE;

    constructor(public payload: string) {}
}

export class DropDatatypeSuccess implements Action {
    readonly type = DROP_DATATYPE_SUCCESS;

    constructor(public payload: any[]) {}
}

export class DropDatatypeFail implements Action {
    readonly type = DROP_DATATYPE_FAIL;

    constructor(public payload: any) {}
}

/*
* Exports of datastypes actions
*/
export type All = SelectDatatypes |
    SelectDatatypesSuccess |
    SelectDatatypesFail |
    CreateDatatype |
    CreateDatatypeSuccess |
    CreateDatatypeFail |
    UpdateDatatype |
    UpdateDatatypeSuccess |
    UpdateDatatypeFail |
    DropDatatype |
    DropDatatypeSuccess |
    DropDatatypeFail;