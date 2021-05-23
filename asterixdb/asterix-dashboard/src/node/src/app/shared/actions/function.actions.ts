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
* Definition of Function Actions
*/
export const SELECT_FUNCTIONS         = '[Function Collection] Select Functions';
export const SELECT_FUNCTIONS_SUCCESS = '[Function Collection] Select Functions Success';
export const SELECT_FUNCTIONS_FAIL    = '[Function Collection] Select Functions Fail';

/*
* Select Functions
*/
export class SelectFunctions implements Action {
  readonly type = SELECT_FUNCTIONS;
  constructor(public payload: string) {}
}

export class SelectFunctionsSuccess implements Action {
  readonly type = SELECT_FUNCTIONS_SUCCESS;
  constructor(public payload: any[]) {}
}

export class SelectFunctionsFail implements Action {
  readonly type = SELECT_FUNCTIONS_FAIL;
  constructor(public payload: any[]) {}
}

/*
* Exports of functions actions
*/
export type All = SelectFunctions |
  SelectFunctionsSuccess |
  SelectFunctionsFail;
