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

export const CANCEL_QUERY                       = '[Query] Cancel SQL++ Query';
export const CANCEL_QUERY_SUCCESS               = '[Query] Cancel SQL++ Query Success';
export const CANCEL_QUERY_FAIL                  = '[Query] Cancel SQL++ Query Fail';

/*
* Cancel SQL++ Query
 */
export class CancelQuery implements Action {
  readonly type = CANCEL_QUERY;
  constructor(public payload: any) {}
}

export class CancelQuerySuccess implements Action {
  readonly type = CANCEL_QUERY_SUCCESS;
  constructor(public payload: any) {}
}

export class CancelQueryFail implements Action {
  readonly type = CANCEL_QUERY_FAIL;
  constructor(public payload: any) {}
}

/*
* Exports of SQL++ actions
*/
export type All = CancelQuery |
  CancelQuerySuccess |
  CancelQueryFail;
