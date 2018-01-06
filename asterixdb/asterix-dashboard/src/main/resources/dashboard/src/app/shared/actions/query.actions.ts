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
import { AsterixDBQueryMessage } from '../models/asterixDB.model';

/*
* Definition of SQL++ Actions
*/
export const EXECUTE_QUERY                  = '[Query] Execute SQL++ Query';
export const EXECUTE_QUERY_SUCCESS          = '[Query] Execute SQL++ Query Success';
export const EXECUTE_QUERY_FAIL             = '[Query] Execute SQL++ Query Fail';
export const EXECUTE_METADATA_QUERY         = '[Query] Execute Metadata SQL++ Query';
export const EXECUTE_METADATA_QUERY_SUCCESS = '[Query] Execute Metadata SQL++ Query Success';
export const EXECUTE_METADATA_QUERY_FAIL     = '[Query] Execute Metadata SQL++ Query Fail';

/*
* Execute SQL++ Query
*/
export class ExecuteQuery implements Action {
  readonly type = EXECUTE_QUERY;
  constructor(public payload: string) {} // the AsterixDB Query String
}

export class ExecuteQuerySuccess implements Action {
  readonly type = EXECUTE_QUERY_SUCCESS;
  constructor(public payload: AsterixDBQueryMessage[]) {}
}

export class ExecuteQueryFail implements Action {
  readonly type = EXECUTE_QUERY_FAIL;
  constructor(public payload: AsterixDBQueryMessage[]) {}
}

/*
* Execute Metadata SQL++ Query
*/
export class ExecuteMetadataQuery implements Action {
  readonly type = EXECUTE_METADATA_QUERY;
  constructor(public payload: string) {} // the AsterixDB Query String
}

export class ExecuteMetadataQuerySuccess implements Action {
  readonly type = EXECUTE_METADATA_QUERY_SUCCESS;
  constructor(public payload: AsterixDBQueryMessage[]) {}
}

export class ExecuteMetadataQueryFail implements Action {
  readonly type = EXECUTE_METADATA_QUERY_FAIL;
  constructor(public payload: AsterixDBQueryMessage[]) {}
}

/*
* Exports of SQL++ actions
*/
export type All = ExecuteQuery |
  ExecuteQuerySuccess |
  ExecuteQueryFail |
  ExecuteMetadataQuery |
  ExecuteMetadataQuerySuccess |
  ExecuteMetadataQueryFail;