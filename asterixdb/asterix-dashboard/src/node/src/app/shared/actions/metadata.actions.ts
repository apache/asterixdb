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
* Definition of Metadata Tree Actions
*/
export const UPDATE_METADATA_TREE         = '[Metadata Tree Query] UPDATE Metadata tree';
export const UPDATE_METADATA_TREE_SUCCESS = '[Metadata Tree Query] UPDATE Metadata tree Success';
export const UPDATE_METADATA_TREE_FAIL    = '[Metadata Tree Query] UPDATE Metadata tree Fail';

/*
* Construct Metadata Tree Actions
*/
export class UpdateMetadataTree implements Action {
  readonly type = UPDATE_METADATA_TREE
  constructor() {}
}

export class UpdateMetadataTreeSuccess implements Action {
  readonly type = UPDATE_METADATA_TREE_SUCCESS;
  constructor(public payload: any) {}
}

export class UpdateMetadataTreeFail implements Action {
  readonly type = UPDATE_METADATA_TREE_FAIL;
  constructor(public payload: any) {}
}

/*
* Exports of Metatada Tree actions
*/
export type All = UpdateMetadataTree |
    UpdateMetadataTreeSuccess |
    UpdateMetadataTreeFail;
