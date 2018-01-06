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
import { AsterixDBQueryMessage, Dataset } from '../models/asterixDB.model';

/*
* Definition of App Actions
*/
export const APP_MODE_CHANGE = '[App State] App Mode Change';

/*
* Guide Select Datasets for UI Helpers
*/
export class ChangeMode implements Action {
  readonly type = APP_MODE_CHANGE;
  constructor(public payload: string) {}
}

/*
* Exports of datasets actions
*/
export type All = ChangeMode;
