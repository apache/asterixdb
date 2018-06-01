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
import { AsterixDBQueryMessage } from '../models/asterixDB.model';
import * as appActions from '../actions/app.actions';

export type Action = appActions.All;

/*
** Interfaces for app state in store/state
*/
export interface State {
  loading: boolean,
  loaded: boolean,
  success: boolean,
  sqlQueryString: string,
  sqlQueryResult: AsterixDBQueryMessage[],
  sqlQueryError: AsterixDBQueryMessage[],
  sqlMetadataQueryString: string,
  sqlMetadataQueryResult: AsterixDBQueryMessage[],
  sqlMetadataQueryError: AsterixDBQueryMessage[]
};

const initialState: State = {
  loading: false,
  loaded: false,
  success: false,
  sqlQueryString: "",
  sqlQueryResult: [],
  sqlQueryError: [],
  sqlMetadataQueryString: "",
  sqlMetadataQueryResult: [],
  sqlMetadataQueryError: [],
};

/*
** Reducer function for app state in store/state
*/
export function appReducer(state = initialState, action: Action) {
  switch (action.type) {

    /*
    * Change the load state to true, and clear previous results
    * to signaling that a EXECUTE a SQL++ Query is ongoing
    */
    case appActions.APP_MODE_CHANGE: {
      return Object.assign({}, state, {
        loading: false,
        loaded: true,
        success: false,
        sqlQueryString: action.payload,
        sqlQueryResult: [],
        sqlQueryError: []
      });
    }
    
    /*
    * Just returns the current store/state object
    */
    default:
      return state;
  }
}
