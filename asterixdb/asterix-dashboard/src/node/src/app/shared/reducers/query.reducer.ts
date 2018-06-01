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
import * as sqlQueryActions from '../actions/query.actions';

export type Action = sqlQueryActions.All;

/*
** Interfaces for sql++ queries in store/state
*/
export interface State {
  loading: boolean,
  loaded: boolean,
  success: boolean,
  sqlQueryString: string,
  sqlQueryResult: AsterixDBQueryMessage[],
  sqlQueryError: AsterixDBQueryMessage[]
};

const initialState: State = {
  loading: false,
  loaded: false,
  success: false,
  sqlQueryString: "",
  sqlQueryResult: [],
  sqlQueryError: []
};

/*
** Reducer function for sql++ queries in store/state
*/
export function sqlReducer(state = initialState, action: Action) {
  switch (action.type) {

    /*
    * Change the load state to true, and clear previous results
    * to signaling that a EXECUTE a SQL++ Query is ongoing
    */
    case sqlQueryActions.EXECUTE_QUERY: {
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
    * Change the load state to false, and loaded to true to signaling
    * that a EXECUTE Query is success and there is data available in the
    * store
    */
    case sqlQueryActions.EXECUTE_QUERY_SUCCESS: {
      return Object.assign({}, state, {
        loading: false,
        loaded: true,
        success: true,
        sqlQueryResult: action.payload,
        sqlQueryError: []
      })
    }

    /*
    * Change the load state to false, and loaded to true to signaling
    * that a EXECUTE Query is failed and there is error data available in the
    * store
    */
    case sqlQueryActions.EXECUTE_QUERY_FAIL: {
      return Object.assign({}, state, {
        loading: false,
        loaded: true,
        success: false,
        sqlQueryResult: [],
        sqlQueryError: action.payload
      })
    }
    
    /*
    * Just returns the current store/state object
    */
    default:
      return state;
  }
}
