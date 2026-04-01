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
import * as sqlQueryActions from '../actions/query.actions';

export type Action = sqlQueryActions.All;

/*
** Interfaces for sql++ queries in store/state
*/
export interface State {
    loading: boolean,
    loaded: boolean,
    success: boolean,
    sqlQueryMetadataString: string,
    sqlQueryMetadataResult: any[],
    sqlQueryMetadataError: any[]
};

const initialState: State = {
    loading: false,
    loaded: false,
    success: false,
    sqlQueryMetadataString: "",
    sqlQueryMetadataResult: [],
    sqlQueryMetadataError: [],
};

/*
** Reducer function for sql++ queries in store/state
*/
export function sqlMetadataReducer(state = initialState, action: Action) {
    switch (action.type) {
        /*
        * Change the load state to true, and clear previous results
        * to signaling that a METADATA EXECUTE a SQL++ Query is ongoing
        */
        case sqlQueryActions.EXECUTE_METADATA_QUERY: {
            return Object.assign({}, state, {
                loading: false,
                loaded: true,
                success: false,
                sqlQueryMetadataString: action.payload,
                sqlQueryMetadataResult: [],
                sqlQueryMetadataError: []
            });
        }

        /*
        * Change the load state to false, and loaded to true to signaling
        * that a  METADATA EXECUTE Query is success and there is data available in the
        * store
        */
        case sqlQueryActions.EXECUTE_METADATA_QUERY_SUCCESS: {
            return Object.assign({}, state, {
                loading: false,
                loaded: true,
                success: true,
                sqlQueryMetadataResult: action.payload,
                sqlQueryMetadataError: []
            })
        }

        /*
        * Change the load state to false, and loaded to true to signaling
        * that a  METADATA EXECUTE Query is failed and there is error data available in the
        * store
        */
        case sqlQueryActions.EXECUTE_METADATA_QUERY_FAIL: {
            return Object.assign({}, state, {
                loading: false,
                loaded: true,
                success: false,
                sqlQueryMetadataResult: [],
                sqlQueryMetadataError: action.payload
            })
        }

        /*
        * Just returns the current store/state object
        */
        default:
            return state;
    }
}