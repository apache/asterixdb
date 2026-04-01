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
import * as IndexAction from '../actions/index.actions';

export type Action = IndexAction.All;

/*
** Interfaces for indexes in store/state
*/
export interface State {
    loaded: boolean,
    loading: boolean,
    indexes: any[],
    createIndex: any[],
    createIndexError: any[],
    createIndexSuccess: boolean,
    createIndexFailed: boolean,
    dropIndex: any[],
    dropIndexError: any[],
    dropIndexSuccess: boolean,
    dropIndexFailed: boolean
};

const initialState: State = {
    loaded: false,
    loading: false,
    indexes: [],
    createIndex: [],
    createIndexError: [],
    createIndexSuccess: false,
    createIndexFailed: false,
    dropIndex: [],
    dropIndexError: [],
    dropIndexSuccess: false,
    dropIndexFailed: false
};

/*
** Reducer function for indexes in store/state
*/
export function indexReducer(state = initialState, action: Action) {
    switch (action.type) {
        /*
        * Change the load state to true to signaling
        * that a SELECT Query is ongoing
        */
        case IndexAction.SELECT_INDEXES: {
            return Object.assign({}, state, { loading: true });
        }

        /*
        * Change the load state to false, and loaded to true to signaling
        * that a SELECT Query is success and there is indexes available in the
        * store
        */
        case IndexAction.SELECT_INDEXES_SUCCESS: {
            return Object.assign({}, state, {
              loaded: true,
              loading: false,
              indexes: action.payload
            })
        }

        /*
        * Change the load state to true to signaling
        * that a CREATE a Index Query is ongoing
        */
        case IndexAction.CREATE_INDEX: {
            return Object.assign({}, state, {
              createIndex: [],
              createIndexName: action.payload,
              createIndexError: [],
              createIndexSuccess: false,
              createIndexFailed: false
            });
        }

        /*
        * Change the load state to false, and loaded to true to signaling
        * that a CREATE a Index Query is success and there is datasets available in the
        * store
        */
        case IndexAction.CREATE_INDEX_SUCCESS: {
            return Object.assign({}, state, {
                createIndex: [],
                createIndexError: [],
                createIndexSuccess: true,
                createIndexFailed: false
            })
        }

        /*
        * Change the load state to false, and loaded to true to signaling
        * that a CREATE a Index Query is success and there is datasets available in the
        * store
        */
        case IndexAction.CREATE_INDEX_SUCCESS: {
            return Object.assign({}, state, {
                createIndex: action.payload,
                createIndexError: [],
                createIndexSuccess: false,
                createIndexFailed: true
            })
        }

        /*
        * Change the load state to true to signaling
        * that a DROP a Index Query is ongoing
        */
        case IndexAction.DROP_INDEX: {
            return Object.assign({}, state, {
              dropIndex: [],
              dropIndexError: [],
              dropIndexName: action.payload,
              dropIndexSuccess: false,
              dropIndexFailed: false
            });
        }

        /*
        * Change the load state to false, and loaded to true to signaling
        * that a DROP a Index Query is success and there is datasets available in the
        * store
        */
        case IndexAction.DROP_INDEX_SUCCESS: {
            return Object.assign({}, state, {
                dropIndex: action.payload,
                dropIndexError: [],
                dropIndexSuccess: true,
                dropIndexFailed: false
            })
        }

        /*
        * Change the load state to false, and loaded to true to signaling
        * that a DROP a Index Query is failed and there is an error message available in the
        * store
        */
        case IndexAction.DROP_INDEX_FAIL: {
            return Object.assign({}, state, {
                dropIndex: [],
                dropIndexError: action.payload,
                dropIndexSuccess: false,
                dropIndexFailed: true
            })
        }

        /*
        * Just returns the current store/state object
        */
        default:
            return state;
    }
}