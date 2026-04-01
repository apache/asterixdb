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
import * as appActions from '../actions/app.actions';

export type Action = appActions.All;

/*
** Interfaces for app state in store/state
*/
export interface State {
    currentQueryIndex: Number;
    sideMenuVisible: boolean;
};

const initialState: State = {
    currentQueryIndex: 0,
    sideMenuVisible: false,
};

/*
** Reducer function for app state in store/state
*/
export function appReducer(state = initialState, action: Action) {
    switch (action.type) {
        /*
        * Global side navigator open/close controller
        */
        case appActions.APP_SIDE_MENU: {
            return Object.assign({}, state, {
                sideMenuVisible: action.payload
            });
        }

        /*
        * Change the load state to true, and clear previous results
        * to signaling that a EXECUTE a SQL++ Query is ongoing
        */
        case appActions.APP_MODE_CHANGE: {
            return Object.assign({}, state, {
            });
        }

        /*
        * store the query currently in codemirror editor
        */
        case appActions.APP_QUERY_INPUT_INDEX: {
            return Object.assign({}, state, {
                    currentQueryIndex: action.payload
                });
            }

        default:
                return state;
        }
}