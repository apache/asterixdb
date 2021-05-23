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

import * as FunctionAction from '../actions/function.actions';

export type Action = FunctionAction.All;

/*
** Interfaces for functions in store/state
*/
export interface State {
  loaded: boolean,
  loading: boolean,
  functions: any[]
}

const initialState: State = {
  loaded: false,
  loading: false,
  functions: []
}

/*
** Reducer function for functions in store/state
*/
export function functionReducer(state = initialState, action: Action) {
  switch (action.type) {
    /*
    * Change the load state to true to signaling
    * that a SELECT Query is ongoing
    */
    case FunctionAction.SELECT_FUNCTIONS: {
      return Object.assign({}, state, {
        loading: true
      });
    }

    /*
     * Change the load state to false, and loaded to true to signaling
     * that a SELECT Query is success and there are functions available in the
     * store
     */
    case FunctionAction.SELECT_FUNCTIONS_SUCCESS: {
      return Object.assign({}, state, {
        loaded: true,
        loading: false,
        functions: action.payload
      });
    }

    /*
    * Just returns the current store/state object
     */
    default:
      return state;
  }
}
