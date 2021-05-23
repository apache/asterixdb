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
import * as sqlCancelActions from '../actions/cancel.actions';

export type Action_type = sqlCancelActions.All;

export interface State {
  currentRequestId: string,
  success: boolean
}

const initialState: State = {
  currentRequestId: "",
  success: false
}

/*
** Reducer function for sql++ queries in store/state
*/
export function cancelReducer(state = initialState, action: Action_type) {
  switch (action.type) {
    case sqlCancelActions.CANCEL_QUERY: {
      return Object.assign({}, state, {
        currentRequsetId: action.payload.requestId,
        success: false
      })
    }

    case sqlCancelActions.CANCEL_QUERY_SUCCESS: {
      return Object.assign({}, state, {
        success: true
      })
    }

    case sqlCancelActions.CANCEL_QUERY_FAIL: {
      return Object.assign({}, state, {
        success: false
      })
    }

    default: {
      return state;
    }
  }
}
