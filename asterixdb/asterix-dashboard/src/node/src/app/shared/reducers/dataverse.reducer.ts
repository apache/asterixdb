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
import * as DataverseAction from '../actions/dataverse.actions';

export type Action = DataverseAction.All;

/*
** Interfaces for dataverses in store/state
*/
export interface State {
    loaded: boolean,
    loading: boolean,
    dataverses: any[],
    createDataverse: any[],
    createDataverseName: string,
    createDataverseError: any[],
    createDataverseSuccess: boolean,
    createDataverseFailed: boolean
    dropDataverse: any[],
    dropDataverseName: string;
    dropDataverseError: any[],
    dropDataverseSuccess: boolean,
    dropDataverseFailed: boolean,
    defaultDataverseName: any
};

const initialState: State = {
    loaded: false,
    loading: false,
    dataverses: [],
    createDataverse: [],
    createDataverseName: "",
    createDataverseError: [],
    createDataverseSuccess: false,
    createDataverseFailed: false,
    dropDataverse: [],
    dropDataverseName: "",
    dropDataverseError: [],
    dropDataverseSuccess: false,
    dropDataverseFailed: false,
    defaultDataverseName: 'Default'
};

/*
** Reducer function for dataverses in store/state
*/
export function dataverseReducer(state = initialState, action: Action) {
    switch (action.type) {

      /*
      * Set the default Dataverse Name
      */
     case DataverseAction.SET_DEFAULT_DATAVERSE: {
        return Object.assign({}, state, { defaultDataverseName: action.payload });
      }

      /*
      * Change the load state to true to signaling
      * that a SELECT Query is ongoing
      */
      case DataverseAction.SELECT_DATAVERSES: {
          return Object.assign({}, state, { loading: true });
      }

      /*
      * Change the load state to false, and loaded to true to signaling
      * that a SELECT Query is success and there is dataverses available in the
      * store
      */
      case DataverseAction.SELECT_DATAVERSES_SUCCESS: {
          return Object.assign({}, state, {
              loaded: true,
              loading: false,
              dataverses: action.payload //  _.sortBy(_.values(action.payload), 'dataverseName')
          })
      }

      /*
      * Change the load state to true to signaling
      * that a CREATE a Dataset Query is ongoing
      */
      case DataverseAction.CREATE_DATAVERSE: {
          return Object.assign({}, state, {
              createDataverse: [],
              createDataverseName: action.payload,
              createDataverseError: [],
              createDataverseSuccess: false,
              createDataverseFailed: false
          });
      }

      /*
      * Change the load state to false, and loaded to true to signaling
      * that a CREATE a Dataverse Query is success and there is a success message available in the
      * store
      */
      case DataverseAction.CREATE_DATAVERSE_SUCCESS: {
          return Object.assign({}, state, {
              createDataverse: action.payload,
              createDataverseError: [],
              createDataverseSuccess: true,
              createDataverseFailed: false
          })
      }

      /*
      * Change the load state to false, and loaded to true to signaling
      * that a CREATE a Dataverse Query is failed and there is an error message available in the
      * store
      */
      case DataverseAction.CREATE_DATAVERSE_FAIL: {
          return Object.assign({}, state, {
              createDataverse: [],
              createDataverseError: action.payload,
              createDataverseSuccess: false,
              createDataverseFailed: true
          })
      }

      /*
      * Change the load state to true to signaling
      * that a DROP a Dataverse Query is ongoing
      */
      case DataverseAction.DROP_DATAVERSE: {
          return Object.assign({}, state, {
              dropDataverse: [],
              dropDataverseName: action.payload,
              dropDataverseError: [],
              dropDataverseSuccess: false,
              dropDataverseFailed: false
          });
      }

      /*
      * Change the load state to false, and loaded to true to signaling
      * that a DROP a Dataverse Query is success and there is success message available in the
      * store
      */
      case DataverseAction.DROP_DATAVERSE_SUCCESS: {
          return Object.assign({}, state, {
              dropDataverse: action.payload,
              dropDataverseError: [],
              dropDataverseSuccess: true,
              dropDataverseFailed: false
          })
      }

      /*
      * Change the load state to false, and loaded to true to signaling
      * that a DROP a Dataverse Query is failed and there is an error message available in the
      * store
      */
      case DataverseAction.DROP_DATAVERSE_FAIL: {
          return Object.assign({}, state, {
              dropDataverse: [],
              dropDataverseError: action.payload,
              dropDataverseSuccess: false,
              dropDataverseFailed: true
          })
      }

      /*
      * Just returns the current store/state object
      */
      default:
          return state;
    }
}