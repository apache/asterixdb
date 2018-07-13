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
import * as DatasetAction from '../actions/dataset.actions';

export type Action = DatasetAction.All;

/*
** Interfaces for datasets in store/state
*/
export interface State {
    loaded: boolean,
    loading: boolean,
    datasets: any[],
    createDataset: any[],
    createDatasetError: any[],
    createDatasetSuccess: boolean,
    createDatasetFailed: boolean,
    dropDataset: any[],
    dropDatasetError: any[],
    dropDatasetSuccess: boolean,
    dropDatasetFailed: boolean,
    guideSelectsDataset: string
};

const initialState: State = {
    loaded: false,
    loading: false,
    datasets: [],
    createDataset: [],
    createDatasetError: [],
    createDatasetSuccess: false,
    createDatasetFailed: false,
    dropDataset: [],
    dropDatasetError: [],
    dropDatasetSuccess: false,
    dropDatasetFailed: false,
    guideSelectsDataset: ""
};

/*
** Reducer function for datasets in store/state
*/
export function datasetReducer(state = initialState, action: Action) {
    switch (action.type) {

      /*
      * Change the selected dataset state to true to signaling
      * UI from metadata guide
      */
      case DatasetAction.GUIDE_SELECT_DATASET: {
          return Object.assign({}, state, { guideSelectsDataset: action.payload });
      }

      /*
      * Change the load state to true to signaling
      * that a SELECT Query is ongoing
      */
      case DatasetAction.SELECT_DATASETS: {
          return Object.assign({}, state, { loading: true });
      }

      /*
      * Change the load state to false, and loaded to true to signaling
      * that a SELECT Query is success and there is datasets available in the
      * store
      */
      case DatasetAction.SELECT_DATASETS_SUCCESS: {
          return Object.assign({}, state, {
              loaded: true,
              loading: false,
              datasets: action.payload
          })
      }

      /*
      * Change the load state to true to signaling
      * that a CREATE a Dataset Query is ongoing
      */
      case DatasetAction.CREATE_DATASET: {
          return Object.assign({}, state, {
              createDataset: [],
              createDatasetError: [],
              createDatasetSuccess: false,
              createDatasetFailed: false
          });
      }

      /*
      * Change the load state to false, and loaded to true to signaling
      * that a CREATE a Dataset Query is success and there is datasets available in the
      * store
      */
      case DatasetAction.CREATE_DATASET_SUCCESS: {
          return Object.assign({}, state, {
              createDataset: action.payload,
              createDatasetName: action.payload,
              createDatasetError: [],
              createDatasetSuccess: true,
              createDatasetFailed: false
          })
      }

      /*
      * Change the load state to false, and loaded to true to signaling
      * that a CREATE a Dataset Query is failed and there is an error message available in the
      * store
      */
      case DatasetAction.CREATE_DATASET_FAIL: {
          return Object.assign({}, state, {
              createDataset: [],
              createDatasetError: action.payload,
              createDatasetSuccess: false,
              createDatasetFailed: true
          })
      }

      /*
      * Change the load state to true to signaling
      * that a DROP a Dataset Query is ongoing
      */
      case DatasetAction.DROP_DATASET: {
          return Object.assign({}, state, {
              dropDataset: [],
              dropDatasetError: [],
              dropDatasetName: action.payload,
              dropDatasetSuccess: false,
              dropDatasetFailed: false
          });
      }

      /*
      * Change the load state to false, and loaded to true to signaling
      * that a DROP a Dataset Query is success and there is datasets available in the
      * store
      */
      case DatasetAction.DROP_DATASET_SUCCESS: {
          return Object.assign({}, state, {
              dropDataset: action.payload,
              dropDatasetError: [],
              dropDatasetSuccess: true,
              dropDatasetFailed: false
          })
      }

      /*
      * Change the load state to false, and loaded to true to signaling
      * that a DROP a Dataset Query is failed and there is an error message available in the
      * store
      */
      case DatasetAction.DROP_DATASET_FAIL: {
          return Object.assign({}, state, {
              dropDataset: [],
              dropDatasetError: action.payload,
              dropDatasetSuccess: false,
              dropDatasetFailed: true
          })
      }

      /*
      * Just returns the current store/state object
      */
      default:
          return state;
    }
}