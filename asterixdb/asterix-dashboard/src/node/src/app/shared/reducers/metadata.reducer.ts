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
import * as metadataTreeActions from '../actions/metadata.actions';

export type Action = metadataTreeActions.All;

/*
** Interfaces for the metadata tree in store/state
*/
export interface State {
  tree: any[],
  loading: boolean,
  loaded: boolean,
};

const initialState: State = {
  tree: [],
  loading: false,
  loaded: false
};

export function metadataTreeReducer(state = initialState, action: Action) {
  switch (action.type) {
    case metadataTreeActions.UPDATE_METADATA_TREE: {
      return Object.assign({}, state, {
        tree: [],
        loading: true,
        loaded: false
      });
    }

    case metadataTreeActions.UPDATE_METADATA_TREE_SUCCESS: {
      return Object.assign({}, state, {
        tree: [...state.tree, action.payload],
        loading: false,
        loaded: true
      });
    }
    /*
    * Just returns the current store/state object
    */
    default:
      return state;
  }
}
