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
    currentRequestId: string,
    loadingHash: {},
    loadedHash: {},
    successHash:{},
    errorHash: {},
    sqlQueryString: string,
    sqlQueryPlanFormat: string,
    sqlQueryStringHash: {},
    sqlQueryPlanFormatHash: {},
    sqlQueryResultHash: {},
    sqlQueryErrorHash: {},
    sqlQueryPrepared: {},
    sqlQueryMetrics: {}
};

const initialState: State = {
    currentRequestId: '',
    loadingHash: {},
    loadedHash: {},
    successHash:{},
    errorHash: {},
    sqlQueryString: "",
    sqlQueryPlanFormat: "",
    sqlQueryStringHash: {},
    sqlQueryPlanFormatHash: {},
    sqlQueryResultHash: {},
    sqlQueryErrorHash: {},
    sqlQueryPrepared: {},
    sqlQueryMetrics: {}
};

/*
** Reducer function for sql++ queries in store/state
*/
export function sqlReducer(state = initialState, action: Action) {

    switch (action.type) {
        case sqlQueryActions.PREPARE_QUERY: {
            return Object.assign({}, state, {
                sqlQueryPrepared: { ...state.sqlQueryPrepared, [action.payload.editorId]: action.payload.queryString },
                loadingHash: state.loadingHash,
                errorHash: state.errorHash,
                sqlQueryErrorHash: state.sqlQueryErrorHash,
                sqlQueryResultHash: state.sqlQueryResultHash,
                sqlQueryStringHash: state.sqlQueryStringHash,
                sqlQueryPlanFormatHash: { ...state.sqlQueryPlanFormatHash, [action.payload.editorId]: action.payload.planFormat },
                sqlQueryMetrics: state.sqlQueryMetrics,
                currentRequestId: state.currentRequestId
            });
        }

        case sqlQueryActions.CLEAN_QUERY: {
            return Object.assign({}, state, {
                sqlQueryPrepared: { ...state.sqlQueryPrepared, [action.payload.editorId]: action.payload.queryString },
                loadingHash: { ...state.loadingHash, [action.payload.editorId]: false },
                loadedHash: { ...state.loadedHash, [action.payload.editorId]: false },
                successHash: { ...state.successHash, [action.payload.editorId]: false },
                errorHash: { ...state.errorHash, [action.payload.editorId]: false },
                sqlQueryStringHash: { ...state.sqlQueryStringHash, [action.payload.editorId]: {} },
                sqlQueryResultHash: { ...state.sqlQueryResultHash, [action.payload.editorId]: {} },
                sqlQueryErrorHash: { ...state.sqlQueryErrorHash, [action.payload.editorId]: [] },
                sqlQueryMetrics: { ...state.sqlQueryMetrics, [action.payload.editorId]: {} },
                currentRequestId: action.payload.editorId
            });
        }

        /*
        * Change the load state to true, and clear previous results
        * to signaling that a EXECUTE a SQL++ Query is ongoing
        */
        case sqlQueryActions.EXECUTE_QUERY: {
            return Object.assign({}, state, {
                currentRequestId: action.payload.requestId,
                loadingHash: { ...state.loadingHash, [action.payload.requestId]: true },
                loadedHash: { ...state.loadedHash, [action.payload.requestId]: false },
                successHash: { ...state.successHash, [action.payload.requestId]: false },
                errorHash: { ...state.errorHash, [action.payload.requestId]: false },
                sqlQueryString: action.payload.queryString,
                sqlQueryPlanFormat: action.payload.planFormat,
                sqlQueryStringHash: { ...state.sqlQueryStringHash, [action.payload.requestId]: action.payload.queryString },
                sqlQueryPlanFormatHash: { ...state.sqlQueryPlanFormatHash, [action.payload.requestId]: action.payload.planFormat },
                sqlQueryResultHash: { ...state.sqlQueryResultHash, [action.payload.requestId]: [] },
                sqlQueryErrorHash: { ...state.sqlQueryErrorHash, [action.payload.requestId]: [] },
                sqlQueryMetrics: { ...state.sqlQueryMetrics, [action.payload.requestId]: [] },
               });
        }

        /*
        * Change the load state to false, and loaded to true to signaling
        * that a EXECUTE Query is success and there is data available in the
        * store
        */
        case sqlQueryActions.EXECUTE_QUERY_SUCCESS: {
            action.payload['planFormat'] = state.sqlQueryPlanFormat;
            return Object.assign({}, state, {
                loadingHash: { ...state.loadingHash, [state.currentRequestId]: false },
                loadedHash: { ...state.loadedHash, [state.currentRequestId]: true },
                successHash: { ...state.successHash, [state.currentRequestId]: true },
                errorHash: { ...state.errorHash, [state.currentRequestId]: false },
                sqlQueryPlanFormatHash: state.sqlQueryPlanFormatHash,
                sqlQueryStringHash: { ...state.sqlQueryStringHash, [state.currentRequestId]: state.sqlQueryString },
                sqlQueryResultHash: { ...state.sqlQueryResultHash, [state.currentRequestId]: action.payload },
                sqlQueryErrorHash: { ...state.sqlQueryErrorHash, [state.currentRequestId]: [] },
                sqlQueryMetrics: { ...state.sqlQueryMetrics, [state.currentRequestId]: action.payload.metrics }
            })
        }

        /*
        * Change the load state to false, and loaded to true to signaling
        * that a EXECUTE Query is failed and there is error data available in the
        * store
        */
        case sqlQueryActions.EXECUTE_QUERY_FAIL: {
            action.payload['planFormat'] = state.sqlQueryPlanFormat;
            return Object.assign({}, state, {
                loadingHash: { ...state.loadingHash, [state.currentRequestId]: false },
                loadedHash: { ...state.loadedHash, [state.currentRequestId]: true },
                successHash: { ...state.successHash, [state.currentRequestId]: false },
                errorHash: { ...state.errorHash, [state.currentRequestId]: true },
                sqlQueryPlanFormatHash: state.sqlQueryPlanFormatHash,
                sqlQueryStringHash: { ...state.sqlQueryStringHash, [state.currentRequestId]: state.sqlQueryString },
                sqlQueryResultHash: { ...state.sqlQueryResultHash, [state.currentRequestId]: [] },
                sqlQueryErrorHash: { ...state.sqlQueryErrorHash, [state.currentRequestId]: action.payload.errors },
                sqlQueryMetrics: { ...state.sqlQueryMetrics, [state.currentRequestId]: [] },
            })
        }

        /*
        * Just returns the current store/state object
        */
        default: {
            return state;
        }
    }
}