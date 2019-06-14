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
import { Injectable } from '@angular/core';
import { Action } from '@ngrx/store';
import { Actions, Effect, ofType } from '@ngrx/effects';
import { Observable ,  of } from 'rxjs';
import { SQLService } from '../services/async-query.service';
import * as sqlQueryActions from '../actions/query.actions';

export type Action = sqlQueryActions.All

@Injectable()
export class SQLQueryEffects {
  constructor(private actions: Actions,
        private sqlService: SQLService) {}

    /* Effect to Execute an SQL++ Query against the AsterixDB */
    @Effect()
    executeQuery$: Observable<Action> = this.actions
        .ofType(sqlQueryActions.EXECUTE_QUERY)
        .switchMap(query => {
            return this.sqlService.executeSQLQuery((query as any).payload.queryString, (query as any).payload.planFormat)
                .map(sqlQueryResult => new sqlQueryActions.ExecuteQuerySuccess(sqlQueryResult))
                .catch(sqlQueryError => of(new sqlQueryActions.ExecuteQueryFail(sqlQueryError)));
    });

    /* Effect to Execute an SQL++ Metadata Query against the AsterixDB
    */
    @Effect()
    executeMetadataQuery$: Observable<Action> = this.actions
        .ofType(sqlQueryActions.EXECUTE_METADATA_QUERY)
        .switchMap(query => {
            return this.sqlService.executeSQLQuery((query as any).payload, (query as any).payload.planFormat)
                .map(sqlMetadataQueryResult => new sqlQueryActions.ExecuteMetadataQuerySuccess(sqlMetadataQueryResult))
                .catch(sqlMetadataQueryError => of(new sqlQueryActions.ExecuteMetadataQueryFail(sqlMetadataQueryError)));
    });
}