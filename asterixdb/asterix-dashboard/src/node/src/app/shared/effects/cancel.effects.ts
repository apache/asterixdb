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
import { map, switchMap, catchError } from 'rxjs/operators';
import { SQLService } from '../services/async-query.service';
import * as sqlCancelActions from '../actions/cancel.actions';

export type Action_type = sqlCancelActions.All;

@Injectable()
export class SQLCancelEffects {
  constructor(private actions: Actions,
              private sqlService: SQLService) {
  }

  /*
   * Effect to Cancel a SQL++ Query against AsterixDB
   */
  @Effect()
  cancelQuery$: Observable<Action_type> = this.actions.pipe(
    ofType(sqlCancelActions.CANCEL_QUERY),
    switchMap(query => {
      return this.sqlService.cancelSQLQuery((query as any).payload.requestId).pipe(
        map(sqlCancelQueryResult => new sqlCancelActions.CancelQuerySuccess(sqlCancelQueryResult)),
        catchError(sqlCancelQueryError => of(new sqlCancelActions.CancelQueryFail(sqlCancelQueryError)))
      )
    })
  )
}
