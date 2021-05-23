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
import { Effect, Actions, ofType } from '@ngrx/effects';
import { Observable ,  of } from 'rxjs';
import { map, switchMap, catchError } from 'rxjs/operators';
import * as functionActions from '../actions/function.actions';
import { SQLService } from "../services/async-query.service";

export type Action = functionActions.All;

@Injectable()
export class FunctionEffects {
  constructor(private actions: Actions,
              private sqlService: SQLService) {}

  /* Effect to load a collection of all functions from AsterixDB */
  @Effect()
  selectFunctions$: Observable<Action> = this.actions.pipe(
    ofType(functionActions.SELECT_FUNCTIONS),
    switchMap(query => {
      return this.sqlService.selectFunctions().pipe(
        map(fn => new functionActions.SelectFunctionsSuccess(fn)),
        catchError(err => of(new functionActions.SelectFunctionsFail(err)))
      )
    })
  );
}
