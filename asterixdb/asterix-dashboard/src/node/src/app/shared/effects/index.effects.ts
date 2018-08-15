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
import { Effect, Actions } from '@ngrx/effects';
import { Observable ,  of } from 'rxjs';
import * as indexActions from '../actions/index.actions';
import { SQLService } from '../services/async-query.service';

export type Action = indexActions.All

@Injectable()
export class IndexEffects {
  constructor(private actions: Actions,
        private sqlService: SQLService) {}

    /* Effect to load a collection of all Index from AsterixDB */
    @Effect()
    selectIndexes$: Observable<Action> = this.actions
        .ofType(indexActions.SELECT_INDEXES)
        .switchMap(query => {
            return this.sqlService.selectIndexes()
                .map(index => new indexActions.SelectIndexesSuccess(index))
                .catch(err => of(new indexActions.SelectIndexesFail(err)));
    });

    /* Effect to create a Index
    */
    @Effect()
    createIndexes$: Observable<Action> = this.actions
        .ofType(indexActions.CREATE_INDEX)
        .switchMap(index => {
            return this.sqlService.createIndex((index as any).payload)
                .map(index => new indexActions.CreateIndexSuccess(index))
                .catch(err => of(new indexActions.CreateIndexFail(err)));
    });

    /* Effect to drop a Index
    */
    @Effect()
    dropIndexes$: Observable<Action> = this.actions
        .ofType(indexActions.DROP_INDEX)
        .switchMap(index => {
            return this.sqlService.dropIndex((index as any).payload)
                .map(index => new indexActions.DropIndexSuccess(index))
                .catch(err => of(new indexActions.DropIndexFail(err)));
    });
}
