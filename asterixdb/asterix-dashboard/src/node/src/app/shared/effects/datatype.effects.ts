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
import * as datatypeActions from '../actions/datatype.actions';
import { SQLService } from '../services/async-query.service';

export type Action = datatypeActions.All

@Injectable()
export class DatatypeEffects {
  constructor(private actions: Actions,
        private sqlService: SQLService) {}

    /* Effect to load a collection of all Datatypes from AsterixDB */
    @Effect()
    selectDatatypes$: Observable<Action> = this.actions
        .ofType(datatypeActions.SELECT_DATATYPES)
        .switchMap(query => {
            return this.sqlService.selectDatatypes()
                .map(datatype => new datatypeActions.SelectDatatypesSuccess(datatype))
                .catch(err => of(new datatypeActions.SelectDatatypesFail(err)));
    });

    /* Effect to create a Datatype from AsterixDB
    */
    @Effect()
    createDatatypes$: Observable<Action> = this.actions
        .ofType(datatypeActions.CREATE_DATATYPE)
        .switchMap(datatype => {
            return this.sqlService.createDatatype((datatype as any).payload)
                .map(datatype => new datatypeActions.CreateDatatypeSuccess(datatype))
                .catch(err => of(new datatypeActions.CreateDatatypeFail(err)));
    });

    /* Effect to drop a Datatype from AsterixDB
    */
    @Effect()
    dropDatatypes$: Observable<Action> = this.actions
        .ofType(datatypeActions.DROP_DATATYPE)
        .switchMap(datatype => {
            return this.sqlService.dropDatatype((datatype as any).payload)
                .map(datatype => new datatypeActions.DropDatatypeSuccess(datatype))
                .catch(err => of(new datatypeActions.DropDatatypeFail(err)));
    });
}