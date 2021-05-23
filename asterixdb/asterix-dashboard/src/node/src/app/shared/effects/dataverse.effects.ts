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
import { map, switchMap, catchError } from "rxjs/operators";
import * as dataverseActions from '../actions/dataverse.actions';
import { SQLService } from '../services/async-query.service';

export type Action = dataverseActions.All

@Injectable()
export class DataverseEffects {
  constructor(private actions: Actions, private sqlService: SQLService) {}
    /* Effect to set the default Dataverse */
    @Effect()
    setDefaultDataverse$: Observable<Action> = this.actions.pipe(
      ofType(dataverseActions.SET_DEFAULT_DATAVERSE),
      switchMap(query => {
        return new Observable().pipe(map(dataverse => new dataverseActions.SetDefaultDataverse('Default')))
      })
    );

    /* Effect to load a collection of all Dataverses from AsterixDB */
    @Effect()
    selectDataverses$: Observable<Action> = this.actions.pipe(
      ofType(dataverseActions.SELECT_DATAVERSES),
      switchMap(query => {
        return this.sqlService.selectDataverses().pipe(
          map(dataverse => new dataverseActions.SelectDataversesSuccess(dataverse)),
          catchError(err => of(new dataverseActions.SelectDataversesFail(err)))
        )
      })
    );

    /* Effect to create Dataverse from AsterixDB
    */
    @Effect()
    createDataverses$: Observable<Action> = this.actions.pipe(
      ofType(dataverseActions.CREATE_DATAVERSE),
      switchMap(dataverseName => {
        return this.sqlService.createDataverse((dataverseName as any).payload).pipe(
          map(dataverse => new dataverseActions.CreateDataverseSuccess(dataverse)),
          catchError(err => of(new dataverseActions.CreateDataverseFail(err)))
        )
      })
    );

    /* Effect to drop a Dataverse from AsterixDB
    */
    @Effect()
    dropDataverses$: Observable<Action> = this.actions.pipe(
      ofType(dataverseActions.DROP_DATAVERSE),
      switchMap(dataverseName => {
        return this.sqlService.dropDataverse((dataverseName as any).payload).pipe(
          map(dataverse => new dataverseActions.DropDataverseSuccess(dataverse)),
          catchError(err => of(new dataverseActions.DropDataverseFail(err)))
        )
      })
    );
}
