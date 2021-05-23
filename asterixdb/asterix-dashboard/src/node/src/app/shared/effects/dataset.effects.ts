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
import { Effect, Actions, ofType} from '@ngrx/effects';
import { Observable ,  of } from 'rxjs';
import { switchMap, map, catchError } from "rxjs/operators";
import * as datasetActions from '../actions/dataset.actions';
import { SQLService } from '../services/async-query.service';

export type ActionType = datasetActions.All

@Injectable()
export class DatasetEffects {
    constructor(private actions: Actions,
        private sqlService: SQLService) {}

    /* Effect to load a collection of all Datasets from AsterixDB
    */
    @Effect()
    selectDatasets$: Observable<ActionType> = this.actions.pipe(
      ofType(datasetActions.SELECT_DATASETS),
      switchMap(query => {
        return this.sqlService.selectDatasets().pipe(
          map(dataset => new datasetActions.SelectDatasetsSuccess(dataset)),
          catchError(err => of(new datasetActions.SelectDatasetsFail(err)))
        )
      })
    );

    /* Effect to create a Datasets from AsterixDB
    */
    @Effect()
    createDatasets$: Observable<ActionType> = this.actions.pipe(
      ofType(datasetActions.CREATE_DATASET),
      switchMap(dataset => {
        return this.sqlService.createDataset((dataset as any).payload).pipe(
          map(dataset => new datasetActions.CreateDatasetSuccess(dataset)),
          catchError(err => of(new datasetActions.CreateDatasetFail(err)))
        )
      })
    );

    /* Effect to drop a Datasets from AsterixDB
    */
    @Effect()
    dropDatasets$: Observable<ActionType> = this.actions.pipe(
      ofType(datasetActions.DROP_DATASET),
      switchMap(dataset => {
        return this.sqlService.dropDataset((dataset as any).payload).pipe(
          map(dataset => new datasetActions.DropDatasetSuccess(dataset)),
          catchError(err => of(new datasetActions.DropDatasetFail(err)))
        )
      })
    );

    /* Effect of sampling a datasets from AsterixDB
    */
    @Effect()
    sampleDataset$: Observable<ActionType> = this.actions.pipe(
      ofType(datasetActions.SAMPLE_DATASET),
      switchMap(dataset => {
        return this.sqlService.sampleDataset((dataset as any).payload.dataset).pipe(
          map(dataset => new datasetActions.SampleDatasetSuccess(dataset)),
          catchError(err => of(new datasetActions.SampleDatasetFail(err)))
        )
      })
    );
}
