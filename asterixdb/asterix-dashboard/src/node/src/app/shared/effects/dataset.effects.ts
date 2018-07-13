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
import { Effect, Actions } from '@ngrx/effects';
import { Observable ,  of } from 'rxjs';
import * as datasetActions from '../actions/dataset.actions';
import { SQLService } from '../services/async-query.service';
import 'rxjs/add/operator/switchMap'

export type Action = datasetActions.All

@Injectable()
export class DatasetEffects {
    constructor(private actions: Actions,
        private sqlService: SQLService) {}

    /* Effect to load a collection of all Datasets from AsterixDB
    */
    @Effect()
    selectDatasets$: Observable<Action> = this.actions
        .ofType(datasetActions.SELECT_DATASETS)
        .switchMap(query => {
            return this.sqlService.selectDatasets()
                .map(dataset => new datasetActions.SelectDatasetsSuccess(dataset))
                .catch(err => of(new datasetActions.SelectDatasetsFail(err)));
    });

    /* Effect to create a Datasets from AsterixDB
    */
    @Effect()
    createDatasets$: Observable<Action> = this.actions
        .ofType(datasetActions.CREATE_DATASET)
        .switchMap(dataset => {
            return this.sqlService.createDataset((dataset as any).payload)
                .map(dataset => new datasetActions.CreateDatasetSuccess(dataset))
                .catch(err => of(new datasetActions.CreateDatasetFail(err)));
    });

    /* Effect to drop a Datasets from AsterixDB
    */
    @Effect()
    dropDatasets$: Observable<Action> = this.actions
        .ofType(datasetActions.DROP_DATASET)
        .switchMap(dataset => {
            return this.sqlService.dropDataset((dataset as any).payload)
                .map(dataset => new datasetActions.DropDatasetSuccess(dataset))
                .catch(err => of(new datasetActions.DropDatasetFail(err)));
    });
}