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
import { Component, Inject } from '@angular/core';
import { Store } from '@ngrx/store';
import { Observable } from 'rxjs';
import * as dataverseActions from '../../shared/actions/dataverse.actions';
import * as datasetActions from '../../shared/actions/dataset.actions';
import * as datatypesActions from '../../shared/actions/datatype.actions';
import * as indexesActions from '../../shared/actions/index.actions';
import { ViewChild} from '@angular/core';
import { MatDialog, MatDialogRef, MAT_DIALOG_DATA } from '@angular/material';

@Component({
    moduleId: module.id,
    selector: 'awc-metadata',
    templateUrl: 'metadata.component.html',
    styleUrls: ['metadata.component.scss']
})

export class MetadataComponent {
    dataverses$: Observable<any>;
    dataverses: any;
    datasetsFiltered: any;
    datasets$: Observable<any>;
    datasets: any;
    datatypesFiltered :any;
    datatypes$: Observable<any>;
    datatypes: any;
    indexesFiltered: any;
    indexes$: Observable<any>;
    indexes: any;

    constructor(private store: Store<any>, public dialog: MatDialog) {
        this.refreshMetadata();
    }

    ngOnInit() {
        this.dataverses$ = this.store.select(s => s.dataverse.dataverses.results);

         // Watching for Dataverses
         this.dataverses$ = this.store.select(s => s.dataverse.dataverses.results);
         this.dataverses$.subscribe((data: any[]) => {
             this.dataverses = data;
         });

         // Watching for Datasets
         this.datasets$ = this.store.select(s => s.dataset.datasets.results);
         this.datasets$.subscribe((data: any[]) => {
                this.datasets = data;
                this.datasetsFiltered = this.filter(this.datasets);
         });

         // Watching for Datatypes
         this.datatypes$ = this.store.select(s => s.datatype.datatypes.results);
         this.datatypes$.subscribe((data: any[]) => {
            this.datatypes = data;
            this.datatypesFiltered = this.filter(this.datatypes);
         });

         // Watching for indexes
         this.indexes$ = this.store.select(s => s.index.indexes.results);
         this.indexes$.subscribe((data: any[]) => {
            this.indexes = data;
            this.indexesFiltered = this.filter(this.indexes);
         });
    }

    refreshMetadata() {
        this.store.dispatch(new dataverseActions.SelectDataverses('-'));
        this.store.dispatch(new datasetActions.SelectDatasets('-'));
        this.store.dispatch(new datatypesActions.SelectDatatypes('-'));
        this.store.dispatch(new indexesActions.SelectIndexes('-'));
    }

    dataverseFilter = {}
    dataverseFilterMap = new Map();

    filter(data){
        let results = [];

        if (data) {
            for (let i=0; i< data.length; i++){
                let keyCompare = data[i].DataverseName
                this.dataverseFilterMap.forEach((value: boolean, key: string) => {
                    if (keyCompare === key) {
                        results.push(data[i]);
                    }
                });
            }
        }
        return results;
    }

    @ViewChild('datasetsPanel') datasetsPanel;
    panelOpenState: boolean = false;

    checkStatus = [];
    generateFilter(dataverse, event, i) {

        if (this.checkStatus[i] == undefined) {
            this.checkStatus.push(event.checked);
        } else {
            this.checkStatus[i] = event.checked;
        }

        if (event.checked === true) {
            this.dataverseFilter[dataverse] = event.checked;
            this.dataverseFilterMap.set(dataverse, event.checked);
        } else {
            delete this.dataverseFilter[dataverse];
            this.dataverseFilterMap.delete(dataverse);
        }

        this.datasetsFiltered = this.filter(this.datasets);
        this.datatypesFiltered = this.filter(this.datatypes);
        this.indexesFiltered = this.filter(this.indexes);

        /* Open the dataset expansion panel if there is anything to show */
        if (this.datasetsFiltered.length > 0) {
            this.panelOpenState = true;
        } else {
            this.panelOpenState = false;
        }
    }

    /*
    * opens the metadata inspector
    */
    openMetadataInspectorDialog(data): void {
        let metadata = JSON.stringify(data, null, 8);
        metadata = metadata.replace(/^{/, '');
        metadata = metadata.replace(/^\n/, '');
        metadata = metadata.replace(/}$/, '');
        let dialogRef = this.dialog.open(DialogMetadataInspector, {
            width: '500px',
            data: metadata,
            hasBackdrop: false
        });
    }
}

@Component({
    selector: 'dataset-create-dialog',
    templateUrl: 'metadata-inspector.component.html',
    styleUrls: ['metadata-inspector.component.scss']
})

export class DialogMetadataInspector {
    constructor(  public dialogCreateDsRef: MatDialogRef<DialogMetadataInspector>,
                  @Inject(MAT_DIALOG_DATA) public data: any) { }

    onClickClose() {
        this.dialogCreateDsRef.close();
    }
}