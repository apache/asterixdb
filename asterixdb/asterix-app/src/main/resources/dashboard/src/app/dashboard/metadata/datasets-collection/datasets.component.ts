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
import { Component, ElementRef, ViewChild, Inject, Input } from '@angular/core';
import { Dataset } from '../../../shared/models/asterixDB.model'
import { Store } from '@ngrx/store';
import { Observable } from 'rxjs/Observable';
import * as datasetActions from '../../../shared/actions/dataset.actions'
import {DataSource} from '@angular/cdk/collections';
import {BehaviorSubject} from 'rxjs/BehaviorSubject';
import { Subscription } from "rxjs/Rx";
import { State } from '../../../shared/reducers/dataset.reducer';
import 'rxjs/add/operator/startWith';
import 'rxjs/add/observable/merge';
import 'rxjs/add/operator/map';
import 'rxjs/add/operator/debounceTime';
import 'rxjs/add/operator/distinctUntilChanged';
import 'rxjs/add/observable/fromEvent';
import { MatPaginator } from '@angular/material';
import { SelectionModel } from '@angular/cdk/collections';
import { MatDialog, MatDialogRef, MAT_DIALOG_DATA } from '@angular/material';

/**
 * Root component
 * Defines our application's layout
 */
@Component({
  selector: 'awc-datasets',
  templateUrl: 'datasets.component.html',
  styleUrls: ['datasets.component.scss']
})

export class DatasetCollection {
  displayedColumns = "['CompactionPolicy', 'DatasetId', 'DatasetName', 'DatasetType', 'DatatypeDataverseName', 'DatatypeName', 'DataverseName', 'GroupName', 'PendingOp', 'Timestamp']"

/*
  compactionPolicy: string;
  compactionPolicyProperties: CompactionPolicyProperties[]; *
  datasetId: string;
  datasetName: string;
  datasetType:string;
  datatypeDataverseName: string;
  datatypeName: string;
  dataverseName: string;
  groupName:string;
  hints: string[]; *
  internalDetails: InternalDetails; *
  pendingOp: string;
  timestamp: string; */
  datasetName: string;  
  dataSource: DatasetDataSource | null;
  loaded$: Observable<any>
  @Input('message') errorMessage: string = ""
  dsName = "";

  constructor(private store: Store<any>, public dialog: MatDialog) {
    this.loaded$ = this.store.select('dataset');

    // Watching the name of the latest create dataset
    this.store.select(s => s.dataset.createDatasetName).subscribe((data: any) => {
      this.dsName = data;
    })

    // Watching the name of the latest drop dataset
    this.store.select(s => s.dataset.dropDatasetName).subscribe((data: any) => {
      this.dsName = data;
    })

    // Watching for the if there is a change in the collection
		this.store.select(s => s.dataset.createDatasetSuccess).subscribe((data: any) => {
      if (data === true) {
        this.getDatasets();
        this.errorMessage = "SUCCEED: CREATE DATASET " + this.dsName;
      }  
    })

    // Watching for the if there is a error in a create dataset operation 
		this.store.select(s => s.dataset.createDatasetError).subscribe((data: any) => {
      if (data.errors) {
        this.errorMessage = "ERROR: " + data.errors[0].msg;
      }  
    })


    // Watching for the success message in a drop dataset operation 
    this.store.select(s => s.dataset.dropDatasetSuccess).subscribe((data: any) => {
      if (data === true) {
        this.getDatasets();
        this.errorMessage = "SUCCEED: DROP DATASET " + this.dsName;
      }  
    })

    // Watching for the if there is a error in a drop dataset operation 
		this.store.select(s => s.dataset.dropDatasetError).subscribe((data: any) => {
      if (data.errors) {
        this.errorMessage = "ERROR: " + data.errors[0].msg;
      }  
    })
  }

  getDatasets() {
    // Trigger the effect to refresh the dataset
    this.store.dispatch(new datasetActions.SelectDatasets('-'));
  }

  ngOnInit() {
    // Assign the datasource for the table 
    this.dataSource = new DatasetDataSource(this.store);
  }

  /* 
  * opens the create dataverse dialog
  */
  openCreateDatasetDialog(): void {
    let dialogRef = this.dialog.open(DialogCreateDataset, {
      width: '420px',
      data: { name: this.datasetName }
    });

    dialogRef.afterClosed().subscribe(result => {
      this.datasetName = result;
    });
  }

  /* 
  * opens the drop dataverse dialog
  */
  openDropDatasetDialog(): void {
    let dialogRef = this.dialog.open(DialogDropDataset, {
      width: '420px',
      data: { name: this.datasetName }
    });

    dialogRef.afterClosed().subscribe(result => {
      this.datasetName = result;
    });
  }

  /* 
  * Clean up the error message on the screen
  */
  onClick(): void {
    this.errorMessage = "";
  }

  output: any;

  highlight(row){
    this.output = JSON.stringify(row, null, 2);
  }

  @ViewChild('querymetadata') inputQuery;
  
  /* Cleans up error message */
  cleanUp() {
    this.errorMessage = ""; 
    // Cascading   
    this.inputQuery.cleanUp(); 
  }
}

@Component({
  selector: 'dataset-create-dialog',
  templateUrl: 'dataset-create-dialog.component.html',
  styleUrls: ['dataset-create-dialog.component.scss']
})

export class DialogCreateDataset {
  constructor(  private store: Store<any>,
                public dialogCreateDsRef: MatDialogRef<DialogCreateDataset>,
                @Inject(MAT_DIALOG_DATA) public data: any) { }

  onClick(): void {
    this.store.dispatch(new datasetActions.CreateDataset(this.data.datasetName));
    this.dialogCreateDsRef.close();
  }

  onNoClick(): void {
    this.dialogCreateDsRef.close();
  }
}

@Component({
  selector: 'dataset-drop-dialog',
  templateUrl: 'dataset-drop-dialog.component.html',
  styleUrls: ['dataset-drop-dialog.component.scss']
})

export class DialogDropDataset {
  constructor(  private store: Store<any>,
                public dialogDropDsRef: MatDialogRef<DialogDropDataset>,
                @Inject(MAT_DIALOG_DATA) public data: any) { }

  onClick(): void {
    this.store.dispatch(new datasetActions.DropDataset(this.data.datasetName));
    this.dialogDropDsRef.close();
  }

  onNoClick(): void {
    this.dialogDropDsRef.close();
  }
}

export class DatasetDataSource extends DataSource<any> {
  private datasets$: Observable<any>
  constructor(private store: Store<any>) {
    super();
    this.datasets$ = this.store.select(s => s.dataset.datasets.results);
  }

  /** Connect function called by the table to retrieve one stream containing the data to render. */
  connect(): Observable<Dataset[]> {
      const displayDataChanges = [
        this.datasets$,
      ];

    return this.datasets$;
  }

  disconnect() {}
}
