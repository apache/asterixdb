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
import { Component, Inject, Input } from '@angular/core';
import { Index } from '../../../shared/models/asterixDB.model'
import { Store } from '@ngrx/store';
import { Observable } from 'rxjs/Observable';
import * as indexActions from '../../../shared/actions/index.actions'
import { ElementRef, ViewChild} from '@angular/core';
import {DataSource} from '@angular/cdk/collections';
import {BehaviorSubject} from 'rxjs/BehaviorSubject';
import 'rxjs/add/operator/startWith';
import 'rxjs/add/observable/merge';
import 'rxjs/add/operator/map';
import 'rxjs/add/operator/debounceTime';
import 'rxjs/add/operator/distinctUntilChanged';
import 'rxjs/add/observable/fromEvent';
import { Subscription } from "rxjs/Rx";
import { State } from '../../../shared/reducers/index.reducer';
import { MatPaginator } from '@angular/material';
import { SelectionModel } from '@angular/cdk/collections';
import { MatDialog, MatDialogRef, MAT_DIALOG_DATA } from '@angular/material';

/**
 * Root component
 * Defines our application's layout
 */
@Component({
  selector: 'awc-indexes',
  templateUrl: 'indexes.component.html',
  styleUrls: ['indexes.component.scss']
})

export class IndexCollection {
  displayedColumns = "['dataverseName', 'datasetName', 'indexName', 'indexStructure', 'isPrimary', 'timestamp', 'pendingOp']"
  /*
  dataverseName: string;
  datasetName: string;
  indexName: string;
  indexStructure: string;
  searchKey: string[];
  isPrimary: boolean;
  timestamp: string;
  pendingOp: string;
  */
  indexName: string;
  dataSource: IndexDataSource | null;
  loaded$: Observable<any>;
  @Input('message') errorMessage: string = ""
  
  idxName = "";

  constructor(private store: Store<any>, public dialog: MatDialog) {
    this.loaded$ = this.store.select('index');
    // Watching the name of the latest create index
    this.store.select(s => s.index.createIndexName).subscribe((data: any) => {
      this.idxName = data;
    })

    // Watching the name of the latest drop index
    this.store.select(s => s.index.dropIndexName).subscribe((data: any) => {
      this.idxName = data;
    })

    // Watching for the if there is a change in the collection
		this.store.select(s => s.index.createIndexSuccess).subscribe((data: any) => {
      if (data === true) {
        this.getIndexes();
        this.errorMessage = "SUCCEED: CREATE INDEX " + this.idxName;
      }  
    })

    // Watching for the if there is a error in a create index operation 
		this.store.select(s => s.index.createIndexError).subscribe((data: any) => {
      if (data.errors) {
        this.errorMessage = "ERROR: " + data.errors[0].msg;
      }  
    })

    // Watching for the success message in a drop index operation 
    this.store.select(s => s.index.dropIndexSuccess).subscribe((data: any) => {
      if (data === true) {
        this.getIndexes();
        this.errorMessage = "SUCCEED: DROP INDEX " + this.idxName;
      }  
    })

    // Watching for the if there is a error in a drop index operation 
		this.store.select(s => s.index.dropIndexError).subscribe((data: any) => {
      if (data.errors) {
        this.errorMessage = "ERROR: " + data.errors[0].msg;
      }  
    })
  }

  getIndexes() {
    // Triggers the effect to refresg the indexes
    this.store.dispatch(new indexActions.SelectIndexes('-'));
  }

  ngOnInit() {
    // Assign the datasource for the table 
    this.dataSource = new IndexDataSource(this.store);
  }  

  /* 
  * opens the create index dialog
  */
  openCreateIndexDialog(): void {
    let dialogRef = this.dialog.open(DialogCreateIndex, {
      width: '420px',
      data: { name: this.indexName }
    });

    dialogRef.afterClosed().subscribe(result => {
      this.indexName = result;
    });
  }


   /* 
  * opens the drop index dialog
  */
  openDropIndexDialog(): void {
    let dialogRef = this.dialog.open(DialogDropIndex, {
      width: '420px',
      data: { name: this.indexName }
    });

    dialogRef.afterClosed().subscribe(result => {
      this.indexName = result;
    });
  }

  /* 
  * Clean up the error message on the screen
  */
  onClick(): void {
    this.errorMessage = "";
  }

   /* Showing all the index metadata information */
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
  selector: 'index-create-dialog',
  templateUrl: 'index-create-dialog.component.html',
  styleUrls: ['index-create-dialog.component.scss']
})

export class DialogCreateIndex {
  constructor(  private store: Store<any>,
                public dialogCreateIdxRef: MatDialogRef<DialogCreateIndex>,
                @Inject(MAT_DIALOG_DATA) public data: any) { }

  onClick(): void {
    this.store.dispatch(new indexActions.CreateIndex(this.data.indexName));
    this.dialogCreateIdxRef.close();
  }

  onNoClick(): void {
    this.dialogCreateIdxRef.close();
  }
}

@Component({
  selector: 'index-drop-dialog',
  templateUrl: 'index-drop-dialog.component.html',
  styleUrls: ['index-drop-dialog.component.scss']
})

export class DialogDropIndex {
  constructor(  private store: Store<any>,
                public dialogDropIdxRef: MatDialogRef<DialogDropIndex>,
                @Inject(MAT_DIALOG_DATA) public data: any) { }

  onClick(): void {
    console.log(this.data.indexName)
    this.store.dispatch(new indexActions.DropIndex(this.data.indexName));
    this.dialogDropIdxRef.close();
  }

  onNoClick(): void {
    this.dialogDropIdxRef.close();
  }
}

export class IndexDataSource extends DataSource<any> {
  private indexes$: Observable<any>

  constructor(private store: Store<any>) {
    super();
    this.indexes$ = this.store.select(s => s.index.indexes.results);
  }

  /** Connect function called by the table to retrieve one stream containing the data to render. */
  connect(): Observable<Index[]> {
      const displayDataChanges = [
        this.indexes$,
      ];

    return this.indexes$;
  }

  disconnect() {}
}
