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
import { Component, ChangeDetectionStrategy, Inject, Input } from '@angular/core';
import { Datatype } from '../../../shared/models/asterixDB.model'
import { Store } from '@ngrx/store';
import { Observable } from 'rxjs/Observable';
import * as datatypeActions from '../../../shared/actions/datatype.actions'
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
import { State } from '../../../shared/reducers/datatype.reducer';
import { MatPaginator } from '@angular/material';
import { SelectionModel } from '@angular/cdk/collections';
import { MatDialog, MatDialogRef, MAT_DIALOG_DATA } from '@angular/material';

@Component({
  selector: 'awc-datatypes',
  templateUrl: 'datatypes.component.html',
  styleUrls: ['datatypes.component.scss']
})

export class DatatypeCollection {
  displayedColumns = "['datatypeName', 'dataverseName', 'timeStamp']"
  /*
  datatypeName: string;
  dataverseName: string;
  derived: DatatypeDerived;
  timeStamp: string;
  */
  datatypeName: string;
  dataSource: DatatypeDataSource | null;
  loaded$: Observable<any>;
  @ViewChild(MatPaginator) paginator: MatPaginator;
  @ViewChild('filter') filter: ElementRef;
  selection = new SelectionModel<string>(true, []);
  @Input('message') errorMessage: string = ""
  dtName = "";
  
  constructor(private store: Store<any>, public dialog: MatDialog) {
    this.loaded$ = this.store.select('datatype');
    // Watching the name of the latest create datatype
    this.store.select(s => s.datatype.createDatatypeName).subscribe((data: any) => {
      this.dtName = data;
    })

    // Watching the name of the latest drop datatype
    this.store.select(s => s.datatype.dropDatatypeName).subscribe((data: any) => {
      this.dtName = data;
    })

    // Watching for the if there is a change in the collection
		this.store.select(s => s.datatype.createDatatypeSuccess).subscribe((data: any) => {
      if (data === true) {
        this.getDatatypes();
        this.errorMessage = "SUCCEED: CREATE DATATYPE " + this.dtName;
      }  
    })

    // Watching for the if there is a error in a create datatype operation 
		this.store.select(s => s.datatype.createDatatypeError).subscribe((data: any) => {
      if (data.errors) {
        this.errorMessage = "ERROR: " + data.errors[0].msg;
      }  
    })

    // Watching for the success message in a drop datatype operation 
    this.store.select(s => s.datatype.dropDatatypeSuccess).subscribe((data: any) => {
      if (data === true) {
        this.getDatatypes();
        this.errorMessage = "SUCCEED: DROP DATATYPE " + this.dtName;
      }  
    })

    // Watching for the if there is a error in a drop datatype operation 
		this.store.select(s => s.datatype.dropDatatypeError).subscribe((data: any) => {
      if (data.errors) {
        this.errorMessage = "ERROR: " + data.errors[0].msg;
      }  
    })
  }

  getDatatypes() {
    // Triggers the effect to refresg the datatypes
    this.store.dispatch(new datatypeActions.SelectDatatypes('-'));
  }

  ngOnInit() {
    // Assign the datasource for the table 
    this.dataSource = new DatatypeDataSource(this.store);
  }

   /* 
  * opens the create datatype dialog
  */
  openCreateDatatypeDialog(): void {
    let dialogRef = this.dialog.open(DialogCreateDatatype, {
      width: '420px',
      data: { name: this.datatypeName }
    });

    dialogRef.afterClosed().subscribe(result => {
      this.datatypeName = result;
    });
  }

  /* 
  * opens the drop datatype dialog
  */
  openDropDatatypeDialog(): void {
    let dialogRef = this.dialog.open(DialogDropDatatype, {
      width: '420px',
      data: { name: this.datatypeName }
    });

    dialogRef.afterClosed().subscribe(result => {
      this.datatypeName = result;
    });
  }

  onClick(): void {
    this.errorMessage = "";
  }

  /* Showing all the datatype metadata information */
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
  selector: 'datatype-create-dialog',
  templateUrl: 'datatype-create-dialog.component.html',
  styleUrls: ['datatype-create-dialog.component.scss']
})

export class DialogCreateDatatype {
  constructor(  private store: Store<any>,
                public dialogCreateDtRef: MatDialogRef<DialogCreateDatatype>,
                @Inject(MAT_DIALOG_DATA) public data: any) { }

  onClick(): void {
    this.store.dispatch(new datatypeActions.CreateDatatype(this.data.datasetName));
    this.dialogCreateDtRef.close();
  }

  onNoClick(): void {
    this.dialogCreateDtRef.close();
  }
}

@Component({
  selector: 'datatypes-drop-dialog',
  templateUrl: 'datatype-drop-dialog.component.html',
  styleUrls: ['datatype-drop-dialog.component.scss']
})

export class DialogDropDatatype {
  constructor(  private store: Store<any>,
                public dialogDropDtRef: MatDialogRef<DialogDropDatatype>,
                @Inject(MAT_DIALOG_DATA) public data: any) { }

  onClick(): void {
    this.store.dispatch(new datatypeActions.DropDatatype(this.data.datatypeName));
    this.dialogDropDtRef.close();
  }

  onNoClick(): void {
    this.dialogDropDtRef.close();
  }
}

export class DatatypeDataSource extends DataSource<any> {
    private datatypes$: Observable<any>

    constructor(private store: Store<any>) {
      super();
      this.datatypes$ = this.store.select(s => s.datatype.datatypes.results);
    }

    /** Connect function called by the table to retrieve one stream containing the data to render. */
    connect(): Observable<Datatype[]> {
        const displayDataChanges = [
          this.datatypes$,
        //  this._filterChange,
        ];

      return this.datatypes$;
    }

    disconnect() {}
}
