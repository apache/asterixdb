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
import { Component, ChangeDetectionStrategy, Inject, OnInit, AfterViewChecked, AfterViewInit, Input} from '@angular/core';
import { Dataverse } from '../../../shared/models/asterixDB.model'
import { Store } from '@ngrx/store';
import { Observable } from 'rxjs/Observable';
import * as dataverseActions from '../../../shared/actions/dataverse.actions'
import { ElementRef, ViewChild} from '@angular/core';
import { DataSource } from '@angular/cdk/collections';
import { BehaviorSubject } from 'rxjs/BehaviorSubject';
import 'rxjs/add/operator/startWith';
import 'rxjs/add/observable/merge';
import 'rxjs/add/operator/map';
import 'rxjs/add/operator/debounceTime';
import 'rxjs/add/operator/distinctUntilChanged';
import 'rxjs/add/observable/fromEvent';
import { Subscription } from "rxjs/Rx";
import * as fromRoot from '../../../shared/reducers/dataverse.reducer';
import { State } from '../../../shared/reducers/dataverse.reducer';
import { MatPaginator } from '@angular/material';
import { SelectionModel } from '@angular/cdk/collections';
import { MatDialog, MatDialogRef, MAT_DIALOG_DATA } from '@angular/material';

@Component({
  selector: 'awc-dataverses',
  templateUrl: 'dataverses.component.html',
  styleUrls: ['dataverses.component.scss'],
})

export class DataverseCollection implements OnInit, AfterViewChecked, AfterViewInit {
  dataverseName: string;
  displayedColumns = ['DataverseName', 'Dataformat', 'Timestamp', 'PendingOp'];
  dataSource: DataversesDataSource | null;
  loaded$: Observable<any>;
  selection = new SelectionModel<string>(true, []);
  @Input('message') errorMessage: string = ""
  dvName = "";

  constructor(private store: Store<any>, public dialog: MatDialog) {
    this.loaded$ = this.store.select('dataverse');
    
    // Watching the name of the latest created dataset
    this.store.select(s => s.dataverse.createDataverseName).subscribe((data: any) => {
      this.dvName = data;
    })

    // Watching for the success message in a drop dataset operation 
    this.store.select(s => s.dataverse.dropDataverseName).subscribe((data: any) => {
      this.dvName = data;
    })

    // Watching for the if there is a change in the collection
		this.store.select(s => s.dataverse.createDataverseSuccess).subscribe((data: any) => {
      if (data === true) {
        this.getDataverse();
        this.errorMessage = "SUCCEED: CREATED DATAVERSE " + this.dvName + " ";
      }  
    })

    // Watching for the if there is a error in a create dataverse operation 
		this.store.select(s => s.dataverse.createDataverseError).subscribe((data: any) => {
      if (data.errors) {
        this.errorMessage = "ERROR: " + data.errors[0].msg;
      }  
    })

    // Watching for the if there is a change in the collection
		this.store.select(s => s.dataverse.dropDataverseSuccess).subscribe((data: any) => {
      if (data === true) {
        this.getDataverse();
        this.errorMessage = "SUCCEED: DROP DATAVERSE " + this.dvName;
      }  
    })

    // Watching for the if there is a error in a drop dataverse operation 
		this.store.select(s => s.dataverse.dropDataverseError).subscribe((data: any) => {
      if (data.errors) {
        this.errorMessage = "ERROR: " + data.errors[0].msg;
      }  
    })
  }

  getDataverse() {
    // Triggers the effect to refresg the dataverse
    this.store.dispatch(new dataverseActions.SelectDataverses('-'));
  }

  ngOnInit() {
    // Assign the datasource for the table 
    this.dataSource = new DataversesDataSource(this.store);
  }

  ngAfterViewChecked() {}

  ngAfterViewInit() {}


  /* 
  * opens the create dataverse dialog
  */
  openCreateDataverseDialog(): void {
    let dialogRef = this.dialog.open(DialogCreateDataverse, {
      width: '420px',
      data: { name: this.dataverseName }
    });

    dialogRef.afterClosed().subscribe(result => {
      //reference code
      //this.dvName = result;
    });
  }

  /* 
  * opens the drop dataverse dialog
  */
  openDropDataverseDialog(): void {
    let dialogRef = this.dialog.open(DialogDropDataverse, {
      width: '420px',
      data: { name: this.dataverseName }
    });

    dialogRef.afterClosed().subscribe(result => {
      this.dataverseName = result;
    });
  }

   /* 
  * Clean up the error message on the screen
  */
  onClick(): void {
    this.errorMessage = "";
  }

  selectedRowIndex: number = -1;

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
  selector: 'dataverse-create-dialog',
  templateUrl: 'dataverses-create-dialog.component.html',
  styleUrls: ['dataverses-create-dialog.component.scss']
})

export class DialogCreateDataverse {
  constructor(  private store: Store<any>,
                public dialogCreateDvRef: MatDialogRef<DialogCreateDataverse>,
                @Inject(MAT_DIALOG_DATA) public data: any) { }

  onClick(): void {
    this.store.dispatch(new dataverseActions.CreateDataverse(this.data.dataverseName));
    this.dialogCreateDvRef.close(this.data.dataverseName);
  }

  onNoClick(): void {
    this.dialogCreateDvRef.close();
  }
}

@Component({
  selector: 'dataverse-drop-dialog',
  templateUrl: 'dataverses-drop-dialog.component.html',
  styleUrls: ['dataverses-drop-dialog.component.scss']
})

export class DialogDropDataverse {
  constructor(  private store: Store<any>,
                public dialogDropDvRef: MatDialogRef<DialogDropDataverse>,
                @Inject(MAT_DIALOG_DATA) public data: any) { }

  onClick(): void {
    this.store.dispatch(new dataverseActions.DropDataverse(this.data.dataverseName));
    this.dialogDropDvRef.close(this.data.dataverseName);
  }

  onNoClick(): void {
    this.dialogDropDvRef.close();
  }
}

/**
 * Data source to provide what data should be rendered in the table. Note that the data source
 * can retrieve its data in any way. In this case, the data source is provided a reference
 * to a common data base, ExampleDatabase. It is not the data source's responsibility to manage
 * the underlying data. Instead, it only needs to take the data and send the table exactly what
 * should be rendered.
 */
 export class DataversesDataSource extends DataSource<any> {
    dataverse$: Observable<any>
    _filterChange = new BehaviorSubject('');
    get filter(): string { return this._filterChange.value; }
    set filter(filter: string) { this._filterChange.next(filter); }

    constructor(private store: Store<any>) {
      super();
      this.dataverse$ = this.store.select(s => s.dataverse.dataverses.results);
    }

    /** Connect function called by the table to retrieve one stream containing the data to render. */
    connect(): Observable<Dataverse[]> {
        const displayDataChanges = [
          this.dataverse$,
        ];

      return this.dataverse$;
    }

    disconnect() {}
  }
