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
import { Component, ViewChild, ChangeDetectionStrategy, ChangeDetectorRef } from '@angular/core';
import { Observable } from 'rxjs/Observable';
import { Store } from '@ngrx/store';
import * as sqlQueryActions from '../../shared/actions/query.actions'
import * as dataverseActions from '../../shared/actions/dataverse.actions'
import * as datasetActions from '../../shared/actions/dataset.actions'
import * as datatypeActions from '../../shared/actions/datatype.actions'
import * as indexActions from '../../shared/actions/index.actions'


import * as CodeMirror from 'codemirror';

/*
 * Query metadata component
 * has editor (codemirror) for writing some query
 */
@Component({
	moduleId: module.id,
	selector: 'awc-query-metadata',
	templateUrl:'input-metadata.component.html',
	styleUrls: ['input-metadata.component.scss'],
})

export class InputQueryMetadataComponent {
	private dataverses$: Observable<any>;
	private datatypes$: Observable<any>;
	private datasets$: Observable<any>;
	private indexes$: Observable<any>;
	dataverses = [];
	datatypes = [];
	datasets = [];
	indexes = [];
	queryMetadataString: string = "";
	loaded$: Observable<any>;
	errorMessage: string = "";

  /**
  * Initialize codemirror
  */
  codemirrorMetadataConfig = 	{ 	mode: "asterix",
    //lineNumbers: true,
    lineWrapping: true,
    showCursorWhenSelecting: true,
    autofocus: true
  };

	constructor(private store: Store<any>, private changeDetector: ChangeDetectorRef) {
		this.store.select("sqlMetadataQuery").subscribe((data: any) => {
			if (data.success === false){
				if (data.sqlQueryMetadataError.errors){
					this.errorMessage = "ERROR: " + data.sqlQueryMetadataError.errors[0].msg
					this.changeDetector.detectChanges();
				}
			} else {
				this.errorMessage = "SUCCEED";

				// Refresh the tables automatically
				let stringQuery = data.sqlQueryMetadataString;
				stringQuery = stringQuery.toUpperCase();

				if (stringQuery.includes("CREATE DATAVERSE") || stringQuery.includes("DROP DATAVERSE") ){
    				this.store.dispatch(new dataverseActions.SelectDataverses('-'));
				}
				else if (stringQuery.includes("CREATE DATASET") || stringQuery.includes("DROP DATASET")){
    				this.store.dispatch(new datasetActions.SelectDatasets('-'));
				}
				else if (stringQuery.includes("CREATE TYPE") || stringQuery.includes("DROP TYPE")){
    				this.store.dispatch(new datatypeActions.SelectDatatypes('-'));
				}
				else if (stringQuery.includes("CREATE INDEX") || stringQuery.includes("DROP INDEX")){
    				this.store.dispatch(new indexActions.SelectIndexes('-'));
				}

				this.changeDetector.detectChanges();
			}

		})
	}

	getQueryResults(queryMetadataString: string) {
    	this.store.dispatch(new sqlQueryActions.ExecuteMetadataQuery(queryMetadataString));
	  }

	executeQuery() {
		this.getQueryResults(this.queryMetadataString.replace(/\n/g, " "));
		// Component View Refresh

	}

	onClick() {
		this.errorMessage = "";
	}

	/* Cleans up error message */
	cleanUp() {
		this.errorMessage = "";
	}
}
