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
import { Component, ViewChild } from '@angular/core';
import { Observable } from 'rxjs/Observable';
import { Store } from '@ngrx/store';
import * as sqlQueryActions from '../../shared/actions/query.actions'
import * as CodeMirror from 'codemirror';

/*
 * query component
 * has editor (codemirror) for writing some query
 */
@Component({
	moduleId: module.id,
	selector: 'awc-query',
	templateUrl:'input.component.html',
	styleUrls: ['input.component.scss']
})

export class InputQueryComponent {
	private guideSelectedDataset$: Observable<any>;		
	private dataverses$: Observable<any>;	
	private datatypes$: Observable<any>;
	private datasets$: Observable<any>;	
	private indexes$: Observable<any>;	
	dataverses = [];
	datatypes = [];
	datasets = [];
	indexes = [];
	datasetName = "";
	dataverseName = "";
	queryString: string = ""
	
	/* Codemirror configuration
	*/
	codemirrorConfig = 	{ 	mode: "asterix",
							lineWrapping: true,
							showCursorWhenSelecting: true,
							autofocus: true
						}	;

	loaded$: Observable<any>

	constructor(private store: Store<any>) {
		// Watching for guide selected or clicked dataset
		this.guideSelectedDataset$ = this.store.select(s => s.dataset.guideSelectsDataset);
		this.guideSelectedDataset$.subscribe((data: any) => {
			if (data) {
				this.datasetName = data;
				for (let i = 0; i < this.datasets.length; i++) {
					if ( this.datasets[i]['DatasetName'] === this.datasetName ) {
						this.dataverseName = this.datasets[i]['DataverseName'];
					}
				}
				this.queryString = "USE " + this.dataverseName + "; SELECT * FROM " + this.datasetName;
			}
		});

		// Watching for Datatypes
		this.dataverses$ = this.store.select(s => s.dataverse.dataverses.results);
		this.dataverses$.subscribe((data: any[]) => {
			this.dataverses = data;
		});

		// Watching for Datasets
		this.datasets$ = this.store.select(s => s.dataset.datasets.results);
		this.datasets$.subscribe((data: any[]) => {
			this.datasets = data;
		});
	}

	getQueryResults(queryString: string) {
    	this.store.dispatch(new sqlQueryActions.ExecuteQuery(queryString));
  	}

	onClick() {
		this.getQueryResults(this.queryString.replace(/\n/g, " "));
	}
}
