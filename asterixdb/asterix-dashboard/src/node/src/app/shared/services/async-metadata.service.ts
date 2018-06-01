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
import { Injectable, ApplicationRef  } from '@angular/core';
import { Store } from '@ngrx/store';
import { Observable } from "rxjs/Observable";
import 'rxjs/add/operator/map';
import 'rxjs/add/observable/from';
import * as dataverseActions from '../../shared/actions/dataverse.actions'
import * as datasetActions from '../../shared/actions/dataset.actions'
import * as datatypesActions from '../../shared/actions/datatype.actions'
import * as indexesActions from '../../shared/actions/index.actions'
import * as metadataActions from '../../shared/actions/metadata.actions'

/*
	Metadata service watch any changes in Dataverses, Datasets, Datatypes, indexes
	state in store and builds a tree state structure with datasets->dataverse->datatype->index relationship
*/
@Injectable()
export class MetadataService {

	/* Arrays to expose updated dataverses, datasets, datatypes,
	indexes collections*/
	dv = [];
	ds = [];
	dt = [];
	idx = [];

	/*
	* contructor will initialize the store state watchers
	*/
	constructor(private store: Store<any>, private ref: ApplicationRef ) {

		/* Watching changes in dataverse collection */
		this.store.select(s => s.dataverse.dataverses).subscribe((data: any) => {
			if (data.results) {
				this.dv = []
				for (let i = 0; i < data.results.length; i++) {
						let node = { id: 0, DataverseName: "", Datasets:[] }
						node.id = i
						node.DataverseName = data.results[i]['DataverseName']
						this.dv.push(node)
				}
				this.updateMetadataTree();
			}
		})

		/* Watching changes in datasets collections */
		this.store.select(s => s.dataset.datasets).subscribe((data: any) => {
			if (data.results) {
				this.ds = data.results;
				this.updateMetadataTree();
			}
		})

		/* Watching changes in datatypes collections */
		this.store.select(s => s.datatype.datatypes).subscribe((data: any) => {
			if (data.results) {
				this.dt = data.results;
				this.updateMetadataTree();
			}
		})

		/* Watching changes in index collections */
		this.store.select(s => s.index.indexes).subscribe((data: any) => {
			if (data.results) {
				this.idx = data.results;
				this.updateMetadataTree();
			}
		})
	}

	/*
	*	convenience function to update and return the metadata tree.
	*/
  	getMetadataTree(): Observable<any[]> {
		return Observable.from(this.dv);
	}
	  
  	updateMetadataTree() {
	for (let i = 0; i < this.dv.length; i++) {
		// Filling datasets
		this.dv[i]['Datasets'] = [];
		for (let j = 0; j < this.ds.length; j++) {
			if (this.ds[j]['DataverseName'] === this.dv[i]['DataverseName']){

				// Filling datatypes, there is one datatype per dataset
				this.ds[j]['Datatype'] = [];
				for (let k = 0; k < this.dt.length; k++) {
					if (this.dt[k]['DatatypeName'] === this.ds[j]['DatatypeName']){
						this.ds[j]['Datatype'] = this.dt[k]; // push(this.dt[k])
					}
				}

				// Filling indexes
				this.ds[j]['Indexes'] = [];
				for (let l = 0; l < this.idx.length; l++) {
					if (this.idx[l]['DatasetName'] === this.ds[j]['DatasetName']){
						this.ds[j]['Indexes'].push(this.idx[l])
					}
				}

				this.dv[i]['Datasets'].push(this.ds[j])
			}
		}
	}

	this.store.dispatch(new metadataActions.UpdateMetadataTree());
	}	
}
