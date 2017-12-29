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
import { Component, OnInit, ChangeDetectorRef } from '@angular/core';
import { Router } from '@angular/router';
import { Dataverse } from '../../shared/models/asterixDB.model';
import { Store } from '@ngrx/store';
import { Observable } from 'rxjs/Observable';
import * as dataverseActions from '../../shared/actions/dataverse.actions';
import * as datasetActions from '../../shared/actions/dataset.actions';
import * as datatypesActions from '../../shared/actions/datatype.actions';
import * as indexesActions from '../../shared/actions/index.actions';
import * as metadataActions from '../../shared/actions/metadata.actions';
import * as datasetsActions from '../../shared/actions/dataset.actions';
import { ElementRef, ViewChild} from '@angular/core';
import {DataSource} from '@angular/cdk/collections';
import {BehaviorSubject} from 'rxjs/BehaviorSubject';
import 'rxjs/add/operator/startWith';
import 'rxjs/add/observable/merge';
import 'rxjs/add/operator/map';
import 'rxjs/add/operator/debounceTime';
import 'rxjs/add/operator/distinctUntilChanged';
import 'rxjs/add/observable/fromEvent';
import { Subscription } from 'rxjs/Rx';
import * as fromRoot from '../../shared/reducers/dataverse.reducer';
import { State } from '../../shared/reducers/dataverse.reducer';
import { TreeModule, TreeNode} from 'primeng/primeng';


/**
 * query component
 * has editor (codemirror) for writing some query
 */
@Component({
	moduleId: module.id,
	selector: 'awc-metadata',
	templateUrl: 'metadata.component.html',
	styleUrls: ['metadata.component.scss']
})

export class MetadataComponent implements OnInit {
	nodesAll = [];
	nodesDatasets = [];
	nodesDatatypes = [];
	nodesIndexes = [];

	constructor(private store: Store<any>, private changeDetector: ChangeDetectorRef) {}

	ngOnInit(): void {

		// Watching for the metadata tree
		this.store.select(s => s.metadata.tree).subscribe((data: any[]) => {
			
			this.nodesAll = [];
			this.nodesDatasets = [];
			this.nodesDatatypes = [];
			this.nodesIndexes = [];
			const indexesMenu = [];
			const datatypesMenu = [];
			const datasetsMenu = [];
			const dataversesRoot = { label: '', children: []};
			dataversesRoot.label = 'DATAVERSES';
			dataversesRoot.children = [];

			for (let i = 0; i < data.length; i++) {

				// Don't want to show metadata system datasets, datatypes or indexes
			// if (data[i]['DataverseName'] && data[i]['DataverseName'] !== "Metadata" )
			//	{
					// Counting dataverses to align the menu identifiers
			    	const dataverse = { label: '', children: [] }; 
					dataverse.label = data[i]['DataverseName'];
					dataversesRoot.children.push(dataverse);

					// Adding the datasets to correspondent dataverse
					if (data[i]['Datasets'].length) {
						const datasetRoot = { label: '', children: [] }; 
						datasetRoot.label = 'DATASETS';
						dataverse.children.push(datasetRoot);
						for (let j = 0; j < data[i]['Datasets'].length; j++) {
							const dataset = { label: '', children: [] }; 
							dataset.label = data[i]['Datasets'][j]['DatasetName'];

							//
							// Adding the datatype to correspondent dataset
							//
							if (data[i]['Datasets'][j]['Datatype']) {
								const datatypeRoot = { label: '', children: [] };
								datatypeRoot.label = 'Datatype: ' + data[i]['Datasets'][j]['Datatype']['DatatypeName'];
								//
								// Looking for the datatype fields
								//
								if (data[i]['Datasets'][j]['Datatype']['Derived']) {
									if (data[i]['Datasets'][j]['Datatype']['Derived']['Record']) { 
										const datatypeFieldsRoot = { label: '', leaf: true, expanded: true, children: [] };
										datatypeFieldsRoot.label = 'FIELDS';
										for (let k = 0; k < data[i]['Datasets'][j]['Datatype']['Derived']['Record']['Fields'].length; k++) {
											const datatypeField = { label: '', children: [] }; 
											datatypeField.label = data[i]['Datasets'][j]['Datatype']['Derived']['Record']['Fields'][k]['FieldName'] + ": " + data[i]['Datasets'][j]['Datatype']['Derived']['Record']['Fields'][k]['FieldType'];
											datatypeFieldsRoot.children.push(datatypeField);
										}
										datatypeRoot.children.push(datatypeFieldsRoot);

									}
								}
								dataset.children.push(datatypeRoot);

								datatypeRoot.label = data[i]['Datasets'][j]['Datatype']['DatatypeName'];
								datatypesMenu.push(datatypeRoot);
							}

							//
							// Adding the indexes to correspondent dataset
							//
							if (data[i]['Datasets'][j]['Indexes'].length) {
								const indexRoot = { label: '', children: [] }; 
								indexRoot.label = 'INDEXES';
								
								for (let k = 0; k < data[i]['Datasets'][j]['Indexes'].length; k++) {
									const indexChild = { label: '', children: [] }; 
									indexChild.label = data[i]['Datasets'][j]['Indexes'][k]['IndexName'];

									// is Primary
									const indexIsPrimaryRoot = { label: '', children: [] };
									indexIsPrimaryRoot.label = 'isPrimary' + ': ' + data[i]['Datasets'][j]['Indexes'][k]['IsPrimary'];
									indexChild.children.push(indexIsPrimaryRoot);
								
									// SearchKey
									if (data[i]['Datasets'][j]['Indexes'][k]['SearchKey']) {
										const indexSearchKeyRoot = { label: '', children: [] };
										indexSearchKeyRoot.label = 'SEARCH KEY';
										for (let l = 0; l < data[i]['Datasets'][j]['Indexes'][k]['SearchKey'].length; l++) {
											const indexsearchKeyField = { label: '', children: [] };
											indexsearchKeyField.label = data[i]['Datasets'][j]['Indexes'][k]['SearchKey'][l]
											indexSearchKeyRoot.children.push(indexsearchKeyField);
										}

										indexChild.children.push(indexSearchKeyRoot);
										indexesMenu.push(indexChild);
									}

									indexRoot.children.push(indexChild);
								}

								dataset.children.push(indexRoot);
								datasetRoot.children.push(dataset);
								datasetsMenu.push(dataset);
							}
						}
					}
			//	}
			}
			
			this.nodesAll.push(dataversesRoot);

			/*
			* Making the rest of the stand alone submenus
			*/

			// Adding the DATASET stand alone submenu
			const datasetMenuRoot = { label: '', children: [] };
			datasetMenuRoot.label = 'DATASETS';
			datasetMenuRoot.children = datasetsMenu;
			this.nodesDatasets.push(datasetMenuRoot);

			// Adding the DATATYPES stand alone submenu
			const datatypeMenuRoot = { label: '', children: [] };
			datatypeMenuRoot.label = 'DATATYPES';
			datatypeMenuRoot.children = datatypesMenu;
			this.nodesDatatypes.push(datatypeMenuRoot);

			// Adding the DATATYPE stand alone submenu
			const indexesMenuRoot = { label: '', children: [] };
			indexesMenuRoot.label = 'INDEXES';
			indexesMenuRoot.children = indexesMenu;
			this.nodesIndexes.push(indexesMenuRoot);

			// Component View Refresh
			this.changeDetector.detectChanges();
		});
	}

	/*
	* UI helpers to select dataverses from the guide menu
	*/
	nodeSelectAll(event) {
		if (event.node.parent && event.node.parent.label === 'DATASETS') {
			const datasetName = event.node.label.replace(/-;-/g);
			this.store.dispatch(new datasetsActions.GuideSelectDatasets(datasetName));
		}
	}
	
	nodeSelectDataset(event) {
		if (event.node.parent && event.node.parent.label === 'DATASETS') {
			const datasetName = event.node.label.replace(/-;-/g);
			this.store.dispatch(new datasetsActions.GuideSelectDatasets(datasetName));
		}
	}
}
