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
import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { Dataverse } from '../../shared/models/asterixDB.model'
import { Store } from '@ngrx/store';
import { Observable } from 'rxjs/Observable';
import * as dataverseActions from '../../shared/actions/dataverse.actions'
import * as datasetActions from '../../shared/actions/dataset.actions'
import * as datatypesActions from '../../shared/actions/datatype.actions'
import * as indexesActions from '../../shared/actions/index.actions'
import * as metadataActions from '../../shared/actions/metadata.actions'
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
import * as fromRoot from '../../shared/reducers/dataverse.reducer';
import { State } from '../../shared/reducers/dataverse.reducer';
import * as sqlQueryActions from '../../shared/actions/query.actions'
/*
 * query component
 * has editor (codemirror) for writing some query
 */
@Component({
	moduleId: module.id,
	selector: 'awc-query-container',
	templateUrl:'query-container.component.html',
	styleUrls: ['query-container.component.scss']
})

export class QueryContainerComponent {
	nodes = []
	constructor(private store: Store<any>) {

		this.store.select(s => s.metadata.tree).subscribe((data: any[]) => {
			this.nodes = []
			for (let i = 0; i < data.length; i++) {
				if (data[i]['DataverseName']) {
				    let node = { id: 0, name:"", children:[] };
				    node.id = i;
				    node.name = data[i]['DataverseName'];
						for (let j = 0; j < data[i]['Datasets'].length; j++) {
							let children = { id: 0, name:"", children:[] };
							children.id = j
							children.name = data[i]['Datasets'][j]['DatasetName'];
							node.children.push(children)
						}
						this.nodes.push(node)
				}
			}
		});
	}

	treeCalc() {
		this.store.dispatch(new metadataActions.UpdateMetadataTree());
	}
}
