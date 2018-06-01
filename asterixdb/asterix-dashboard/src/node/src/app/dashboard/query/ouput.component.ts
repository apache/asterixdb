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
import { Component, OnInit, ViewChild,  AfterViewInit, ChangeDetectorRef, Pipe, PipeTransform } from '@angular/core';
import { Observable } from 'rxjs/Observable';
import { Store } from '@ngrx/store';
import * as sqlQueryActions from '../../shared/actions/query.actions'
import { saveAs } from 'file-saver';
import { DomSanitizer } from '@angular/platform-browser';
import {TreeModule,TreeNode} from 'primeng/primeng';

/**
 * query component
 * has editor (codemirror) for writing some query
 */

@Pipe({ name: 'safeHtml'})
export class SafeHtmlPipe implements PipeTransform  {
  constructor(private sanitized: DomSanitizer) {}
  transform(value) {
    return this.sanitized.bypassSecurityTrustHtml(value);
  }
}

@Component({
	moduleId: module.id,
	selector: 'awc-results',
	templateUrl:'output.component.html',
	styleUrls: ['output.component.scss']
})


export class QueryOutputComponent implements OnInit {
	queryMessage: string;
	treeData = [];
	flattenData = [];
	dataColumns = [];
	query_message: string;
	execution_time: number;
	loaded$: Observable<any>
	data: any[];
	loading: Boolean;
	jsonOutput = "";
	selectedOutputView = "NONE";
	outputQueryString = "";
	toogleExpand = "EXPAND TREE"
	
	/* Codemirror configuration */
	codemirrorConfig = 	{ 	mode: "asterix",
		lineWrapping: true,
		showCursorWhenSelecting: true
	};

	generateTreeMenu(node, rootMenu): any {

		// Check in case the root object is not defined properly
		if (rootMenu === undefined) {
			rootMenu = { label: '', children: []};
		}

		let nodeArray = [];
		
		// Going through all the keys in a node looking for objects or array of key values
		// and create a sub menu if is an object.
		Object.keys(node).map((k) => {		

			if (typeof node[k] === 'object') {
				let nodeObject = { label: '', children: []};
				nodeObject = { label: '', children: []};
				nodeObject.label = k;
				// if this is an object then a new node is created and
				// recursive call to find and fill with the nested elements
				let newNodeObject = this.generateTreeMenu(node[k], nodeObject);

				// if this is the first node, then will become the root.
				if (rootMenu.children) {
					rootMenu.children.push(newNodeObject)
				} else {
					rootMenu = newNodeObject
				}
			}
			else {
				// Array of key values converted into a unique string with a : separator 
				let nodeKeyValue = { label: '', children: []};
				nodeKeyValue.label = k + " : " + node[k]
				nodeArray.push(nodeKeyValue);
			}
		})

		// The array will be added as value to a parent key.
		if (nodeArray.length > 0) {
			rootMenu.children = nodeArray.concat(rootMenu.children)
		}
		
		return rootMenu 
	}

	constructor(private store: Store<any>, private changeDetector: ChangeDetectorRef) {
		this.loaded$ = this.store.select(s => s.sqlQuery.loaded);
		this.store.select("sqlQuery").subscribe((data: any) => {
			// Set the output toolbar query string and default view settings
			if (data.loaded) {
				this.selectedOutputView = "TABLE";
				this.loading = true;
				this.data = data.sqlQueryResult.results;
				this.treeData = [];
				let stringQuery = data.sqlQueryString;
	
				// Preparing the toolbar 
				if (stringQuery.length > 150) {
					this.outputQueryString = ": " + stringQuery.slice(0, 150) + " (..)"
				} else {
					this.outputQueryString = ": " + stringQuery;
				}

				// Processing the results 
				if (data.sqlQueryResult.results && data.sqlQueryResult.results.length > 0 && this.data[0]) {

					/* Removing the root object, disabled for the time being 
					var resultKeyList = Object.keys(this.data[0]);
					var resultKey: string = resultKeyList[0]; 
					*/

					for (let i = 0; i < this.data.length; i++) {

						/* Removing the root object, disabled for the time being 
						if (this.data[i][resultKey] instanceof Object) {	
							this.data[i] = this.data[i][resultKey];
						}*/	

						let nodeContent = { label:"[" + i + "]" , children: []};
						this.treeData.push(this.generateTreeMenu(this.data[i], nodeContent))
					}

					this.loading = false;
				} 
	
				// JSON OUTPUT
				// Making into a JSON String for JSON String Output
				this.jsonOutput = JSON.stringify(data.sqlQueryResult.results, null, 2)
				
				// TABLE OUTPUT
				if (this.data && this.data.length > 0) {

					this.collapseAll();
					// Normalize the data ( removing the first key if is an object )
					// TODO: Move it into a recursive function.
					this.dataColumns = [];

					var resultKeyList = Object.keys(this.data[0]);
					var resultKey: string = resultKeyList[0]; 
					if (this.data[0][resultKey] instanceof Object) {	
						// is a SQL++ Query Results 
						var nestedKeyList = Object.keys(this.data[0][resultKey]);
						for (let i = 0; i < nestedKeyList.length; i++) {
							if (typeof this.data[0][resultKey][nestedKeyList[i]] === 'object') {
								// Creating a key to display a nested type
								this.dataColumns.push({field: 'nestedString' + i, header: nestedKeyList[i]})
								 				
							} else {
								this.dataColumns.push({field: nestedKeyList[i], header: nestedKeyList[i] })
							}
							
						}
					}
					else { // is a SQL++ Metadata Results and there is an Array
						for (let i = 0; i < resultKeyList.length; i++) {
							this.dataColumns.push({field: resultKeyList[i], header: resultKeyList[i] })
						}
					}

					// Now prepare the data ( SQL++ Query, Metatada Queries no need to change anything ).
					// TODO: Move it into a recursive function.
					if (this.data[0][resultKey] instanceof Object) {	
						// is a SQL++ Query Results 
						for (let i = 0; i < this.data.length; i++) {

							// // is a SQL++ Query Results 
							var nestedKeyList = Object.keys(this.data[i][resultKey]);
							for (let k = 0; k < nestedKeyList.length; k++) {
								if ( typeof this.data[i][resultKey][nestedKeyList[k]] === 'object' ){
										// Creating a display value to for a nested type JSON.stringify(jsObj, 
										var nestedObjectStr = JSON.stringify(this.data[i][resultKey][nestedKeyList[k]], null, '\n');
										var nestedKey = 'nestedString' + k;
										this.data[i][resultKey][nestedKey] = nestedObjectStr; 				
								} 
							}

							this.data[i] = this.data[i][resultKey];
						}	
					}
				}
			}
      	});
	}

	/* 
	* Subscribing to store values
	*/
	ngOnInit(): void {
		this.loaded$ = this.store.select('sqlQuery');
		this.store.select("sqlQuery").subscribe((data: any) => {
			if (data.sqlQueryError.errors){
				this.queryMessage = data.sqlQueryError.errors[0].msg
			}else{
				this.queryMessage = ""
			}
		})
	}

	/* 
	* Changes view mode [ TABLE, TREE, JSON VIEW ]
	*/
	onSelect(value: any) {
		this.selectedOutputView = value;		
	}

	/* 
	* Export to CSV 
	*/
    exportToCSV(){
		var blob = new Blob([this.jsonOutput], {type: "text/csv;charset=utf-8"});
		saveAs(blob, "Asterix-results.csv");
	}
	
	/*
	*  Export to plain text
	*/
    exportToText(){
		var exportOutput = this.jsonOutput;
		var blob = new Blob([exportOutput], {type: "text/plain;charset=utf-8"});
		saveAs(blob, "Asterix-results.txt");
	}
	
	/*
	*  Expand/Collapse Tree
	*/
    expandTree(){
		if (this.toogleExpand === "EXPAND TREE"){
			this.expandAll();
		} else {
			this.collapseAll();
		}
	}
	
	expandAll(){
		this.toogleExpand = "TREE COLLAPSE";
        this.treeData.forEach( node => {
            this.expandRecursive(node, true);
        } );
    }

    collapseAll(){
		this.toogleExpand = "EXPAND TREE";
        this.treeData.forEach( node => {
            this.expandRecursive(node, false);
        } );
    }
    
    private expandRecursive(node:TreeNode, isExpand:boolean){
        node.expanded = isExpand;
        if(node.children){
            node.children.forEach( childNode => {
                this.expandRecursive(childNode, isExpand);
            } );
        }
    }
}
