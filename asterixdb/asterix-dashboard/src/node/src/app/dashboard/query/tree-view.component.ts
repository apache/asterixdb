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
import { Component, Input, NgZone, SimpleChange, ViewChild } from '@angular/core';
import { MatTableDataSource } from '@angular/material/table';
import { saveAs } from 'file-saver';

@Component({
    selector: 'tree-view',
    templateUrl: 'tree-view.component.html',
    styleUrls: ['tree-view.component.scss']
})

export class TreeViewComponent {
    @Input() data: any;
    @Input() queryId: any;

    jsonVisible: any = false;
    tableVisible: any = true;
    treeVisible: any = false;
    jsonData: any;
    jsonPath_: any = ': < JSON PATH >';
    rawData: any;
    treeData: any;
    treeData_: any;
    metrics: any;
    currentIndex: any = 0;
    currentRange: any;
    /* see 10 records as initial set */
    pagedefaults: any = { pageIndex: 0, pageSize:10, lenght: 0};
    pageSizeOptions = [5, 10, 25, 100, 200];
    viewMode = 'JSON';
    showGoTop = false;
    showGoBottom = false;
    EXPANDED = true;
    COLLAPSED = false;

    flattenData = [];
    dataSource = new MatTableDataSource<any>();


    private eventOptions: boolean|{capture?: boolean, passive?: boolean};

    constructor( private ngZone: NgZone) {}

    ngOnInit() {
        this.ngZone.runOutsideAngular(() => {
            window.addEventListener('scroll', this.scroll, <any>this.eventOptions);
        });
    }

    ngOnChanges(changes: SimpleChange) {
        this.rawData = this.data['results'];
        if (this.rawData) {
            this.showResults(this.pagedefaults, this.EXPANDED);
        }
    }

    /*
    * Filters the resulst array of JSON Objects
    */
    filter(element, index, array) {
        var params = Object.create(this) ;
        var startRange = (params.pageSize * params.pageIndex)
        return index >= startRange && index < startRange + params.pageSize;
    }

    showResults(range, expanded) {
        this.currentRange = range;
        this.currentIndex = this.currentRange.pageIndex;
        this.treeData = this.rawData.filter(this.filter, this.currentRange);
        // Build the dynamic table column names
        this.buildTableColums(this.treeData[0]);
        // Flat the results to display in a table
        this.flatDataforTable(this.treeData);

        if (this.treeData.length > 0) {
            this.metrics = this.data['metrics'];
            this.metrics['resultSizeKb'] = (this.metrics.resultSize/1024).toFixed(2);
            var myData_ = [];
            for (let i = 0; i < this.treeData.length; i++) {
                let  nodeContent= {};
                // mat-paginator start counting from 1, thats why the i+1 trick
                myData_.push(this.generateTree(this.treeData[i], '/', nodeContent, (this.currentRange.pageSize * this.currentRange.pageIndex) + (i + 1), 0, expanded));
            }

            this.treeData_ = myData_;
            /* Prepare the JSON view */
            this.jsonData = JSON.stringify(this.treeData, null, 8)
        } else {
            console.log('no data')
            this.treeData = [];
        }
    }

    /*
    * Shows JSON View
    */
    showJSON() {
        this.jsonVisible = true;
        this.treeVisible = false;
        this.tableVisible = false;
    }

    /*
    * Shows Table View
    */
    showTable() {
        this.jsonVisible = false;
        this.treeVisible = false;
        this.tableVisible = true;
        this.viewMode = 'TABLE';
    }

    /*
    * Shows Tree Mode
    */
    showTree() {
        this.jsonVisible = false;
        this.treeVisible = true;
        this.tableVisible = false;
        this.viewMode = 'TREE';
    }

    /*
    * Export to CSV
    */
    exportToCSV(){
        var exportOutput = JSON.stringify(this.rawData, null, 4);
        var blob = new Blob([this.jsonData], {type: "text/csv;charset=utf-8"});
        saveAs(blob, "Asterix-results.csv");
    }

    /*
    *  Export to plain text
    */
    exportToText(){
        var exportOutput = JSON.stringify(this.rawData, null, 4);
        var blob = new Blob([exportOutput], {type: "text/json;charset=utf-8"});
        saveAs(blob, "Asterix-results.json");
    }

    /*
    * This function converts the json object into a node/array graph structure ready to be display as a tree
    * it will also augment the nodes with a link containing the path that the elements occupies in the json graph
    */
    generateTree(node, nodeLink, rootMenu, index, level, expanded): any {
        // Check in case the root object is not defined properly
        if (rootMenu === {}) {
            console.log(expanded)
            rootMenu = { item: '', label: 'K', key: '', value: '', link: '/', visible: expanded, children: [], level: 0};
        }

        let nodeArray = [];

        // Going through all the keys in a node looking for objects or array of key values
        // and create a sub menu if is an object.
        Object.keys(node).map((k) => {

            if (typeof node[k] === 'object') {
                if(Array.isArray(node[k]) ){
                    let nodeObject = { nested: true, item: '', label: '', key: '', value: '', type: 'ARRAY', link: '/', visible: expanded, children: [], level: level };
                    nodeObject.item = index;
                    nodeObject.label = k;
                    nodeObject.key = k;
                    nodeObject.value = node[k];
                    nodeObject.link = nodeLink + '/' + k;
                    nodeObject.level = level;
                    level = level + 1;
                    // if this is an object then a new node is created and
                    // recursive call to find and fill with the nested elements
                    let newNodeObject = this.generateTree(node[k], nodeObject.link, nodeObject, index, level, expanded);
                    // if this is the first node, then will become the root.
                    if (rootMenu.children) {
                        rootMenu.children.push(newNodeObject)
                    } else {
                        rootMenu = newNodeObject;
                        newNodeObject.type = 'ROOT';
                    }
                } else {
                    let nodeObject = { nested: true, item: '', label: '', key: '', value: '', type: 'OBJECT', link: '/', visible: expanded, children: [], level: level };
                    nodeObject.item = index;
                    nodeObject.label = k;
                    nodeObject.key = k;
                    nodeObject.value = node[k];
                    nodeObject.link = nodeLink + '/' + k;
                    nodeObject.level = level;
                    level = level + 1;
                    // if this is an object then a new node is created and
                    // recursive call to find and fill with the nested elements
                    let newNodeObject = this.generateTree(node[k], nodeObject.link, nodeObject, index, level, expanded);
                    // if this is the first node, then will become the root.
                    if (rootMenu.children) {
                        rootMenu.children.push(newNodeObject)
                    } else {
                        nodeObject.nested = false;
                        newNodeObject.visible = expanded;
                        newNodeObject.type = 'ROOT';
                        rootMenu = newNodeObject
                    }
                }
            }
            else {
                // Array of key values converted into a unique string with a : separator
                let nodeKeyValue = { nested: false, item: '', label: '', key: '', value: '', type: 'KEYVALUE', link: '/', visible: expanded, children: [], level: level};
                nodeKeyValue.item = index;
                nodeKeyValue.label = k + " : " + node[k];
                nodeKeyValue.key = k;
                nodeKeyValue.value = node[k];
                nodeKeyValue.link = nodeLink + '/' + k + '/' + node[k];
                nodeKeyValue.level = level;
                nodeArray.push(nodeKeyValue);
            }
        })
        // The array will be added as value to a parent key.
        if (nodeArray.length > 0) {
            rootMenu.children = nodeArray.concat(rootMenu.children)
        }

        return rootMenu
    }

    gotoTop() {
        window.document.getElementById('top').scrollIntoView();
    }

    ngOnDestroy() {
        window.removeEventListener('scroll', this.scroll, <any>this.eventOptions);
    }

    scroll = ($event): void => {
        this.ngZone.run(() => {
            this.showGoTop = false;
            this.showGoBottom = true;
            var element = document.getElementById('top');
            if (element) {
                var bodyRect = document.body.getBoundingClientRect(),
                elemRect = element.getBoundingClientRect(),
                offset   = elemRect.top - bodyRect.top;
                var elementOptimizedPlan = document.getElementById('OPTIMIZED PLAN');
                var elementPlan = document.getElementById('PLAN');

                // this is calculated just manually
                var elementOptimizedPlanOffset = 0;
                if (elementOptimizedPlan) {
                    elementOptimizedPlanOffset = elementOptimizedPlan.clientHeight;
                }

                var elementPlanOffset = 0;
                if (elementPlan) {
                    elementPlanOffset = elementPlan.clientHeight;
                }

                if (window.pageYOffset > 600 + elementPlanOffset + elementOptimizedPlanOffset) {
                    this.showGoTop = true;
                } else {
                    this.showGoBottom = false;
                }
            }
        })
    }

    changeJsonPathValue(event) {
        this.jsonPath_ = event.link;
    }

    dataExpand() {
        this.showResults(this.currentRange, this.EXPANDED);
    }

    dataCollapse() {
        this.showResults(this.currentRange, this.COLLAPSED);
    }

    /*
    * Build the table column names from result data
    */
    displayedColumns: string[] = [];
    buildTableColums(item) {
        var resultKeyList = Object.keys(item);
        var resultKey: string = resultKeyList[0];
        if (item[resultKey] instanceof Object) {
            // is a SQL++ Query Results
            var nestedKeyList = Object.keys(item[resultKey]);
            this.displayedColumns = nestedKeyList;
        }
        else { // is a SQL++ Metadata Results and there is an Array
            this.displayedColumns = resultKeyList;
        }
    }

    /*
    * Flat the result data for Table display
    */
    flatDataforTable(data) {
        var resultKeyList = Object.keys(data[0]);
        var resultKey: string = resultKeyList[0];
        this.flattenData = [];
        if (data[0][resultKey] instanceof Object) {
            for (let i = 0; i < data.length; i++) {
                var nestedKeyList = Object.keys(data[i][resultKey]);
                for (let k = 0; k < nestedKeyList.length; k++) {
                    if ( typeof data[i][resultKey][nestedKeyList[k]] === 'object' ){
                        var nestedObjectStr = JSON.stringify(data[i][resultKey][nestedKeyList[k]], null, '\n');
                        // Not Implemented Yet
                    } else {
                        this.flattenData[i] = data[i][resultKey];
                    }
                }
            }
        }
        else {
            this.flattenData = data;
        }

        this.dataSource.data = this.flattenData;
    }

    jsonTransform(item) {
        return JSON.stringify(item, null, 4);
    }
}