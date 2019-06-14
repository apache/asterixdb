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
import * as cloneDeep from 'lodash/cloneDeep';

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
    displayedColumns: string[] = [];

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
        // Flat the results to display in a table
        this.BuildTableFlatData(this.treeData);

        if (this.treeData.length > 0) {
            this.metrics = this.data['metrics'];
            this.metrics['resultSizeKb'] = (this.metrics.resultSize/1024).toFixed(2);
            var myData_ = [];
            for (let i = 0; i < this.treeData.length; i++) {
                // mat-paginator start counting from 1, thats why the i+1 trick
                myData_.push(this.generateTree(this.treeData[i], '/', {}, (this.currentRange.pageSize * this.currentRange.pageIndex) + (i + 1), 0, expanded));
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
    * This function converts ONE json object into a node/array graph structure ready to be display as a tree
    * it will also augment the nodes with a link containing the path that the elements occupies in the json graph
    */
    generateTree(node, nodeLink, nodeRoot, index, level, expanded): any {

        // this for the case when the result does not have a key value
        // manually create the root node
        if(Object.keys(nodeRoot).length === 0) {
            var rootLabel = '';
            var rootType = 'ROOT'
            if (typeof node !== 'object') {
              index = ""
              rootLabel = node;
              rootType = 'ROOT-VALUE'
            }
            nodeRoot = { item: index, label: rootLabel, key: '', value: '', type: rootType, link: '/', visible: expanded, children: [], level: 0}
            level = 1;
        }

        // Going through all the keys in a node looking for objects or array of key values
        // and create a sub menu if is an object.
        let nodeArray = [];
        Object.keys(node).map((k) => {
            if (typeof node[k] === 'object') {
                let nodeObject = { nested: true, item: '', label: '', key: '', value: '', type: '', link: '/', visible: expanded, children: [], level: level };
                nodeObject.item = index;
                nodeObject.label = k;
                nodeObject.key = k;
                nodeObject.value = node[k];
                nodeObject.link = nodeLink + '/' + k;
                nodeObject.level = level;
                level = level + 1;
                if(Array.isArray(node[k]) ){
                    nodeObject.type = 'ARRAY';
                } else {
                    nodeObject.type = 'OBJECT';
                }
                var newNodeObject = this.generateTree(node[k], nodeObject.link, nodeObject, index, level, expanded);
                if (nodeRoot.children) {
                    nodeRoot.children.push(newNodeObject)
                }
            }
            else {
                // key values converted into a unique string with a : separator
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
            nodeRoot.children = nodeArray.concat(nodeRoot.children)
        }

        return nodeRoot
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
    * Flat the result data for Table display
    */
    BuildTableFlatData(data) {

        this.flattenData = [];
        this.displayedColumns = []

        for (let i = 0; i < data.length; i++) {
            if (data[i] instanceof Object) {

                var itemsKeyList = Object.keys(data[i]);
                var objectNode = cloneDeep(data[i]);

                for (let j = 0; j < itemsKeyList.length; j++) {

                    var itemsKey: string = itemsKeyList[j];
                    if (data[i][itemsKey] instanceof Object) {
                        objectNode[itemsKey] = JSON.stringify(data[i][itemsKey], null, '\n');
                    } else {
                        objectNode[itemsKey] = data[i][itemsKey]
                    }
                    if (this.displayedColumns.indexOf(itemsKey) === -1) {
                        this.displayedColumns.push(itemsKey)
                    }
                }
                this.flattenData.push(objectNode)
            } else {
                this.displayedColumns.push('value')
                this.flattenData.push({ 'value': data[0] })
            }
        }

        this.dataSource.data = this.flattenData;
    }

    jsonTransform(item) {
        return JSON.stringify(item, null, 4);
    }

    checkView() {
        if (!this.treeVisible) {
            return true;
        } else {
            return false
        }
    }
}