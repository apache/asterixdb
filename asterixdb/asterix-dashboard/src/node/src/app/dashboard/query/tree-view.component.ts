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
import { Component, Input, NgZone, SimpleChange, ViewChild, Inject } from '@angular/core';
import {MatDialog, MatDialogRef, MAT_DIALOG_DATA} from '@angular/material/dialog';
import {MatFooterCell, MatTableDataSource} from '@angular/material/table';
import { saveAs } from 'file-saver';
import * as cloneDeep from 'lodash/cloneDeep';

export interface DialogData {
  exportFormat: string;
  fileName: string;
}

@Component({
    selector: 'tree-view',
    templateUrl: 'tree-view.component.html',
    styleUrls: ['tree-view.component.scss']
})

export class TreeViewComponent {
    @Input() data: any;
    @Input() queryId: any;
    @Input() planFormat: any;
    @Input() plan: any;
    @Input() planName: any;
    @Input() jsonPlan: any;
    @Input() inputToOutput: Object;

    isExplain: boolean = false;
    outputFormat: string = 'json';
    isCSV: boolean = false;
    hasHeader: boolean = false;
    exportFormat: string = 'json';
    exportFileName: string = 'asterixdb-query-results';

    jsonVisible: any = true;
    jsonlVisible: any = true;
    tableVisible: any = false;
    treeVisible: any = false;
    planVisible: any = false;
    jsonData: any;
    jsonlData: any;
    jsonPath_: any = ': < JSON PATH >';
    rawData: any;
    treeData: any;
    treeData_: any;
    metrics: any;
    currentIndex: any = 0;
    currentRange: any;
    /* see 10 records as initial set */
    pagedefaults: any = { pageIndex: 0, pageSize:10, lenght: 0};
    pageSize = 10;
    pageSizeOptions = [5, 10, 25, 100, 200, 300, 400];
    viewMode = 'JSON';
    showGoTop = false;
    showGoBottom = false;
    EXPANDED = true;
    COLLAPSED = false;

    flattenData = [];
    displayedColumns: string[] = [];

    dataSource = new MatTableDataSource<any>();


    private eventOptions: boolean|{capture?: boolean, passive?: boolean};

    constructor( private ngZone: NgZone, public dialog: MatDialog) {}

    ngOnInit() {
        this.ngZone.runOutsideAngular(() => {
            window.addEventListener('scroll', this.scroll, <any>this.eventOptions);
        });
    }

    ngOnChanges(changes: SimpleChange) {
        if (this.inputToOutput) {
            this.jsonVisible = true;
            this.jsonlVisible = false;
            this.planVisible = false;
            this.tableVisible = false;
            this.treeVisible = false;
            this.viewMode = 'JSON';

            this.isExplain = this.inputToOutput['isExplain'];
            this.outputFormat = this.inputToOutput['outputFormat'];

            if (this.outputFormat == 'CSV_header') {
              this.hasHeader = true;
              this.outputFormat = 'CSV';
            }

            if (this.outputFormat == 'CSV' && !this.isExplain) {
              this.exportFileName = 'asterixdb-query-results';
              this.exportFormat = 'csv'
              this.isCSV = true;
              this.jsonVisible = false;
              this.tableVisible = true;
              this.viewMode = "TABLE";
            }

            if (this.data.length == 0) {
              //case where 0 objects are returned
              this.rawData = [];
              this.showResults(this.pagedefaults, this.EXPANDED);
            } else {
              this.rawData = this.data['results'];
              if (this.rawData) {
                this.showResults(this.pagedefaults, this.EXPANDED);
              }
            }

            if (this.isExplain) {
              this.jsonVisible = false;
              this.jsonlVisible = false;
              this.planVisible = true;
              this.tableVisible = false;
              this.treeVisible = false;
              this.viewMode = "PLAN";
            }
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
        // update pagesize
        this.pageSize = range.pageSize;

        this.currentRange = range;
        this.currentIndex = this.currentRange.pageIndex;

        if (this.rawData.length > 0) {
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
            this.jsonlData = this.jsonlinesTransform(this.treeData);
          } else {
            console.log('no data')
            this.treeData = [];
          }
        } else {
          this.treeData = [];
          this.jsonData = JSON.stringify([ ], null, 8);
          this.jsonlData = "";
          this.metrics = {"resultCount": 0};

          //clear tree data
          var myData_ = [];
          for (let i = 0; i < this.treeData.length; i++) {
            // mat-paginator start counting from 1, thats why the i+1 trick
            myData_.push(this.generateTree(this.treeData[i], '/', {}, (this.currentRange.pageSize * this.currentRange.pageIndex) + (i + 1), 0, expanded));
          }

          this.treeData_ = myData_;

          //clear table data
          this.dataSource.data = [];
          this.displayedColumns = [];
        }

    }

    /*
    * Shows JSON View
    */
    showJSON() {
        this.jsonVisible = true;
        this.jsonlVisible = false;
        this.treeVisible = false;
        this.tableVisible = false;
        this.planVisible = false;
        this.viewMode = 'JSON'
    }

    /*
    * Shows JSONL View
     */
    showJSONL() {
      this.jsonVisible = false;
      this.jsonlVisible = true;
      this.treeVisible = false;
      this.tableVisible = false;
      this.planVisible = false;
      this.viewMode = 'JSONL'
    }

    /*
    * Shows Table View
    */
    showTable() {
        this.jsonVisible = false;
        this.jsonlVisible = false;
        this.treeVisible = false;
        this.tableVisible = true;
        this.planVisible = false;
        this.viewMode = 'TABLE';
    }

    /*
    * Shows Tree Mode
    */
    showTree() {
        this.jsonVisible = false;
        this.jsonlVisible = false;
        this.treeVisible = true;
        this.tableVisible = false;
        this.planVisible = false;
        this.viewMode = 'TREE';
    }

    /*
    * Shows Plan Viewer Mode
     */
    showPlan() {
      this.jsonVisible = false;
      this.jsonlVisible = false;
      this.treeVisible = false;
      this.tableVisible = false;
      this.planVisible = true;
      this.viewMode = 'PLAN';
    }

    /*
    * Export to CSV
    */
    exportToCSV(){
      var csvJoin = this.rawData.join("");
      var blob = new Blob([csvJoin], {type: "text/csv;charset=utf=8"});
      if (this.exportFileName == "") {
        saveAs(blob, "asterixdb-query-results.csv");
        this.exportFileName = "asterixdb-query-results";
      }
      else {
        saveAs(blob, this.exportFileName + ".csv");
      }
    }

    /*
    *  Export to JSON
    */
    exportToJSON(){
        var exportOutput = JSON.stringify(this.rawData, null, 4);
        var blob = new Blob([exportOutput], {type: "text/json;charset=utf-8"});
        if (this.exportFileName == "") {
          saveAs(blob, "asterixdb-query-results.json");
          this.exportFileName = "asterixdb-query-results";
        }
        else
          saveAs(blob, this.exportFileName + ".json");
    }

    /*
    * Export to JSON Lines (JSONL)
    */
    exportToJSONLines() {
      var exportOutput = this.jsonlinesTransform(this.rawData);
      var blob = new Blob([exportOutput], {type: "text/json;charset=utf-8"});
      if (this.exportFileName == "") {
        saveAs(blob, "asterixdb-query-results.jsonl");
        this.exportFileName = "asterixdb-query-results";
      }

      else
        saveAs(blob, this.exportFileName + ".jsonl");
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

        if (node != null) {
          Object.keys(node).map((k) => {
            if (typeof node[k] === 'object' && (node[k] != null || node[k] != undefined)) {
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
        }

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

    /*
    * Build the table column names from result data
    * Flat the result data for Table display
    */
    BuildTableFlatData(data) {
      if (this.isCSV)
        this.buildTableFlatCSVData(data);
      else
        this.buildTableFlatJSONData(data);
    }

    /*
    * Reads JSON and creates data for table display
     */
    buildTableFlatJSONData(data) {
      this.flattenData = [];
      this.displayedColumns = []

      const replacer = (key, value) => typeof value === 'undefined' ? null : value;

      for (let i = 0; i < data.length; i++) {
        if (data[i] instanceof Object) {

          var itemsKeyList = Object.keys(data[i]);
          var objectNode = cloneDeep(data[i]);

          for (let j = 0; j < itemsKeyList.length; j++) {

            var itemsKey: string = itemsKeyList[j];
            if (data[i][itemsKey] instanceof Object) {
              objectNode[itemsKey] = JSON.stringify(data[i][itemsKey], replacer, '\n');
            } else {
              objectNode[itemsKey] = data[i][itemsKey]
            }
            if (this.displayedColumns.indexOf(itemsKey) === -1) {
              this.displayedColumns.push(itemsKey)
            }
          }
          this.flattenData.push(objectNode)
        } else {
          //only create one column called "value" if you are doing a select value
          if (i == 0)
            this.displayedColumns.push('value')
          this.flattenData.push({ 'value': data[i] })
        }
      }

      this.dataSource.data = this.flattenData;
    }

    /*
    * Reads CSV data and creates data for table display
     */
    buildTableFlatCSVData(data) {
      this.flattenData = [];
      this.displayedColumns = [];

      for (let i = 0; i < data.length; i++) {
        //split the string using the comma delimiter
        let items = this.readCSV(data[i]);

        //iterate over these elements
        let item_idx = 0;
        let row_obj = {};

        for (let item of items) {
          //if it is the header row
          if (this.hasHeader && i == 0) {
            this.displayedColumns.push(item);
          } else if (!this.hasHeader && i == 0) {
            this.displayedColumns.push("Column " + (item_idx + 1).toString());
            row_obj["Column " + (item_idx + 1).toString()] = item;
          } else {
            if (this.displayedColumns.length > 0) {
              //has header
              row_obj[this.displayedColumns[item_idx]] = item;
            } else {
              //does not have header
              row_obj["Column " + (item_idx + 1).toString()] = item;
            }
          }

          item_idx++;
        }

        if (Object.keys(row_obj).length > 0)
          this.flattenData.push(row_obj);
      }

      this.dataSource.data = this.flattenData;
    }

    jsonTransform(item) {
        return JSON.stringify(item, null, 4);
    }

    /*
    * function transformers json item into a string of JSONL
     */
    jsonlinesTransform(item) {
      let buildString = "";

      let counter = 0;

      let newLineRE = /\r?\n|\r/g;

      for (let obj of item) {
        buildString += JSON.stringify(obj).replace(newLineRE, "");

        //new line delimeter
        if (counter < item.length - 1)
          buildString += "\n";

        counter++;
      }

      return buildString;
    }

    checkView() {
        if (!this.treeVisible) {
            return true;
        } else {
            return false
        }
    }

    /*
    * Function opens dialog to pick between JSON and JSONL
     */
    openJSONExportPicker() {
      const dialogRef = this.dialog.open(DialogExportPicker, {
        width: '350px',
        data: {exportFormat: this.exportFormat, fileName: this.exportFileName}
      });

      dialogRef.afterClosed().subscribe(result => {
        if (result[0] != 'cancel') {
          this.exportFormat = result[0];
          this.exportFileName = result[1];

          if (this.exportFormat == 'json')
            this.exportToJSON();
          else if (this.exportFormat == 'jsonl')
            this.exportToJSONLines();
          else if (this.exportFormat == 'csv')
            this.exportToCSV();

          if (this.exportFormat == 'jsonl')
            this.exportFormat = 'json';
        }
      });
    }

    /*
    * Function reads row of CSV and returns the list of items
     */
    readCSV(row: string) {
      let items = []
      let escapeMode = false;
      let currentItem = "";

      for (let i = 0; i < row.length; i++) {
        let curr_char = row.charAt(i);

        if (curr_char == '"' && escapeMode)
          //turn off escape mode
          escapeMode = false;
        else if (curr_char == '"' && !escapeMode)
          //turn on escape mode
          escapeMode = true;
        else {
          //if char is a comma and not in escape mode
          if ((curr_char == ',' && !escapeMode) || i == row.length - 1) {
            //push current item and reset for next item
            items.push(currentItem);
            currentItem = "";
          } else {
            currentItem += curr_char;
          }
        }
      }

      return items;
    }
}

@Component({
  selector: 'dialog-export-picker',
  templateUrl: 'dialog-export-picker.html',
  styleUrls:  ['dialog-export-picker.scss']
})
export class DialogExportPicker {

  constructor(
    public dialogRef: MatDialogRef<DialogExportPicker>,
    @Inject(MAT_DIALOG_DATA) public data: DialogData) {}
}

