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
import {Component, Input, SimpleChange, HostListener, ViewChild, ElementRef} from '@angular/core';
import { Subject } from "rxjs";

export  interface planCount {
  nodesCnt: number,
  levelsCnt: number
}

@Component({
  selector: 'plan-viewer',
  templateUrl: 'plan-viewer.component.html',
  styleUrls: ['plan-viewer.component.scss'],
})

export class PlanViewerComponent {

  @Input() planFormat: any;
  @Input() plan: any;
  @Input() planName: any;
  @Input() jsonPlan: any;

  plan_: any;
  jsonVisible = false;
  detailed: boolean = false;
  nodesArr: any[];
  nodeIdsArr: any[] = [];
  ids: any[] = [];
  edgesArr: any[];

  //search variables
  flatJSONGraph: any = {};
  searchRegex: string = "";
  searchMatches: any[] = [];
  matchesFound: boolean = false;
  matchIdx: number = 0;

  //variables for ngx-graph
  zoomToFit$: Subject<boolean> = new Subject();
  center$: Subject<boolean> = new Subject();
  update$: Subject<boolean> = new Subject();
  panToNode$: Subject<any> = new Subject();

  //drop down variables
  planOrientation = "BT";
  selectedNode = "n11";

  previousNodeId: any;
  previouseOccurrenceArrayIdx: number;

  selectedVariableOccurrences: any;
  selectedVariableOccurrencesArray: any[];
  occurrenceArrayIdx: number = 0;

  nodeIdx = 0;
  orientations: any[] = [
    {
      label: "Bottom to Top",
      value: "BT"
    },
    {
      label: "Top to Bottom",
      value: "TB"
    }
    /*
    Left to Right or Right to Left do not look right yet

    {
      label: "Left to Right",
      value: "LR"
    },
    {
      label: "Right to Left",
      value: "RL"
    }
    */
  ];

  colors = [
    "#00ff00",
    "#0000ff",
    "#ffff00",
    "#8b008b",
    "#ffa500",
    "#ee82ee",
    "#ff0000",
    "#9acd32",
    "#20b2aa",
    "#00fa9a",
    "#db7093",
    "#eee8aa",
    "#6495ed",
    "#ff1493",
    "#ffa07a",
    "#2f4f4f",
    "#8b4513",
    "#006400",
    "#808000",
    "#483d8b",
    "#000080"
  ]

  coloredNodes: any = {};

  variablesOccurrences: any = {};
  variablesDeclarations: any = {};
  variables: any[];

  constructor() {}

  ngOnInit() {}

  ngAfterViewInit() {
  }

  ngOnChanges() {
    this.plan_ = this.plan;

    /* If plan format is JSON analyze and augment for visualization */
    if (this.planFormat === 'JSON') {
      //clear previous plans results
      this.nodesArr = [];
      this.edgesArr = [];
      this.nodeIdsArr = [];
      this.ids = [];
      this.selectedNode = 'n11';

      this.previousNodeId = undefined;
      this.previouseOccurrenceArrayIdx = undefined;

      this.selectedVariableOccurrences = undefined;
      this.selectedVariableOccurrencesArray = undefined;
      this.occurrenceArrayIdx = 0;

      this.nodeIdx = 0;

      this.coloredNodes = {};

      this.variablesOccurrences = {};
      this.variablesDeclarations = {};
      this.variables = undefined;

      this.searchRegex = "";
      this.searchMatches = [];
      this.matchesFound = false;
      this.matchIdx = 0;

      let nodesSet = new Set();
      let edgesSet = new Set();

      let recurseResults = this.createLinksEdgesArray(this.plan_, this.nodesArr, this.edgesArr, nodesSet, edgesSet);

      this.nodesArr = recurseResults[0];
      this.edgesArr = recurseResults[1];

      this.variables = Object.keys(this.variablesOccurrences);

      //get declarations from variableOccurrences
      for (let variable of this.variables) {
        //extract first occurrence of variable (last because we parse from bottom->down)
        this.variablesDeclarations[variable] = this.variablesOccurrences[variable][this.variablesOccurrences[variable].length-1];
      }

      this.jsonVisible = false;
    } else {
      this.jsonVisible = true;
    }
  }

  /*
  Function that makes the entire graph to fit the view
   */
  fitGraph() {
    this.zoomToFit$.next(true);
    this.center$.next(true);
  }

  /*
  * Create links array and edges array for NgxGraphModule
   */
  createLinksEdgesArray(plan, nodesArr, edgesArr, nodesSet, edgesSet) {
    let nodes = {};
    nodes = plan;

    let nodeToAdd = {};

    if (nodes) {
      if (!nodesSet.has(nodes['operatorId'])) {
        nodesSet.add(nodes['operatorId']);
        nodeToAdd['id'] = "n" + nodes['operatorId'];
        nodeToAdd['id'] = nodeToAdd['id'].replace(/\./g, '');

        //read variables and expressions from node and add to this.nodeOccurrences
        this.storeVariablesExpressions(nodes, nodeToAdd['id']);

        this.flatJSONGraph[nodeToAdd['id']] = "";

        //logic for label
        nodeToAdd['label'] = nodes['operatorId'] + " : " + nodes['operator'];

        nodeToAdd['detailed_label'] = nodes['operatorId'] + " : " + nodes['operator'];
        nodeToAdd['physical_operator'] = nodes['physical-operator'];
        nodeToAdd['execution_mode'] = "[" + nodes['execution-mode'] + "]"
        nodeToAdd['min_time'] = nodes['min-time'];
        nodeToAdd['max_time'] = nodes['max-time'];
        nodeToAdd["details"] = {};

        nodeToAdd['selected'] = false;
        nodeToAdd['operator'] = nodes['operator'];

        //case for having both expressions and variables
        if (nodes['expressions'] && nodes['variables']) {
          nodeToAdd['detailed_label'] += `${this.variableExpressionStringify(nodes['expressions'])} <- ${this.variableExpressionStringify(nodes['variables'])}`;
        }
        //case for having only expressions
        if (nodes['expressions'] && nodes['variables'] == undefined) {
          nodeToAdd['detailed_label'] += this.variableExpressionStringify(nodes['expressions']);
        }

        //case for having only variables
        if (nodes['variables'] && nodes['expressions'] == undefined) {
          //if data scan, different
          if (nodes['data-source']) {
            nodeToAdd['details']['data-scan'] = `[]<-${this.variableExpressionStringify(nodes['variables'])}<-${nodes['data-source']}`;
          }
          //else
          else
            nodeToAdd['detailed_label'] += `(${this.variableExpressionStringify(nodes['variables'])})`;
        }

        //limit value
        if (nodes['value']) {
          nodeToAdd['detailed_label'] += ` ${nodes['value']}`;
        }

        //group by operator group-by list
        if (nodes['group-by-list'])
          nodeToAdd['details']['group_by_list'] = `group by ([${this.groupByListStringify(nodes['group-by-list'])}])`;

        //group by operator decor-list
        if (nodes['decor-list'])
          nodeToAdd['details']['decor_list'] = `decor ([${this.groupByListStringify(nodes['decor-list'])}])`;

        //join operator condition
        if (nodes['condition']) {
          nodeToAdd['details']['condition'] = `join condition (${nodes['condition']})`;
        }

        let nodeDropDown = {};
        nodeDropDown['label'] = nodeToAdd['label'];
        nodeDropDown['value'] = nodeToAdd['id'];
        this.nodeIdsArr.push(nodeDropDown);

        for (let val of Object.values(nodeToAdd)) {
          this.flatJSONGraph[nodeToAdd['id']] += String(val).toLowerCase() + " ";
        }

        //Dynamic node coloring
        if (nodeToAdd['operator'] in this.coloredNodes) {
          nodeToAdd['color'] = this.coloredNodes[nodeToAdd['operator']];
        } else {
          if (this.colors.length > 1) {
            let nodeColor = this.colors[0];
            this.colors.splice(0, 1);
            nodeToAdd['color'] = nodeColor;

            this.coloredNodes[nodeToAdd['operator']] = nodeColor;
          } else {
            let nodeColor = "#ffffff";
            nodeToAdd['color'] = nodeColor;

            this.coloredNodes[nodeToAdd['operator']] = nodeColor;
          }
        }

        this.ids.push(nodeToAdd['id']);

        nodesArr.push(nodeToAdd);
      }

      if (nodes['inputs']) {
        for (let i = 0; i < nodes['inputs'].length; i++) {
          let edge = nodes['operatorId'].slice() + "to" + nodes['inputs'][i]['operatorId'].slice();
          edge = edge.replace(/\./g, '');

          if (!edgesSet.has(edge)) {
            edgesSet.add(edge);

            //create the edge
            let edgeToAdd = {};

            edgeToAdd['id'] = "e" + edge;
            edgeToAdd['source'] = "n" + nodes['inputs'][i]['operatorId'];
            edgeToAdd['source'] = edgeToAdd['source'].replace(/\./g, '');
            edgeToAdd['target'] = "n" + nodes['operatorId'];
            edgeToAdd['target'] = edgeToAdd['target'].replace(/\./g, '');

            edgesArr.push(Object.assign(edgeToAdd, {}));
          }


          let recurseResult = this.createLinksEdgesArray(nodes['inputs'][i], nodesArr, edgesArr, nodesSet, edgesSet);

          nodesArr = recurseResult[0];
          edgesArr = recurseResult[1];
          nodesSet = recurseResult[2];
          edgesSet = recurseResult[3];

        }
      }
    }

    return [nodesArr, edgesArr, nodesSet, edgesSet];
  }

  /*
  * Extracts variables and expressions and stores occurences in this.variablesOccurrences
   */
  storeVariablesExpressions(node, id) {
    if (node['expressions']) {
      if (node['operator'] == 'assign') {
        for (let expression of node['expressions']) {
          let matches = expression.match(/\$\$.*?(?=,|\])/g)

          if (matches) {
            for (let match of matches) {
              this.addVariableExpression(match, id);
            }
          }
        }
      } else {
        for (let expression of node['expressions']) {
          this.addVariableExpression(expression, id);
        }
      }
    }
    if (node['variables']) {
      for (let variable of node['variables']) {
        this.addVariableExpression(variable, id);
      }
    }
    if (node['group-by-list']) {
      for (let item of node['group-by-list']) {
        this.addVariableExpression(item.variable, id);
        this.addVariableExpression(item.expression, id);
      }
    }
    if (node['decor-list']) {
      for (let item of node['decor-list']) {
        this.addVariableExpression(item.variable, id);
        this.addVariableExpression(item.expression, id);
      }
    }
    if (node['condition'] || node['operator'] == 'exchange') {
      //does this for joins or exchanges (HASH_PARTITION_EXCHANGE contains variables/expressions)
      //regex extracts variables / expressions ($$ match until a ',' ']', or '('
      let matches = node['physical-operator'].match(/\$\$.*?(?=,|\]|\()/g)

      if (matches) {
        for (let match of matches) {
          this.addVariableExpression(match, id);
        }
      }
    }
  }

  /*
  * Helper function that creates a set if var/exp not in this.variableOccurrences, then stores the id of the node
   */
  addVariableExpression(varExp, id) {
    if (!(varExp in this.variablesOccurrences)) {
      this.variablesOccurrences[varExp] = [];
    }
    if (!(this.variablesOccurrences[varExp].includes(varExp)))
      this.variablesOccurrences[varExp].push(id);
  }

  /*
  * Conducts the string match in query plan viewer
   */
  onClickSearch() {
    if (this.searchRegex != "") {
      this.searchMatches = [];

      for (let searchId in this.flatJSONGraph) {
        if (this.flatJSONGraph[searchId].includes(String(this.searchRegex).toLowerCase())) {
          this.searchMatches.push(searchId);
        }
      }

      if (this.searchMatches.length > 0) {
        //matches found
        this.matchesFound = true;
        this.matchIdx = 0;

        let currentIdx = this.ids.indexOf(this.selectedNode);
        let nextIdx = this.ids.indexOf(this.searchMatches[this.matchIdx]);

        this.nodesArr[currentIdx]['selected'] = false;
        this.nodesArr[nextIdx]['selected'] = true;

        this.panToNode(this.ids[nextIdx]);
        this.nodeIdx = nextIdx;
      }
    }
  }

  /*
  * Function that resets all the navigation variables
   */
  clearSelections() {
    //clear main selection variables
    this.nodeIdx = 0;
    this.selectedNode = "n11";

    //clear variable occurrences variables
    this.selectedVariableOccurrences = undefined;
    this.selectedVariableOccurrencesArray = [];
    this.occurrenceArrayIdx = 0;

    //clear search variables
    this.searchRegex = "";
    this.searchMatches = [];
    this.matchesFound = false;
    this.matchIdx = 0;

    for (let node of this.nodesArr) {
      node['selected'] = false;
    }

    this.panToNode(this.selectedNode);
  }

  /*
  * Select variable from variable occurrences drop down
   */
  setSelectedVariableOccurrences(variable) {
    this.selectedVariableOccurrences = variable;

    //set the array
    this.selectedVariableOccurrencesArray = this.variablesOccurrences[this.selectedVariableOccurrences];

    //set the selected node to the first in the array
    this.panToNode(this.selectedVariableOccurrencesArray[0]);
    this.occurrenceArrayIdx = 0;
  }

  /*
  * Jumps to selected variable's declaration (first occurrence in a DFS)
   */
  jumpToDeclaration() {
    this.previousNodeId = this.selectedNode;
    this.previouseOccurrenceArrayIdx = this.occurrenceArrayIdx;

    this.panToNode(this.variablesDeclarations[this.selectedVariableOccurrences])

    this.occurrenceArrayIdx = this.selectedVariableOccurrencesArray.length - 1;
  }

  /*
  * Jump back to previous node after a call to jumpToDeclaration()
   */
  jumpBack() {
    this.panToNode(this.previousNodeId);

    this.occurrenceArrayIdx = this.previouseOccurrenceArrayIdx;

    this.previousNodeId = undefined;
    this.previouseOccurrenceArrayIdx = undefined;
  }

  /*
  * Sets the orientation of the graph (top to bottom, left to right, etc.)
   */
  setOrientation(orientation: string): void {
    this.planOrientation = orientation;
    this.update$.next(true);
  }

  setDetail(checked: boolean) {
    this.detailed = checked;
    this.update$.next(true);
  }

  /*
  * Pans to node in the graph. Jumps to specified node and updates selection variables
   */
  panToNode(id: any) {
    this.selectedNode = id;

    this.nodesArr[this.nodeIdx]['selected'] = false;

    let nodeIdx = this.nodesArr.map(function(e) { return e.id}).indexOf(id);
    this.nodeIdx = nodeIdx;
    this.nodesArr[nodeIdx]['selected'] = true;

    this.panToNode$.next(id);
    this.update$.next(true);
  }

  /*
  * Increments current node in graph (going "down" the graph in a DFS)
   */
  incrementNode() {
    let currentIdx = this.ids.indexOf(this.selectedNode);
    if (currentIdx + 1 < this.nodesArr.length) {
      this.nodesArr[currentIdx]['selected'] = false;
      this.nodesArr[currentIdx + 1]['selected'] = true;
      this.panToNode(this.ids[currentIdx + 1]);
      this.nodeIdx = currentIdx + 1;
    }
  }

  /*
  * Decrements current node in graph (going "up" the graph in a DFS)
   */
  decrementNode() {
    let currentIdx = this.ids.indexOf(this.selectedNode);

    if (currentIdx - 1 >= 0) {
      this.nodesArr[currentIdx]['selected'] = false;
      this.nodesArr[currentIdx-1]['selected'] = true;
      this.panToNode(this.ids[currentIdx - 1]);
      this.nodeIdx = currentIdx - 1;
    }
  }

  /*
  * Increments current node but in occurrence (Jumping to the next occurrence of the selected variable)
   */
  incrementOccurrence() {
    let currentIdx = this.ids.indexOf(this.selectedNode);
    if (this.occurrenceArrayIdx + 1 < this.selectedVariableOccurrencesArray.length) {
      let nextIdx = this.ids.indexOf(this.selectedVariableOccurrencesArray[this.occurrenceArrayIdx + 1]);

      this.nodesArr[currentIdx]['selected'] = false;
      this.nodesArr[nextIdx]['selected'] = true;

      this.panToNode(this.ids[nextIdx]);
      this.nodeIdx = nextIdx;
      this.occurrenceArrayIdx += 1;
    } else {
      //wrap around to first item
      let nextIdx = this.ids.indexOf(this.selectedVariableOccurrencesArray[0]);

      this.nodesArr[currentIdx]['selected'] = false;
      this.nodesArr[nextIdx]['selected'] = true;

      this.panToNode(this.ids[nextIdx]);
      this.nodeIdx = nextIdx;
      this.occurrenceArrayIdx = 0;
    }
  }

  /*
  * Decrements current node but in occurrence (Jumping to the previous occurrence of the selected variable)
   */
  decrementOccurrence() {
    let currentIdx = this.ids.indexOf(this.selectedNode);
    if (this.occurrenceArrayIdx - 1 >= 0) {
      let nextIdx = this.ids.indexOf(this.selectedVariableOccurrencesArray[this.occurrenceArrayIdx - 1]);

      this.nodesArr[currentIdx]['selected'] = false;
      this.nodesArr[nextIdx]['selected'] = true;

      this.panToNode(this.ids[nextIdx]);
      this.nodeIdx = nextIdx;
      this.occurrenceArrayIdx -= 1;
    } else {
      //wrap around to last item
      let nextIdx = this.ids.indexOf(this.selectedVariableOccurrencesArray[this.selectedVariableOccurrencesArray.length - 1]);

      this.nodesArr[currentIdx]['selected'] = false;
      this.nodesArr[nextIdx]['selected'] = true;

      this.panToNode(this.ids[nextIdx]);
      this.nodeIdx = nextIdx;
      this.occurrenceArrayIdx = this.selectedVariableOccurrencesArray.length - 1;
    }
  }

  /*
  * Increments current node but in search match (Jumping to the next occurrence of the search results)
   */
  incrementMatch() {
    let currentIdx = this.ids.indexOf(this.selectedNode);
    if (this.matchIdx + 1 < this.searchMatches.length) {
      let nextIdx = this.ids.indexOf(this.searchMatches[this.matchIdx + 1]);

      this.nodesArr[currentIdx]['selected'] = false;
      this.nodesArr[nextIdx]['selected'] = true;

      this.panToNode(this.ids[nextIdx]);
      this.nodeIdx = nextIdx;
      this.matchIdx += 1;
    } else {
      //wrap around to first item
      let nextIdx = this.ids.indexOf(this.searchMatches[0]);

      this.nodesArr[currentIdx]['selected'] = false;
      this.nodesArr[nextIdx]['selected'] = true;

      this.panToNode(this.ids[nextIdx]);
      this.nodeIdx = nextIdx;
      this.matchIdx = 0;
    }
  }

  /*
  * Decrements current node but in search match (Jumping to the previous occurrence of the search results)
   */
  decrementMatch() {
    let currentIdx = this.ids.indexOf(this.selectedNode);
    if (this.matchIdx - 1 >= 0) {
      let nextIdx = this.ids.indexOf(this.searchMatches[this.matchIdx - 1]);

      this.nodesArr[currentIdx]['selected'] = false;
      this.nodesArr[nextIdx]['selected'] = true;

      this.panToNode(this.ids[nextIdx]);
      this.nodeIdx = nextIdx;
      this.matchIdx -= 1;
    } else {
      //wrap around to last item
      let nextIdx = this.ids.indexOf(this.searchMatches[this.searchMatches.length - 1]);

      this.nodesArr[currentIdx]['selected'] = false;
      this.nodesArr[nextIdx]['selected'] = true;

      this.panToNode(this.ids[nextIdx]);
      this.nodeIdx = nextIdx;
      this.matchIdx = this.searchMatches.length - 1;
    }
  }

  /*
  * Function takes in array of objects and stringifies
   */
  groupByListStringify(variables: any[]) {
    let buildString = "";

    let listSize = variables.length;
    let counter = 0;

    for (let variable of variables) {
      if (counter < listSize - 1) {
        buildString += variable['variable'] + " := " + variable['expression'] + "; ";
      } else {
        buildString += variable['variable'] + " := " + variable['expression'];
      }

      counter++;
    }

    return buildString;
  }

  /*
  * Function that stringifys variables / objects array
   */
  variableExpressionStringify(arr: any[]) {
    if (arr.length == 1) {
      return "[" + arr[0] + "]";
    } else {
      return "[" + arr.toString() + "]";
    }
  }
}


