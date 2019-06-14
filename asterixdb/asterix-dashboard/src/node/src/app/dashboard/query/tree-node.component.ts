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
import { Component, Input, Output, OnInit, OnChanges, EventEmitter } from '@angular/core';

@Component({
    moduleId: module.id,
    selector: 'tree-node',
    templateUrl: 'tree-node.component.html',
    styleUrls: ['tree-node.component.scss'],
})

export class TreeNodeComponent {

    @Input() node: any;
    @Output() jsonPath = new EventEmitter();

    node_: any;
    final = true;
    visible = true;
    nestedVisible = false;
    nodeContentKeys: any;
    nodeChildren: any;
    jsonPathChild_: any;

    constructor() { this.final = true; }

    initData() {
        this.node_ = this.node;
       // this.nodeChildren = this.node.children;
    }

    changeJsonPathValue(event) {
        this.jsonPathChild_ = event.link;
        this.jsonPath.emit(event);
    }

    ngOnChanges() {
       this.initData();
    }

    ngOnInit() {
       this.initData();
    }

    toggle(node){
        node.visible = !node.visible;
    }

    toggleNested(item){
        item.visible = !item.visible;
    }

    checkVisible(item) {
        return (item.visible);
    }

    nodeCheckType(node) {
        return node.type;
    }

    childrenCount(children) {
        return children.length;
    }

    jsonPathSelect(item, index){
        var el = document.getElementById('item'+ item.item + item.key + item.value + index);
        el.style.color = 'blue';
        var itemLink = ' [ ' + item.item + ' ] ' + ': ' + item.link;
        this.jsonPath.emit({ link: itemLink });
    }

    jsonPathDeselect(item, index) {
        var el = document.getElementById('item'+ item.item + item.key + item.value + index);
        el.style.color = "black";
    }

    actionIcon(item) {
        if(item.visible === true) {
            return '-';
        }
        else {
            return '+';
        }
    }

    checkRoot(item) {
        if(item.level === 0) {
            return true;
        }
        else {
            return false;
        }
    }
}