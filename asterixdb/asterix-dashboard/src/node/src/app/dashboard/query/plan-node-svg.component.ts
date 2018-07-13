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
import { Renderer2, ViewEncapsulation, Component, Input } from '@angular/core';

export interface ViewParams {
    viewMode: string,
    width: string,
    height: string,
    visible: string,
    display: string,
    opacity: number,
    border: string
}

export const FULL:ViewParams = {
    viewMode: 'FULL',
    width: '350px',
    height: '180px',
    visible: 'visible',
    display: 'block',
    opacity: 1,
    border: "2px solid #0000FF"
};

export const NORMAL:ViewParams = {
    viewMode: 'NORMAL',
    width: '200px',
    height: '60px',
    visible: 'hidden',
    display: 'none',
    opacity: 0,
    border: "none"
};

@Component({
    moduleId: module.id,
    selector: 'plan-node-svg',
    templateUrl: 'plan-node-svg.component.html',
    styleUrls: ['plan-node-svg.component.scss'],
    encapsulation: ViewEncapsulation.None,
})

export class PlanNodeSVGComponent {
    @Input() node: any;
    @Input() level;
    @Input() item = 0;
    @Input() subplan = 0;
    @Input() planName = "";
    @Input() viewParams;

    details: any;
    viewParams_: any;

    constructor(private renderer: Renderer2) {}

    numberOfInputs: 0;
    selected = false;

    ngOnInit() {

        this.viewParams_ = NORMAL;

        /* Some preprocessing to show explanation details */
        if (this.node.inputs){
            this.numberOfInputs = this.node.inputs.length;
        } else {
            this.numberOfInputs = 0;
        }

        if (this.node) {
            let node_=  JSON.parse(JSON.stringify(this.node));

            if (node_.inputs) {
                delete node_['inputs'];
            }

            if (node_.subplan) {
                delete node_['subplan'];
            }

            if (node_.visible != undefined ) {
                delete node_['visible'];
            }

            if (node_.viewDetails != undefined) {
                delete node_['viewDetails'];
            }

            if (node_.operator) {
                delete node_['operator'];
            }

            if (node_.operatorId) {
                delete node_['operatorId'];
            }

            this.details = JSON.stringify(node_, null, 8);

            this.details = this.details.replace(/^{/, '');
            this.details = this.details.replace(/^\n/, '');
            this.details = this.details.replace(/}$/, '');
        }
    }

    getNodeName() {
        if(this.node) {
            if (this.node.operator) {
                return (this.node.operator).toUpperCase();
            } else {
                return "NA";
            }
        }
    }

    getNodeOperatorId() {
        if(this.node) {
            if (this.node.operatorId) {
                return (this.node.operatorId).toUpperCase();
            } else {
                return "NA";
            }
        }
    }

    getNodeSubPlan() {
        if(this.node) {
            if (this.node['inputs']) {
                if (this.node['inputs'][this.item]) {
                    if (this.node['inputs'][this.item]['subplan']) {
                        return "Subplan";
                    } else {
                        return "";
                    }
                } else {
                    return "";
                }
            }
        }
    }

    seeDetails(me) {
        // Future Implementation
    }

    checkSubPlan() {
        if(this.node) {
            if (this.node['inputs']) {
                if (this.node['inputs'][this.item]) {
                    if (this.node['inputs'][this.item]['subplan']) {
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }
    }

    checkMerge() {
        if(this.node) {
            if (this.node['mergeWith']) {
                return true;
            } else {
                return false;
            }
        }
    }
}