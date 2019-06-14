import { Component, Input, SimpleChange } from '@angular/core';

export  interface planCount {
    nodesCnt: number,
    levelsCnt: number
}

@Component({
    selector: 'plan-view',
    templateUrl: 'plan-view.component.html',
    styleUrls: ['plan-view.component.scss'],
})

export class PlanViewComponent {

    @Input() planFormat: any;
    @Input() plan: any;
    @Input() planName: any;
    @Input() jsonPlan: any;

    plan_: any;
    numberOfLevels: number = 0;
    numberOfNodes: number = 0;
    jsonVisible = false;
    jsonButtonDisabled = false;

    constructor() {}

    ngOnInit() {}

    ngOnChanges() {
        this.plan_ = this.plan;
        /* If plan format is JSON analyze and augment for visualization */
        if (this.planFormat === 'JSON') {
            let summary : planCount = {nodesCnt:0, levelsCnt:0}
            summary = this.analyzePlan(this.plan_, summary);
            this.numberOfLevels = summary.levelsCnt;
            this.numberOfNodes = summary.nodesCnt;
            this.jsonVisible = false;
            this.jsonButtonDisabled = false;
        } else {
            this.jsonVisible = true;
            this.jsonButtonDisabled = true;
        }
	}

    /*
    * See the JSON contents inside of each node
    */
    showJSON() {
        this.jsonVisible = !this.jsonVisible;
    }

    /*
    * Check the merge paths, from operation ID
    */
    operation = [];
    checkOperationId(operationId, levelsCnt){
        console.log('LEVEL:' + levelsCnt + 'OP' + operationId)
       // console.log(this.operation)
        if (this.operation.length > 0) {
            for (let i = 0; i < this.operation.length; i++) {
                if (this.operation[i] === operationId) {
                    console.log('found')
                    console.log('BREAK')
                    this.operation = [];
                    return true;
                }
            }
        }
        this.operation.push(operationId);
        console.log('not found')
        return false;
    }

    /*
    * Counts the number of nodes/operations in the tree
    */
    analyzePlan(plan, planCounter) {
        planCounter.nodesCnt += 1;
        planCounter.levelsCnt += 1;
        let nodes = {}
        nodes = plan;
        // augment
        if (nodes) {
            nodes['visible'] = true;
            nodes['viewDetails'] = false;
            if (nodes['inputs']) {
                for (let i = 0; i< nodes['inputs'].length; i++)
                {
                    planCounter = this.analyzePlan(nodes['inputs'][i], planCounter);
                }
            }
        }
        return planCounter;
    }

    /*
    * See the JSON contents inside of each node, with pre-format
    * Not used in this version
    */
    toggleViewDetails(plan) {
        let nodes = {}
        nodes = plan;
        // augment
        nodes['visible'] = true;
        nodes['viewDetails'] = !nodes['viewDetails'];
        if (nodes['inputs']) {
            for (let i = 0; i< nodes['inputs'].length; i++)
            {
                this.toggleViewDetails(nodes['inputs'][i]);
            }
        }
    }
}