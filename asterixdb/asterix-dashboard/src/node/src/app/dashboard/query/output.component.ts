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
import { Component } from '@angular/core';
import { Observable } from 'rxjs';
import { Store } from '@ngrx/store';

@Component({
    moduleId: module.id,
    selector: 'awc-results',
    templateUrl: 'output.component.html',
    styleUrls: ['output.component.scss']
})


export class QueryOutputComponent {
    data: any[];
    currentQueryActive$: Observable < any > ;
    currentQueryActive: string;
    queryLogicalPlan = "";
    queryOptimizedLogicalPlan = "";
    logicalPlan: any;
    optimalLogicalPlan: any;
    results$: Observable < any > ;
    planFormat$: Observable < any > ;
    planFormat: any;
    SQLresults: any;
    queryId: any = "";
    observedPlanFormat = "";


    constructor(private store: Store <any>) {
        let key = '1';
        this.currentQueryActive$ = this.store.select(s => s.app.currentQueryIndex);
        /* this when the editor changes */
        this.currentQueryActive$.subscribe((data: any) => {
            if (data) {
                this.currentQueryActive = data;
                if (this.SQLresults) {
                  this.currentQueryActive = data;
                  this.resultsProccess(this.SQLresults, this.currentQueryActive);
                }
            } else {
                this.currentQueryActive = "0";
            }
        })
        /* this is the output when the quey runs for the first time */
        this.results$ = this.store.select(s => s.sqlQuery.sqlQueryResultHash);
        this.results$.subscribe((data: any) => {
            if (Object.keys(data).length !== 0 && data[this.currentQueryActive]) {
                this.resultsProccess(data, this.currentQueryActive);
            } else if (Object.keys(data).length === 0) {
                this.resultsProccess([], this.currentQueryActive);
            }
        })
    }

    resultsProccess(data: any, queryId) {
        this.SQLresults = data;
        this.queryLogicalPlan = "";
        this.queryOptimizedLogicalPlan = "";
        this.planFormat = "JSON";
        if (this.SQLresults[queryId]) {
            // Extract the logical plan
            if (this.SQLresults[queryId]['plans']) {
                if (this.SQLresults[queryId]['plans']['logicalPlan']) {
                    this.queryLogicalPlan = JSON.stringify(this.SQLresults[queryId]['plans']['logicalPlan'], null, 8);
                    this.logicalPlan = this.SQLresults[queryId]['plans']['logicalPlan'];
                    this.planFormat = this.SQLresults[queryId]['planFormat'];
                    if (this.planFormat === 'JSON') {
                        this.queryLogicalPlan = JSON.stringify(this.SQLresults[queryId]['plans']['logicalPlan'], null, 8);
                    } else {
                        this.queryLogicalPlan = this.SQLresults[queryId]['plans']['logicalPlan'];
                    }
                }
                if (this.SQLresults[queryId]['plans']['optimizedLogicalPlan']) {
                    this.optimalLogicalPlan = this.SQLresults[queryId]['plans']['optimizedLogicalPlan'];
                    this.planFormat = this.SQLresults[queryId]['planFormat'];
                    if (this.planFormat === 'JSON') {
                        this.queryOptimizedLogicalPlan = JSON.stringify(this.SQLresults[queryId]['plans']['optimizedLogicalPlan'], null, 8);
                    } else {
                        this.queryOptimizedLogicalPlan = this.SQLresults[queryId]['plans']['optimizedLogicalPlan'];
                    }
                }
            }

            if (this.SQLresults[queryId]['results'] && this.SQLresults[queryId]['results'].length > 0) {
                this.data = this.SQLresults[queryId];
                this.queryId = queryId;
            } else {
                this.data = [];
            }
        } else {
              this.data = [];
        }
    }
}