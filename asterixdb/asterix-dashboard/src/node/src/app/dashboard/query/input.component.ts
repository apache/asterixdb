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
import { Component, ViewChild } from '@angular/core';
import { Observable } from 'rxjs';
import { Store } from '@ngrx/store';
import * as sqlQueryActions from '../../shared/actions/query.actions';
import * as appActions from '../../shared/actions/app.actions'
import * as dataverseActions from '../../shared/actions/dataverse.actions'
import * as CodeMirror from 'codemirror';
/*
 * Query component
 * has editor (codemirror)
 */
@Component({
    moduleId: module.id,
    selector: 'awc-query',
    templateUrl: 'input.component.html',
    styleUrls: ['input.component.scss']
})

export class InputQueryComponent {
    currentQuery = 0;
    queryString: string = "";
    metricsString: {};
    queryErrorMessageString: string = "";
    collapse = false;
    input_expanded_icon = 'expand_less';
    queryRequest: any;
    queryPrepare: any;
    queryMetrics$: Observable <any> ;
    queryMetrics: {};
    querySuccess$: Observable <any> ;
    querySuccess: Boolean = false;
    queryError$: Observable <any> ;
    queryError: Boolean = false;
    queryErrorMessage$: Observable <any> ;
    queryErrorMessages: {};
    queryPrepared$: Observable <any> ;
    queryPrepared: {};
    queryPlanFormats$: Observable <any> ;
    queryPlanFormats: {};
    preparedQueryCount: number;
    previousDisabled = true;
    nextDisabled = true;
    querySuccesResults: any;
    querySpinnerVisible: boolean = false;
    dataverses$: Observable<any>;
    dataverses: any;
    defaultDataverse = 'Default';
    selected = 'None';
    history = [];
    currentHistory = 0;
    viewCurrentHistory = 0; // for the view
    sideMenuVisible$: Observable<any>;
    sideMenuVisible: any;
    none = 'None';
    planFormat = 'JSON';
    historyStringSelected = '';
    formatOptions = 'JSON';
    /* Codemirror configuration */
    codemirrorConfig = {
    mode: "asterix",
    lineWrapping: true,
    showCursorWhenSelecting: true,
    autofocus: true,
    lineNumbers: true,
    };

    constructor(private store: Store < any > ) {
        this.currentQuery = 0;
        this.querySuccess$ = this.store.select(s => s.sqlQuery.successHash);
        this.querySuccess$.subscribe((data: any) => {
            this.querySuccesResults = data;
            this.querySpinnerVisible = false;
            if (data != undefined && data[this.currentQuery] === true) {
                this.querySuccess = true;
            } else {
                this.querySuccess = false;
            }
        })

        /* Watching for SQL Input Errors in current Query */
        this.queryError$ = this.store.select(s => s.sqlQuery.errorHash);
        this.queryError$.subscribe((data: any) => {
            this.querySpinnerVisible = false;
            if (data != undefined && data[this.currentQuery] === true) {
                this.queryError = true;
                this.showErrors();
            } else {
                this.queryError = false;
            }
        })

        /* Watching for Queries that are in prepared state,
        * those are SQL strings that still has not been executed
        */
        this.queryPrepared$ = this.store.select(s => s.sqlQuery.sqlQueryPrepared);
        this.queryPrepared$.subscribe((data: any) => {
            if (data) {
                this.queryPrepared = data
                this.preparedQueryCount = Object.keys(this.queryPrepared).length;
                if (this.preparedQueryCount == 0) {
                    // Initialize Query Editor, prepare the default query
                    this.queryPrepare = {
                        editorId: String(this.currentQuery),
                        queryString: this.queryString,
                        planFormat: this.planFormat
                    };
                    this.store.dispatch(new sqlQueryActions.PrepareQuery(this.queryPrepare));
                } else {
                    if (this.queryPrepared && this.queryPrepared[this.currentQuery]) {
                        this.queryString = this.queryPrepared[this.currentQuery];
                    }
                }
            } else {
                this.queryPrepared = {};
            }
        })

        /* Watching for Metrics */
        this.queryMetrics$ = this.store.select(s => s.sqlQuery.sqlQueryMetrics);
        this.queryMetrics$.subscribe((data: any) => {
            if (data != undefined) {
                this.queryMetrics = Object.assign(data);
                if (this.queryMetrics && this.queryMetrics[this.currentQuery]) {
                    this.metricsString = "SUCCESS: ";
                    this.metricsString += " Execution time: " + this.queryMetrics[this.currentQuery].executionTime;
                    this.metricsString += " Elapsed time: " + this.queryMetrics[this.currentQuery].elapsedTime;
                    this.metricsString += " Size: " + (this.queryMetrics[this.currentQuery].resultSize/1024).toFixed(2) + ' Kb';
                }
            } else {
                this.queryMetrics = {};
            }
        })

        /* Watching for SQL Input Errors: Error Message stored in Query Cache */
        this.queryErrorMessage$ = this.store.select(s => s.sqlQuery.sqlQueryErrorHash);
        this.queryErrorMessage$.subscribe((data: any) => {
            if (data) {
                this.queryErrorMessages = data;
                this.showErrors();
            } else {
                this.queryErrorMessages = {};
            }
        })

        /* Watching for SQL Input Errors: Error Message stored in Query Cache */
        this.queryPlanFormats$ = this.store.select(s => s.sqlQuery.sqlQueryPlanFormatHash);
        this.queryPlanFormats$.subscribe((data: any) => {
            if (data) {
                this.queryPlanFormats = data;
            } else {
                this.queryPlanFormats = {};
            }
        })

        this.preparedQueryCount = 0;
        // Initialize Query Editor, prepare the default query
        this.queryPrepare = {
            editorId: String(this.currentQuery),
            queryString: this.queryString,
            planFormat: this.formatOptions
        };
        this.store.dispatch(new sqlQueryActions.PrepareQuery(this.queryPrepare));
        // lets inform other views what's the current SQL editor
        this.store.dispatch(new appActions.setEditorIndex(String(this.currentQuery)));
    }

    ngOnInit() {
        this.dataverses$ = this.store.select(s => s.dataverse.dataverses.results);
        // Watching for Dataverses
        this.dataverses$ = this.store.select(s => s.dataverse.dataverses.results);
        this.dataverses$.subscribe((data: any[]) => {
            this.dataverses = data;
            this.defaultDataverse = ''
        });
        this.store.dispatch(new dataverseActions.SelectDataverses('-'), );
    }

    showMetrics() {
        this.querySuccess = false;
        if (this.queryMetrics && this.queryMetrics[this.currentQuery] && this.querySuccesResults[this.currentQuery]) {
            this.metricsString = "SUCCESS: ";
            this.metricsString += " Execution time: " + this.queryMetrics[this.currentQuery].executionTime;
            this.metricsString += " Elapsed time: " + this.queryMetrics[this.currentQuery].elapsedTime;
            this.metricsString += " Size: " + (this.queryMetrics[this.currentQuery].resultSize/1024).toFixed(2) + ' Kb';
            this.querySuccess = true;
        }
    }

    showErrors() {
        this.queryError = false;
        if (this.queryErrorMessages && this.queryErrorMessages[this.currentQuery]) {
            let errorObject = this.queryErrorMessages[this.currentQuery];
            if (errorObject.length != 0) {
            this.queryErrorMessageString = "ERROR: Code: " + JSON.stringify(errorObject[0].code, null, 8);
            this.queryErrorMessageString += " " + JSON.stringify(errorObject[0].msg, null, 8);
            this.queryError = true;
            }
        }
    }

    getQueryResults(queryString: string, planFormat: string) {
        let QueryOrder = this.currentQuery;
        this.queryRequest = {
            requestId: String(QueryOrder),
            queryString: queryString,
            planFormat: planFormat
        };
        this.store.dispatch(new sqlQueryActions.ExecuteQuery(this.queryRequest));
        this.querySpinnerVisible = true;
    }

    onClickRun() {
        let planFormat = this.formatOptions;
        this.getQueryResults(this.queryString, planFormat); // .replace(/\n/g, " "));
        if (this.history.length === 0) {
            this.history.push('Clear');
        }
        this.history.push(this.queryString);
        this.currentHistory = this.history.length - 1;
        this.viewCurrentHistory = this.history.length;
    }

    onClickNew() {
        // Saving first
        this.queryPrepare = {
            editorId: String(this.currentQuery),
            queryString: this.queryString,
            planFormat: this.formatOptions
        };
        this.store.dispatch(new sqlQueryActions.PrepareQuery(this.queryPrepare));
        // Prepare a new Query String, cleanup screen messages
        this.currentQuery = Object.keys(this.queryPrepared).length;
        this.queryString = "";
        this.editor.getDoc().setValue(this.queryString);
        this.queryErrorMessageString = "";
        this.metricsString = "";
        this.querySuccess = false;
        this.queryError = false;
        this.queryPrepare = {
            editorId: String(this.currentQuery),
            queryString: "",
            planFormat: this.formatOptions
        };
        this.store.dispatch(new sqlQueryActions.PrepareQuery(this.queryPrepare));
        // lets inform other views what's the current SQL editor
        let currentQueryIndex = String(this.currentQuery);
        this.store.dispatch(new appActions.setEditorIndex(currentQueryIndex));
        this.selected = "None";
        this.editor.focus();
    }

    onClickClear() {
        let queryClear = {
            editorId: String(this.currentQuery),
            queryString: "",
            planFormat: "JSON"
        };
        this.store.dispatch(new sqlQueryActions.CleanQuery(queryClear));
        this.queryErrorMessageString = "";
        this.queryString = "";
        this.metricsString = "";
        this.dataverseSelected();
        this.editor.getDoc().setValue(this.queryString);
        this.editor.focus();
    }

    onClickPrevious() {
        if (this.currentQuery > 0) {
            this.nextSQLEditor(-1);
        }
    }

    onClickNext() {
        if (this.currentQuery < this.preparedQueryCount - 1) {
            this.nextSQLEditor(1);
        }
    }

    checkNext() {
        if (this.currentQuery == this.preparedQueryCount - 1) {
          return true;
        } else {
          return false;
        }
    }

    checkPrevious() {
        if (this.currentQuery == 0) {
            return true;
        } else {
            return false;
        }
    }

    nextSQLEditor(next) {
        // Saving First
        this.queryPrepare = {
            editorId: String(this.currentQuery),
            queryString: this.queryString,
            planFormat: this.formatOptions
        };
        this.store.dispatch(new sqlQueryActions.PrepareQuery(this.queryPrepare));
        this.currentQuery = this.currentQuery + next;
        this.queryErrorMessageString = "";
        this.metricsString = "";

        // Retrieve Metrics or Error Message if Query was executed
        this.showMetrics();

        // Retrieve Metrics or Error Message if Query was executed
        this.showErrors();

        // Retrieve the prepared SQL string
        this.queryString = this.queryPrepared[this.currentQuery];
        this.editor.getDoc().setValue(this.queryString);

        // Retrieve the prepared SQL plan Format
        this.formatOptions = this.queryPlanFormats[this.currentQuery];

        // lets inform other views what's the current SQL editor
        let currentQueryIndex = String(this.currentQuery);

        // Inform the app we are now active in next editor
        this.store.dispatch(new appActions.setEditorIndex(currentQueryIndex));
    }

    onClickInputCardCollapse() {
        this.collapse = !this.collapse;
        if (this.collapse) {
            this.input_expanded_icon = 'expand_more';
        } else {
            this.input_expanded_icon = 'expand_less';
        }
    }

    /**
     * On component view init
     */
    @ViewChild('editor') editor: CodeMirror.Editor;
    ngAfterViewInit() {
        this.codemirrorInit(this.codemirrorConfig);
    }

    /**
     * Initialize codemirror
     */
    codemirrorInit(config) {
        this.editor = CodeMirror.fromTextArea(this.editor.nativeElement, config);
        this.editor.setSize(null, 'auto');
        this.editor.getDoc().setValue(this.queryString);
        this.editor.on('changes', () => {
            this.queryString = this.editor.getValue();
        });
    }

    dataverseSelected() {
        if (this.selected == undefined) {
            this.queryString = 'None';
        } else if (this.selected === 'None') {
            this.queryString = '';
            this.selected = 'None';
        } else {
            this.queryString = 'Use ' + this.selected + '; ';
        }
        this.editor.getDoc().setValue(this.queryString);
        this.editor.focus();
    }

    historySelected() {
        if (this.historyStringSelected == undefined) {
            this.historyStringSelected = '';
        } else if (this.historyStringSelected === 'Clear') {
            this.history = [];
            this.historyStringSelected = '';
        }
        this.queryString = this.historyStringSelected;
        this.editor.getDoc().setValue(this.queryString);
        this.editor.focus();
    }

    planFormatSelected() {}

    onClickNextHistory() {
        if (this.currentHistory < this.history.length - 1) {
          this.currentHistory++;
          this.viewCurrentHistory++;
          this.queryString = this.history[this.currentHistory];
          this.editor.getDoc().setValue(this.queryString);
          this.editor.focus();
        }
    }

    onClickPrevHistory() {
        if (this.currentHistory > 0) {
            this.currentHistory--;
            this.viewCurrentHistory--;
            this.queryString = this.history[this.currentHistory];
            this.editor.getDoc().setValue(this.queryString);
            this.editor.focus();
        }
    }
}