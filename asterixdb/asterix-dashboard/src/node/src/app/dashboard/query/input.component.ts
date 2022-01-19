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
  import {Component, EventEmitter, Output, ViewChild} from '@angular/core';
  import { Observable } from 'rxjs';
  import { Store } from '@ngrx/store';
  import * as sqlQueryActions from '../../shared/actions/query.actions';
  import * as sqlCancelActions from '../../shared/actions/cancel.actions';
  import * as appActions from '../../shared/actions/app.actions'
  import * as dataverseActions from '../../shared/actions/dataverse.actions'
  import * as CodeMirror from 'codemirror';
  import 'codemirror/addon/edit/closebrackets';
  import 'codemirror/mode/sql/sql';
  import 'codemirror/addon/hint/show-hint';
  import 'codemirror/addon/hint/sql-hint';

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
    @Output() inputToOutputEmitter: EventEmitter<Object> = new EventEmitter<Object>();
    @Output() isErrorEmitter: EventEmitter<Boolean> = new EventEmitter<Boolean>();
    @Output() hideOutputEmitter: EventEmitter<Boolean> = new EventEmitter<Boolean>();

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
    querySuccessWarnings: Boolean = false;
    queryWarnings: any;
    queryWarnings$: Observable<any> ;
    queryWarningsMessages: string[] = [];
    queryWarningsCount = 0;
    queryWarningsShow: Boolean = false;
    queryError$: Observable <any> ;
    queryError: Boolean = false;
    queryErrorMessage$: Observable <any> ;
    queryErrorMessages: {};
    queryPrepared$: Observable <any> ;
    queryPrepared: {};
    queryPlanFormats$: Observable <any> ;
    queryPlanFormats: {};
    cancelQuery$: Observable<any>;
    isCanceled: boolean = false;
    preparedQueryCount: number;
    previousDisabled = true;
    nextDisabled = true;
    querySuccesResults: any;
    querySpinnerVisible: boolean = false;
    dataverses$: Observable<any>;
    dataverses: any;
    defaultDataverse = 'Default';
    selected = 'Default';
    history = [];
    currentHistory = 0;
    viewCurrentHistory = 0; // for the view
    sideMenuVisible$: Observable<any>;
    sideMenuVisible: any;
    none = 'None';
    planFormat = 'JSON';
    historyStringSelected = '';
    historyIdxSelected = 0;
    formatOptions = 'JSON';
    outputOptions = 'JSON';
    explainMode = false;
    inputToOutput = {};
    objectsReturned = 0;
    /* Codemirror configuration */
    //sql++ keywords
    sqlppKeywords = "alter and as asc between by count create delete desc distinct drop from group having in insert into " +
      "is join like not on or order select set union update values where limit use let dataverse dataset exists with index type" +
      "inner outer offset value type if exists declare function explain";

    //sql++ builtin types
    sqlppTypes = "boolean tinyint smallint integer int bigint string float double binary point line rectangle circle polygon" +
      "uuid object array multiset date time datetime duration year_month_duration day_time_duration interval"

    constructor(private store: Store < any > ) {
      this.currentQuery = 0;
      this.querySuccess$ = this.store.select(s => s.sqlQuery.successHash);
      this.querySuccess$.subscribe((data: any) => {
        this.isCanceled = false;
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
          this.queryPrepared = data;
          this.preparedQueryCount = Object.keys(this.queryPrepared).length;

          if (this.queryPrepared && this.queryPrepared[this.currentQuery]) {
            this.queryString = this.queryPrepared[this.currentQuery];
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
            this.objectsReturned = this.queryMetrics[this.currentQuery].resultCount;
            this.isErrorEmitter.emit(false);
            this.hideOutputEmitter.emit(false);
            this.metricsString = "SUCCESS: ";

            if ('warningCount' in this.queryMetrics[this.currentQuery]) {
                this.metricsString += " (WITH " + this.queryMetrics[this.currentQuery].warningCount + " WARNINGS)";
            }

            this.metricsString += " Execution time: " + this.queryMetrics[this.currentQuery].executionTime;
            this.metricsString += " Elapsed time: " + this.queryMetrics[this.currentQuery].elapsedTime;
            this.metricsString += " Size: " + (this.queryMetrics[this.currentQuery].resultSize/1024).toFixed(2) + ' Kb';
          }
        } else {
          this.queryMetrics = {};
          this.objectsReturned = 0;
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

      /* Watching for SQL Input Warnings in current Query*/
      this.queryWarnings$ = this.store.select(s => s.sqlQuery.sqlQueryWarnings);
      this.queryWarnings$.subscribe((data: any) => {
        if (data) {
          this.queryWarnings = data;
          this.showWarnings();
        } else {
          this.queryWarnings = {};
        }
      })

      /*
      * Watching for SQL Cancel Query
       */
      this.cancelQuery$ = this.store.select(s => s.cancelQuery.success);
      this.cancelQuery$.subscribe((data: any) => {
        if (data) {
          this.querySpinnerVisible = false;
          this.isCanceled = true;
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
      this.saveQuery(String(this.currentQuery), this.queryString, this.formatOptions, "JSON");

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
        this.objectsReturned = this.queryMetrics[this.currentQuery].resultCount;
        this.metricsString = "SUCCESS: ";

        if ('warningCount' in this.queryMetrics[this.currentQuery]) {
            this.metricsString += " [WITH " + this.queryMetrics[this.currentQuery].warningCount + " WARNING(S)]";
        }

        this.metricsString += " Execution time: " + this.queryMetrics[this.currentQuery].executionTime;
        this.metricsString += " Elapsed time: " + this.queryMetrics[this.currentQuery].elapsedTime;
        this.metricsString += " Size: " + (this.queryMetrics[this.currentQuery].resultSize/1024).toFixed(2) + ' Kb';

        this.querySuccess = true;
      }
    }

    showWarnings() {
      this.queryWarningsShow = false;
      if (this.queryWarnings && this.queryWarnings[this.currentQuery]) {
        let warningObject = this.queryWarnings[this.currentQuery];

        this.queryWarningsMessages = [];
        this.queryWarningsCount = warningObject.length;
        if (warningObject.length != 0) {
          for (let warning of warningObject.reverse()) {
              let warningString = "WARNING: Code: " + JSON.stringify(warning.code, null, 8);
              warningString += " " + JSON.stringify(warning.msg, null, 8);

              this.queryWarningsMessages.push(warningString);
          }

          this.queryWarningsShow = true;
        }
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

          this.isErrorEmitter.emit(true);
        }
      }
    }

    getQueryResults(queryString: string, planFormat: string, outputFormat: string) {
      let QueryOrder = this.currentQuery;
      this.queryRequest = {
        requestId: String(QueryOrder),
        queryString: queryString,
        planFormat: planFormat,
        format: outputFormat
      };
      this.store.dispatch(new sqlQueryActions.ExecuteQuery(this.queryRequest));
      this.querySpinnerVisible = true;
    }

    onClickExplain() {
      //for future use...currently we do not support explaining for INSERT, UPDATE, or DELETE
      /*
      let insert_regex = /insert/i;
      let update_regex = /update/i;
      let delete_regex = /delete/i;
       */
      let select_regex = /select/i;

      let explainString = "";

      if (select_regex.test(this.queryString))
        explainString = this.queryString.replace(select_regex, "explain $& ");
      else
        explainString = "explain " + this.queryString;

      this.runQuery(explainString, true);

      this.explainMode = true;
      this.sendInputToOutput();
    }

    onClickRun() {
      this.explainMode = false;
      this.sendInputToOutput();

      this.runQuery(this.queryString, this.explainMode);
    }

    runQuery(stringToRun: string, isExplain: boolean) {
      this.autodetectDataverse();

      if (this.queryString != this.queryPrepared[Object.keys(this.queryPrepared).length - 1]) {
        //not the same query as before, currentQuery needs to be incremented and another needs to be dispatched
        //increment currentQuery
        if (this.queryPrepared[Object.keys(this.queryPrepared).length - 1] != '')
          this.currentQuery = Object.keys(this.queryPrepared).length;

        this.saveQuery(String(this.currentQuery), this.queryString, this.formatOptions, this.outputOptions);

        this.history.unshift({query: this.queryString, index: this.currentQuery});

        //this.currentHistory = this.history.length - 1;
        //this.viewCurrentHistory = this.history.length;

        //display
        let currentQueryIndex = String(this.currentQuery);
        this.store.dispatch(new appActions.setEditorIndex(currentQueryIndex));
        this.editor.focus();

      } else {
        //the same query as before, currentQuery is not incremented
        //save the current Query
        this.saveQuery(String(this.currentQuery), this.queryString, this.formatOptions, this.outputOptions);
      }

      let planFormat = this.formatOptions;
      let outputFormat = this.outputOptions;
      this.historyIdxSelected = this.currentQuery;

      this.getQueryResults(stringToRun, planFormat, outputFormat); // .replace(/\n/g, " "));
    }

    onClickStop() {
      let cancel_request = {
        requestId: String(this.currentQuery)
      }

      this.store.dispatch(new sqlCancelActions.CancelQuery(cancel_request));
    }

    onClickNew() {
      // Saving first
      this.saveQuery(String(this.currentQuery), this.queryString, this.formatOptions, this.outputOptions);

      //let output section know to hide
      this.hideOutputEmitter.emit(true);

      this.historyIdxSelected = -1;

      // Prepare a new Query String, cleanup screen messages
      this.currentQuery = Object.keys(this.queryPrepared).length;
      this.queryString = "";
      this.editor.getDoc().setValue(this.queryString);
      this.queryErrorMessageString = "";
      this.metricsString = "";
      this.querySuccess = false;
      this.queryError = false;
      this.queryWarningsShow = false;

      this.saveQuery(String(this.currentQuery), "", this.formatOptions, this.outputOptions);
      // lets inform other views what's the current SQL editor
      let currentQueryIndex = String(this.currentQuery);
      this.store.dispatch(new appActions.setEditorIndex(currentQueryIndex));
      this.selected = "Default";
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
      this.queryWarningsCount = 0;
      this.queryWarningsMessages = [];

      this.dataverseSelected();
      this.editor.getDoc().setValue(this.queryString);
      this.editor.execCommand('goDocEnd')
      this.editor.focus();

      //hide output on clear
      this.hideOutputEmitter.emit(true);
    }

    onClickPrevious() {
      if (this.currentQuery > 0) {
        this.nextSQLEditor(this.currentQuery - 1);
      }
    }

    onClickNext() {
      if (this.currentQuery < this.preparedQueryCount - 1) {
        this.nextSQLEditor(this.currentQuery + 1);
      }
      else {
        if (this.queryString != '') {
            this.onClickNew();
        }
      }
    }

    checkNext() {
      if (this.currentQuery == this.preparedQueryCount) {
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
      this.saveQuery(String(this.currentQuery), this.queryString, this.formatOptions, this.outputOptions);

      //this.currentQuery = this.currentQuery + next;
      this.currentQuery = next;
      this.queryErrorMessageString = "";
      this.metricsString = "";

      // Retrieve Metrics or Error Message if Query was executed
      this.showMetrics();

      // Retrieve Metrics or Error Message if Query was executed
      this.showErrors();

      // Retrieve the prepared SQL string
      this.queryString = this.queryPrepared[this.currentQuery];
      this.editor.getDoc().setValue(this.queryString);

      // Select the query from the QUERY History
      this.historyIdxSelected = this.currentQuery;

      this.autodetectDataverse();

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
      this.codemirrorInit();
    }

    /**
     * Initialize codemirror
     */
    codemirrorInit() {
      this.editor = CodeMirror.fromTextArea(this.editor.nativeElement, {
        mode: {
          name: "sql",
          keywords: this.set(this.sqlppKeywords),
          builtin: this.set(this.sqlppTypes),
          atoms: this.set("true false null missing"),
          dateSQL: this.set("date time datetime duration year_month_duration day_time_duration interval"),
          support: this.set("ODBCdotTable doubleQuote binaryNumber hexNumber commentSlashSlash")
        },
        lineWrapping: true,
        showCursorWhenSelecting: true,
        autofocus: true,
        lineNumbers: true,
        autoCloseBrackets: {
          pairs: "()[]{}''\"\"``",
          closeBefore: ")]}'\":;>",
          triples: "",
          explode: "[]{}()",
          override: true
        },
        extraKeys: {"Ctrl-Space": "autocomplete"},
        hint: CodeMirror.hint.sql,
      });
      this.editor.setSize(null, 'auto');
      this.editor.getDoc().setValue(this.queryString);
      this.editor.on('changes', () => {
        this.queryString = this.editor.getValue();
      });
    }

    dataverseSelected() {
      if (this.selected == undefined) {
        this.queryString = 'None';
      } else if (this.selected === 'None' || this.selected === 'Default') {
        this.queryString = '';
        this.selected = 'Default';
      } else {
        this.queryString = 'USE ' + this.selected + '; \n' + this.queryString;
      }
      this.editor.getDoc().setValue(this.queryString);
      this.editor.execCommand('goDocEnd')
      this.editor.focus();
    }

    historySelected(idx: number) {
      if (this.historyStringSelected == undefined) {
        this.historyStringSelected = '';
      } else if (this.historyStringSelected === 'Clear') {
        this.history = [];
        this.historyStringSelected = '';
      }

      this.nextSQLEditor(idx);
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

    saveQuery(editorId, queryString, planFormat, outputFormat) {
      this.queryPrepare = {
        editorId: String(editorId),
        queryString: queryString,
        planFormat: planFormat,
        format: outputFormat
      };
      this.store.dispatch(new sqlQueryActions.PrepareQuery(this.queryPrepare));
    }

    sendInputToOutput() {
      this.inputToOutput['isExplain'] = this.explainMode;
      this.inputToOutput['outputFormat'] = this.outputOptions;

      this.inputToOutputEmitter.emit(this.inputToOutput);
    }

    autodetectDataverse() {
      let dataverseRegex = /use (.*?);/i

      let matches = this.queryString.match(dataverseRegex);

      let detectedDataverse = "";

      if (matches) {
        if (matches.length == 2) {
          detectedDataverse = matches[1];
        }
      }

      if (detectedDataverse != "") {
        let dataverseNames = this.dataverses.map(function (e) {
          return e.DataverseName
        });

        if (dataverseNames.includes(detectedDataverse)) {
          this.selected = detectedDataverse;
        } else {
          this.selected = "Default";
        }
      } else {
        this.selected = "Default";
      }
    }

    set(str: string) {
      let obj = {};
      let words = str.split(" ");
      for (let i = 0; i < words.length; i++) {
        obj[words[i]] = true;
      }
      return obj;
    }
  }
