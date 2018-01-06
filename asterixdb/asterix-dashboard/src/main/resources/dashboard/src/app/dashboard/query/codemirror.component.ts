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
/**
 * Integrating codemirror (using ng2-codemirror) with our application
 *
 * component from "https://runkit.com/npm/ng2-codemirror"
 *                "https://www.npmjs.com/package/ng2-codemirror"
 * copy component from /src/codemirror.component.ts
 * and modified for custom mode (asterix aql, sql++ code hilighting)
 *
 * therefore, actually we don't need to "npm install ng2-codemirror"
 *
 * Because on the outside of this component,
 * It was hard to access the codemirror instance that 'ng-codemirror' use
 * So, we copied the component in our application and modified it
 *
 * 'codemirror.js(^5.23.0)' is included in the 'index.html'
 * And in this component(codemirror.component.ts)
 * add statement like "declare var CodeMirror: any;"
 *
 * I don't know whether this is right way
 *
 * ref 1) usage : https://embed.plnkr.co/8e9gxss9u10VeFrv29Zt/
 * ref 2) custom mode : http://jsfiddle.net/TcqAf/99/
 * ref 3) integrating : http://stackoverflow.com/questions/37092142/integrating-codemirror-with-angular2-typescript
 * ref 3) integrating :  https://medium.com/@s_eschweiler/using-external-libraries-with-angular-2-87e06db8e5d1#.8ok74uvwg
 */
 import {
   Component,
   Input,
   Output,
   ElementRef,
   ViewChild,
   EventEmitter,
   forwardRef,
   AfterViewInit,
   OnDestroy
 } from '@angular/core';
 import { NG_VALUE_ACCESSOR } from '@angular/forms';
 import * as CodeMirror from 'codemirror';

/**
 * CodeMirror component
 * Usage :
 * <codemirror [(ngModel)]="data" [config]="{...}"></codemirror>
 */
@Component({
  moduleId: module.id,
  selector: 'codemirror',
  providers: [
    {
      provide: NG_VALUE_ACCESSOR,
      useExisting: forwardRef(() => CodemirrorComponent),
      multi: true
    }
  ],
  styleUrls: ['codemirror.component.scss'],
  template: `<textarea class="code" #host></textarea>`,//,
})

export class CodemirrorComponent implements AfterViewInit, OnDestroy {
  @Input() config;
  @Output() change = new EventEmitter();
  @Output() focus = new EventEmitter();
  @Output() blur = new EventEmitter();
  @Output() instance = null;
  @ViewChild('host') host;
  _value = '';

  /**
   * Constructor
   */
  constructor(){
		/**
		 * Custom mode for AsterixDB
		 */
		CodeMirror.defineMode("asterix", function(){
		  var KEYWORD_MATCH = [
				// AQL
				"drop", "dataverse", "dataset",
				"if", "exists", "create",
				"use", "type", "as", "closed",
				"primary", "key",  "hints", "cardinality",
				"index", "on", "btree", "rtree", "keyword",
				"for", "in", "Metadata", "Dataset",
				"return", "Index", "load", "using", "localfs", "path", "format",
				// Query (not perfect)
				"from", "in", "with", "group", "by", "select",
				"let", "where", "order", "asc", "desc", "limit",
				"keeping", "offset", "distinct", "or", "and",
				// Built in functions (TODO)
				// Built in functions (TODO)
				// Built in functions (TODO)
				// Asterix Data Model
				// Primitive type
				"boolean",
				"tinyint", "smallint", "integer", "bigint",
				"float", "double",
				"string",
				"binary", "hex", "base64",
				"point", "line", "rectangle", "circle", "polygon",
				"date", "time", "datetime", "duration", "interval", "uuid",
				// Incomplete information type
				"null", "missing",
				// Derived type
				// object {}, array [], multiset {{}}
				// SQL++
				"DROP", "DATAVERSE", "IF", "EXISTS", "CREATE", "USE", "TYPE", "AS", "DATASET", "PRIMARY", "KEY",
				"INDEX", "SELECT", "VALUE", "INSERT", "INTO", "FROM", "WHERE", "AND", "SOME", "IN", "SATISFIES", "IS", "UNKNOWN", "NOT", "EVERY",
				"GROUP", "BY", "ORDER", "DESC", "LIMIT", "OR", "SET", "DELETE", "LOAD", "USING",
			];

			//"(", ")","{{", "}}", "[", "]",	"{", "}",  ";", ",", ":","?", "=",
      var VAR_MATCH = /[$][a-zA-Z]+(\d*)/;
			var DOT_MATCH = /[.](\S)*/;
			var DOUBLE_QUOTE_MATCH = /["].*["]/;
			var SINGLE_QUOTE_MATCH = /['].*[']/;
			var BREAK_POINT = /(\s)/;

			return {
				startState: function() {return {inString: false};},
				token: function(stream, state) {
					if (state.newLine == undefined)state.newLine = true;

					//match variable reference
					if (stream.match(VAR_MATCH)) {
						return "variable";
					}

					if (stream.match(DOT_MATCH)) {
						return "dot-variable";
					}

					//string variable match
					if (stream.match(DOUBLE_QUOTE_MATCH)) {
						return "string";
					}
					if (stream.match(SINGLE_QUOTE_MATCH)) {
						return "string";
					}

					//keyword match
					for (var i in KEYWORD_MATCH){
						if (state.newLine && stream.match(KEYWORD_MATCH[i])){
								return "keyword";
						 }
					}

					if (stream.peek() === " " || stream.peek() === null){
						state.newLine = true;
					}else{
						state.newLine = false;
					}
					stream.next();
					return null;
				}
			};
		});
	}

  get value() { return this._value; };

  @Input() set value(v) {
    if (v !== this._value) {
      this._value = v;
      this.onChange(v);
    }
  }

  /**
   * On component destroy
   */
  ngOnDestroy() {}

  /**
   * On component view init
   */
  ngAfterViewInit() {
    this.config = this.config || {};
    this.codemirrorInit(this.config);
  }

  /**
   * Initialize codemirror
   */
  codemirrorInit(config){
    this.instance = CodeMirror.fromTextArea(this.host.nativeElement, config);
    this.instance.setValue(this._value);
    this.instance.setSize(null, 90);
    this.instance.on('change', () => {
      this.updateValue(this.instance.getValue());
    });

    this.instance.on('focus', () => {
      this.focus.emit();
    });

    this.instance.on('blur', () => {
      this.blur.emit();
    });
  }

  /**
   * Value update process
   */
  updateValue(value){
    this.value = value;
    this.onTouched();
    this.change.emit(value);
  }

  /**
   * Implements ControlValueAccessor
   */
  writeValue(value){
    this._value = value || '';
    if (this.instance) {
      this.instance.setValue(this._value);
    }
  }

  onChange(_) {}
  onTouched() {}
  registerOnChange(fn){this.onChange = fn;}
  registerOnTouched(fn){this.onTouched = fn;}
}
