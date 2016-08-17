/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
CodeMirror.defineMode("aql", function() {

  var KEYWORD_MATCH = ["return","select","for","from","at","in","let","with","where","order","by","asc","desc","group",
                        "keeping","limit","offset","distinct","dataset","or","and"];
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
            return "variable-2";
          }

          if (stream.match(DOT_MATCH)) {
            return "variable-3";
          }

          //string variable match
          if (stream.match(DOUBLE_QUOTE_MATCH)) {
            return "string";
          }
          if (stream.match(SINGLE_QUOTE_MATCH)) {
            return "string";
          }

          //keyword match
          for (i in KEYWORD_MATCH){
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

      }
  };
});