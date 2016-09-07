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
var SERVER_HOST = "";
var DATAVERSE_QUERY = "for $x in dataset Metadata.Dataverse return $x;"
var DATE_TIME_REGEX = /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}Z)$/;
var DATE_REGEX = /^(\d{4}-\d{2}-\d{2})$/;

var app = angular.module('queryui', ['jsonFormatter', 'ui.codemirror']);

app.service('recordFunctions', function () {
    this.ObjectKeys = function (obj) {
        var typeStr = Object.prototype.toString.call(obj);
        var res = [];
        if (typeStr === "[object Object]") {
            for (var key in obj) {
                if (obj.hasOwnProperty(key) && !(key === "$$hashKey")) {
                    res.push(key)
                }
            }
        } else {
            res = [-1];
        }
        return res;
    }
    this.isObject = function (obj) {
        var typeStr = Object.prototype.toString.call(obj);
        if (typeStr === "[object Object]") {
            return true;
        } else {
            return false;
        }
    }
    this.isArray = function (obj) {
        var typeStr = Object.prototype.toString.call(obj);
        if ((typeStr === "[object Array]" )) {
            return true;
        } else {
            return false;
        }
    }
    this.isNested = function (obj) {
        return this.isObject(obj) || this.isArray(obj);
    }
    this.ObjectValue = function (obj, key) {
        var typeStr;
        var value;
        if (key == -1) {
            typeStr = Object.prototype.toString.call(obj);
            value = obj;
        } else {
            typeStr = Object.prototype.toString.call(obj[key]);
            value = obj[key];
        }
        if (typeStr === "[object Array]") {
            return "List +";
        } else if (typeStr === "[object Object]") {
            return "Record +";
        } else if (typeStr == "[object Null]") {
            return "NULL";
        } else if (DATE_REGEX.exec(value) != null) {
            var dat = new Date(value);
            return dat.getUTCFullYear() + "/" + dat.getUTCMonth() + "/" + dat.getUTCDay();
        } else if (DATE_TIME_REGEX.exec(value) != null) {
            var dat = new Date(value);
            return dat.getUTCFullYear() + "/" + dat.getUTCMonth() + "/" + dat.getUTCDay()
                + " " + dat.getUTCHours() + ":" + dat.getUTCMinutes() + ":" + dat.getUTCSeconds();
        } else {
            return value;
        }
    }
});

app.controller('queryCtrl', function ($rootScope, $scope, $http, recordFunctions) {

    $scope.recordFunctions = recordFunctions;
    $scope.current_tab = 0;
    $scope.current_preview_tab = 0;
    $scope.current_list = 0;
    $scope.maximized = false;
    $scope.collapsed = false;
    $scope.results = [];
    $scope.history = [];
    $scope.dataverses = [];
    $scope.selectedItem = null;
    $scope.selected_dataverse = "";
    $scope.errorText = null;
    $scope.statusText = "Wait...";
    $scope.query_input = "";
    $scope.uiReady = false;

    $scope.queryCmOptions = {
        lineNumbers: true,
        indentWithTabs: true,
        lineWrapping: false,
        mode: 'aql'
    }

    $scope.queryPreviewOptions = {
        indentWithTabs: true,
        lineWrapping: true,
        mode: 'javascript',
        readOnly: true
    }

    $scope.init = function () {
        $http.post("/").then(function (response) {
            SERVER_HOST = location.protocol + "//" + location.hostname + ":" + response.data.api_port;
            $scope.initDataverses();
        }, function (response) {
            $scope.statusText = "Unable to get Asterix HTTP API Port";
        });
    }

    $scope.initDataverses = function () {
        $http.get(SERVER_HOST + "/query?query=" + encodeURI(DATAVERSE_QUERY)).then(function (response) {
                for (i in response.data) {
                    $scope.dataverses.push(response.data[i].DataverseName);
                    $scope.selected_dataverse = $scope.dataverses[0];
                    $scope.statusText = "Web UI Ready";
                    $scope.uiReady = true;
                }
            },
            function (response) {
                $scope.statusText = "Error Occurred Executing Query";
                $scope.errorText = response.data.summary;
                $scope.maximized = false;
            });
        $scope.load();
    }

    $scope.query = function () {
        var timer = new Date().getTime();
        $scope.save($scope.query_input, $scope.selected_dataverse);
        $http.get(SERVER_HOST + "/query?query=" +
            encodeURI("use dataverse " + $scope.selected_dataverse + ";" + $scope.query_input))
            .then(function (response) {
                    $scope.results = response.data;
                    console.log(response);
                    timer = new Date().getTime() - timer;
                    $scope.statusText = "Query returned " + $scope.results.length + " records in " + timer + "ms";
                    $scope.errorText = null;
                    $scope.maximized = false;
                },
                function (response) {
                    $scope.statusText = "Error Occurred Executing Query";
                    $scope.errorText = response.data.summary;
                    $scope.maximized = false;
                    $scope.results = [];
                });
    }

    $scope.isNested = function (obj) {
        for (var key in obj) {
            if (obj.hasOwnProperty(key)) {
                var typeStr = Object.prototype.toString.call(obj[key]);
              if (typeStr === "[object Array]" || typeStr === "[object Object]") {
                return true;
              }
            }
        }
        return false;
    }

    $scope.isRecordPlus = function (obj, key) {
        var value;
        if (key == -1) {
            value = obj;
        } else {
            value = obj[key];
        }
        return $scope.recordFunctions.isNested(value) ? "asterix-nested" : "";
    }

    $scope.viewRecord = function (obj) {
        $scope.selectedItem = obj;
        $scope.current_preview_tab = 0;
        $("#recordModel").modal();
    }

    $scope.previewJSON = function (obj) {
        return JSON.stringify(obj, null, 4);
    }

    $scope.save = function (query, database) {
        var toSave = [query, database];
        if ($scope.history.length >= 1) {
            var i = $scope.history.length - 1;
            if (new String(query).valueOf() === new String($scope.history[i][0]).valueOf()) {
              if (new String(database).valueOf() === new String($scope.history[i][1]).valueOf()) {
                return;
              }
            }
        }
        if ($scope.history.push(toSave) == 11) {
            $scope.history.shift();
        }
        localStorage.setItem("history", JSON.stringify($scope.history));
    }

    $scope.load = function () {
        var history = JSON.parse(localStorage.getItem("history"));
      if (history != null) {
        $scope.history = history;
      }
    }

    $scope.previewHistory = function (entry) {
        $scope.current_tab = 0;
        $scope.query_input = entry[0];
        $scope.selected_dataverse = entry[1];
    }

    $scope.leftContainerClass = function () {
        if ($scope.maximized) {
            return 'col-md-12';
        } else {
            if ($scope.collapsed) {
                return 'col-md-1';
            } else {
                return 'col-md-4'
            }
        }
    }

    $scope.rightContainerClass = function () {
        if ($scope.maximized) {
            return 'col-md-0';
        } else {
            if ($scope.collapsed) {
                return 'col-md-11';
            } else {
                return 'col-md-8'
            }
        }
    }

});
