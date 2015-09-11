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
 $(function() {
    var jobSpecDAG = new Graphs.DAG();
    var jobSpecRenderer;

    function drawJobGraph() {
        var jobGraphDiv = $('#job-graph')[0];
        jobSpecRenderer = new Graphs.JsPlumbRenderer(jobSpecDAG, jobGraphDiv, null);
        jobSpecRenderer.refresh();
    }

    function onJobRunDataReceived(data) {
        var run = data.result;

        if (run.status != 'TERMINATED' && run.status != 'FAILURE') {
            setTimeout(fetchJobRun, 10000);
        }
    }

    function fetchJobRun() {
        $.ajax({
            url : '/rest/jobs/' + $.getURLParam('job-id') + '/job-run',
            method : 'GET',
            dataType : 'json',
            success : onJobRunDataReceived
        });
    }

    function onJobActivityGraphDataReceived(data) {
        var jag = data.result;
        activityMap = new Object;
        var activities = jag.activities;
        for ( var i = 0; i < activities.length; ++i) {
            var activity = activities[i];
        }

        drawJobGraph();

        fetchJobRun();
    }

    function fetchJobActivityGraph() {
        $.ajax({
            url : '/rest/jobs/' + $.getURLParam('job-id') + '/job-activity-graph',
            method : 'GET',
            dataType : 'json',
            success : onJobActivityGraphDataReceived
        });
    }

    function onJobSpecificationDataReceived(data) {
        var jobSpec = data.result;
        var operators = jobSpec.operators;
        for ( var i = 0; i < operators.length; ++i) {
            var op = operators[i];
            jobSpecDAG.addNode(op.id, op);
        }
        var connectors = jobSpec.connectors;
        for ( var i = 0; i < connectors.length; ++i) {
            var conn = connectors[i];
            var sNode = jobSpecDAG.lookupNode(conn['in-operator-id']);
            var sIndex = conn['in-operator-port'];
            var tNode = jobSpecDAG.lookupNode(conn['out-operator-id']);
            var tIndex = conn['out-operator-port'];
            jobSpecDAG.addEdge(conn.id, conn, sNode, sIndex, tNode, tIndex);
        }
        fetchJobActivityGraph();
    }

    function fetchJobSpecification() {
        $.ajax({
            url : '/rest/jobs/' + $.getURLParam('job-id') + '/job-specification',
            method : 'GET',
            dataType : 'json',
            success : onJobSpecificationDataReceived
        });
    }

    function init() {
        fetchJobSpecification();
    }

    jsPlumb.bind("ready", function() {
        init();
    });
});
