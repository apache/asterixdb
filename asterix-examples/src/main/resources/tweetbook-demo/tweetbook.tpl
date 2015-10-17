<!DOCTYPE html>
<!--
 ! Licensed to the Apache Software Foundation (ASF) under one
 ! or more contributor license agreements.  See the NOTICE file
 ! distributed with this work for additional information
 ! regarding copyright ownership.  The ASF licenses this file
 ! to you under the Apache License, Version 2.0 (the
 ! "License"); you may not use this file except in compliance
 ! with the License.  You may obtain a copy of the License at
 !
 !   http://www.apache.org/licenses/LICENSE-2.0
 !
 ! Unless required by applicable law or agreed to in writing,
 ! software distributed under the License is distributed on an
 ! "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 ! KIND, either express or implied.  See the License for the
 ! specific language governing permissions and limitations
 ! under the License.
 !-->
<html style="width: 100%; height: 100%;">
<head>
  <title>ASTERIX Demo</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">

  <link rel="shortcut icon" type="image/png" href="static/img/hyrax.png">

  <!-- Bootstrap & jQuery Styles -->
  <link href="http://ajax.googleapis.com/ajax/libs/jqueryui/1.9.2/themes/base/jquery-ui.css" rel="stylesheet" type="text/css"/>
  <link href="//netdna.bootstrapcdn.com/bootstrap/3.0.0/css/bootstrap.min.css" rel="stylesheet" type="text/css">

  <!-- Bootstrap Javascript -->
  <script src="http://maps.googleapis.com/maps/api/js?sensor=false&libraries=places,drawing,geometry" type="text/javascript"></script>

  <script src="http://code.jquery.com/jquery.min.js"></script>
  <script src="//ajax.googleapis.com/ajax/libs/jqueryui/1.10.3/jquery-ui.min.js"></script>
  <script src="static/js/bootstrap.min.js"></script>
  <script src="static/js/asterix-sdk-stable.js"></script>
  <script src="static/js/rainbowvis.js"></script>
  <script src="static/js/tweetbook.js"></script>

  <style type="text/css">

  .tab-content {
    clear: none;
  }

  #map_canvas img {
    max-width: none;
  }

  .panel-primary {
    border-color: #273f93;
  }

  .panel-primary .panel-heading {
    color: white;
    background-color: #273f93;
    border-color: #273f93;
  }

  #start-date, #end-date {
    position: relative; z-index:100;
  }

  #keyword-textbox, #location-text-box {
    width: 100%;
  }
  </style>
</head>
<body style="width: 100%; height: 100%;">

  <!-- Nav -->
  <div class="navbar navbar-default navbar-static-top">
    <div class="container">
      <div class="navbar-header">
        <button type="button" class="navbar-toggle" data-toggle="collapse" data-target=".navbar-collapse">
          <span class="icon-bar"></span>
          <span class="icon-bar"></span>
          <span class="icon-bar"></span>
        </button>
        <a class="navbar-brand" href="#" style="padding: 0.25em;">
          <img src="static/img/finalasterixlogo.png">
        </a>
      </div>
      <div class="navbar-collapse collapse">
        <ul class="nav navbar-nav" id="mode-tabs">
          <li id="about-active"><a data-toggle="tab" id="about-mode" href="#about-tab">About</a></li>
          <li id="explore-active" class="active"><a data-toggle="tab" id="explore-mode" href="#demo-tab">Explore</a></li>
          <li id="review-active"><a data-toggle="tab" id="review-mode" href="#demo-tab">Review</a></li>
        </ul>
      </div>
    </div>
  </div><!-- /Nav -->

  <div class="tab-content">

    <!-- Begin About Tab -->
    <div class="tab-pane" id="about-tab">
      <div class="container">
        <!-- Welcome Message -->
        <div class="row">
          <p>Welcome to the top-level page of the Tweetbook Demo of AsterixDB.  The purpose of this demo is to illustrate how a "cool application" can be built using the JavaScript SDK of AsterixDB and to exercise all of the AsterixDB HTTP APIs.  If you are building an app of your own, reading the code for this app is a great way to get acquainted with what you'll need to know.</p>

          <p>In this demo, which is based on spatial analysis of Tweets, you will see how to formulate aggregate queries and drill-down queries using the query door of the AsterixDB API.  You will see how to do this either synchronously or asynchronously (for larger queries whose results may take a while to cook).  You will also see how to create and drop datasets (to manage Tweetbooks, notebooks with user commentary on Tweets) and how to perform inserts and deletes (to add/remove Tweetbook entries). Let's walk through the demo.</p>
        </div>
        
        <hr/>

        <!-- Tutorial Part 1: Overview -->
        <div style="margin-bottom: 2em; text-align: center;">
          <img src="static/img/Tutorial1.png" style="max-width:100%;">
        </div><hr/>

        <!-- Tutorial Part 2: Location Search -->
        <div style="margin-bottom: 2em; text-align: center;">
          <img src="static/img/Tutorial2.png" style="max-width:100%;">
        </div><hr/>

        <!-- Tutorial Part 3: Search Results, Drilling Down, Saving Comments -->
        <div style="margin-bottom: 2em; text-align: center;">
          <img src="static/img/Tutorial3.png" style="max-width:100%;">
        </div><hr/>

        <!-- Tutorial Part 4: Review Mode -->
        <div style="margin-bottom: 2em; text-align: center;">
          <img src="static/img/Tutorial4.png" style="max-width:100%;">
        </div> 
      </div>
    </div><!-- End About Tab -->

    <!-- Begin Main Demo Tab -->
    <div class="tab-pane active" id="demo-tab">
      <div class="container">
        <div class="row">
          <!-- Left Column -->
          <div class="col-md-5">

            <!-- Explore Mode: Query Builder Column -->
            <div class="container well" id="explore-well">

              <!-- Query Builder -->
              <div class="panel panel-primary">
                <div class="panel-heading"><h4><b>Query Builder</b></h4></div>
                <ul class="list-group">

                  <!-- Search Keyword -->
                  <li class="list-group-item">
                    <h4 class="list-group-item-heading">Keyword</h4>
                    <input type="text" id="keyword-textbox" placeholder="Enter a keyword, e.g., verizon">
                  </li>

                  <!-- Location -->
                  <li class="list-group-item">
                    <h4 class="list-group-item-heading">Location</h4>
                    <input class="textbox" type="text" id="location-text-box">
                    <div class="btn-group" data-toggle="buttons">
                      <label id="location-label" class="btn btn-default">
                        <input type="radio" id="location-button"> Location
                      </label>
                      <label id="selection-label" class="btn btn-default">
                        <input type="radio" id="selection-button"> Selection
                      </label>
                    </div>
                  </li>

                  <!-- Date Range -->
                  <li class="list-group-item">
                    <h4 class="list-group-item-heading">Limit Date Range</h4>
                    <input type="text" class="textbox" id="start-date"><span class="badge">Start Date</span><br/>
                    <input type="text" class="textbox" id="end-date"><span class="badge">End Date</span>
                  </li>

                  <!-- Results Resolution -->
                  <li class="list-group-item">
                    <h4 class="list-group-item-heading">Result Group Size</h4>

                    <div class="grid-slider" id="grid-lat-slider"></div>
                    <div class="control-label">Latitude: <span id="gridlat">3.0</span></div>

                    <div class="grid-slider" id="grid-lng-slider"></div>
                    <div class="control-label">Longitude: <span id="gridlng">3.0</span></div>
                  </li>

                  <!-- Submission Buttons -->
                  <li class="list-group-item">
                    <button class="btn btn-primary" type="button" id="submit-button">Submit</button>
                    <button class="btn btn-primary" type="button" id="clear-button">Clear</button><br/>
                    <input type="checkbox" value="Submit Asynchronously" name="async" id="asbox" />
                    Submit asynchronously?
                  </li>
                </ul>
              </div>
            </div><!-- End Query Build Column -->

            <!-- Review Mode: Tweetbooks and Async Query Review Column -->
            <div class="container well" id="review-well" style="display:none;">
              <div class="panel panel-default">
                <div class="panel-heading">
                  <h4 class="panel-title">Tweetbooks</h4>
                </div>
                <div class="panel-body">
                  <!-- Box to add a new Tweetbook -->
                  <div class="input-group">
                    <input type="text" class="form-control" id="new-tweetbook-entry" placeholder="Name a new tweetbook">
                    <span class="input-group-btn">
                      <button type="button" class="btn" id="new-tweetbook-button">Add</button>
                    </span>
                  </div>
                  <hr/>

                  <!-- List to add existing tweetbooks -->
                  <div class="btn-group-vertical" id="review-tweetbook-titles"></div>
                </div>
              </div>

              <!-- Background queries -->
              <div class="panel panel-default">
                <div class="panel-heading">
                  <h4 class="panel-title">Background Queries</h4>
                </div>
                <div class="panel-body">
                  <!-- Add existing background queries to list -->
                  <div class="btn-group-vertical" id="review-tweetbook-titles"></div>
                  <div id="async-handle-controls"></div>
                </div>
              </div>
            </div><!-- end Review Mode Column -->
            
            <!-- Container to hold success/error messages -->
            <div id="report-message"></div>
          </div><!-- End Left Column -->

          <!-- Right column holds map & legend -->
          <div class="col-md-7">
          <!-- Map Container -->
          <div class="container well" id="right-col">
            <div id="map_canvas" style="max-width: 100%; height: auto;"></div>
          </div> 

          <!-- Legend Container -->
          <div id="rainbow-legend-container" class="container well" style="display:none;">
            <div class="row">
              <div class="col-md-4 col-md-offset-4" style="text-align: center;"># Tweets</div>
            </div>

            <div class="row">
              <div id="legend-min" class="col-md-2 col-md-offset-1" style="text-align:right;"></div>
              <div id="legend-gradient" class="col-md-5" style="text-align: center;"></div>
              <div id="legend-max" class="col-md-2"></div>
            </div>
          </div><!-- End Legend Container -->
          </div><!-- End Right Column -->
        </div><!-- End Row -->

        <!-- Tweetbook Comment Container -->
        <div class="modal fade" id="drilldown_modal" tabindex="-1" role="dialog" aria-labelledby="myModalLabel" aria-hidden="true">
        <div class="modal-dialog">
          <div class="modal-content">
            <div class="modal-body" id="drilldown_modal_body">

              <!-- Tweet -->
              <div id="modal-body-tweet" class="well well-sm"></div>

              <!-- Existing comments about this tweet -->
              <!-- Create new comment -->
              <div class="panel panel-default" id="modal-existing-note">
                <div class="panel-heading">About this Tweetbook</div>
                <div class="panel-body" id="modal-body-notes-holder">

                  <div class="input-group">
                    <input type="text" class="form-control" id="modal-body-tweet-note">
                    <span class="input-group-btn">
                      <button class="btn btn-default" id="modal-body-trash-icon">
                        <span class="glyphicon glyphicon-trash"></span>
                      </button>
                    </span>
                    <span class="badge" id="modal-current-tweetbook"></span>
                  </div>

                </div>
              </div> 

              <!-- Create new comment -->
              <div class="panel panel-default" id="modal-save-tweet-panel">
                <div class="panel-heading">Save to tweetbook</div>
                <div class="panel-body" id="modal-save-body">

                  <div class="input-group">
                    <span class="input-group-addon">Note</span>
                    <input type="text" class="form-control" id="modal-body-add-note" placeholder="Add a note about this tweet">
                  </div>

                  <div class="input-group">
                    <span class="input-group-addon">Add to</span>
                    <input type="text" class="form-control" id="modal-body-add-to" placeholder="Add it to a Tweetbook">
                  </div><br/>

                  <!-- Comment on tweet to save -->
                  <button type="button" class="btn btn-default" id="save-comment-tweetbook-modal">Save</button>

                  <div id="modal-body-message-holder"></div>

                </div>
              </div> 
            </div>
            <div class="modal-footer">
              <button type="button" class="btn btn-default" data-dismiss="modal">Close</button>
            </div>
          </div>
        </div>
        </div><!-- Modal -->     
      </div><!-- End Container -->
    </div><!-- End Main Demo Tab -->

  </div><!-- End tab list -->
</body>
</html>
