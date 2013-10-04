<!DOCTYPE html>
<html style="width: 100%; height: 100%;">
  <head>
    <title>ASTERIX Demo</title>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">

    <link rel="shortcut icon" type="image/png" href="static/img/hyrax.png">

    <!-- Bootstrap & jQuery Styles -->
    <link href="http://ajax.googleapis.com/ajax/libs/jqueryui/1.9.2/themes/base/jquery-ui.css" rel="stylesheet" type="text/css"/>
    <link href="//netdna.bootstrapcdn.com/bootstrap/3.0.0/css/bootstrap.min.css" rel="stylesheet" type="text/css">

    <!-- Bootstrap Javascript -->
    <script src="http://maps.googleapis.com/maps/api/js?sensor=false&libraries=places" type="text/javascript"></script>
        
    <script src="http://code.jquery.com/jquery.min.js"></script>
    <script src="//ajax.googleapis.com/ajax/libs/jqueryui/1.10.3/jquery-ui.min.js"></script>
    <script src="static/js/bootstrap.min.js"></script>
    <script src="static/js/geostats.js"></script>
    <script src="static/js/asterix-sdk-stable.js"></script>
    <script src="static/js/cherry.js"></script>
    
    <style type="text/css">
        
        #map_canvas img {
            max-width: none;
        }
        
        #legend-holder {
            background: white;
            padding: 10px;
            margin: 10px;
            text-align: center;
        }
        
        #review-handles-dropdown {
            padding: 0.5em;
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
          <ul class="nav navbar-nav">
            <li id="about-demo" class="active"><a id="about-mode" href="#">About</a></li>
            <li id="explore-active"><a id="explore-mode" href="#">Explore</a></li>
            <li id="review-active"><a id="review-mode" href="#">Review</a></li>
          </ul>
        </div>
      </div>
    </div><!-- /Nav -->

    <!-- Search Box -->
    <div class="container">
    
      <div class="row" id="r1">
        <div class="col-md-5">
          <div class="container well" id="explore-well">
          
            <!-- Query Builder -->
            <div class="panel panel-primary">
              <div class="panel-heading"><h4><b>Query Builder</b></h4></div>
              
              <!-- List group -->
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
                    <label class="btn btn-default">
                      <input type="radio" id="location-button"> Location
                    </label>
                    <label class="btn btn-default">
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
                  <button id="submit-button">Submit</button>
                  <button id="clear-button">Clear</button>
                  <button id="show-query-button">Show Query</button><br/>
                  <input type="checkbox" value="Submit Asynchronously" name="async" id="asbox" />
                  Submit asynchronously?
                </li>
              </ul>
            </div>
            
            <!-- On Drilldown, here is a nice modal -->
            <div class="modal hide fade" id="drilldown_modal" tabindex="-1" role="dialog">
              <div class="modal-dialog">
                <div class="modal-content">
                  <div class="modal-header">
                    <button type="button" class="close" data-dismiss="modal" aria-hidden="true">&times;</button>
                    <h4 class="modal-title">Explore Tweets</h4>
                  </div>
                  <div class="modal-body" id="drilldown_modal_body"></div>
                  <div class="modal-footer">
                    <button type="button" class="btn btn-default" data-dismiss="modal">Close</button>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
        
        <div class="col-md-7">
          <div class="container well" id="right-col">
            <div id="map_canvas" style="width: 100%; height: 100%;"></div>
            <div id="map_canvas_legend" style="display:none;"></div>
          </div>
        </div>
      </div>
      
      <!-- Modals and Dialogs -->
      <div class="row">
        <!-- Show Query -> Error TODO Doesn't need to be this... -->
        <div id="dialog"><h4>You must submit a query first.</h4></div>
        
      </div><!-- Row -->

    </div><!-- container -->      
  </body>
</html>
        
        <!--
        <div class="col-md-4 well" id="review-well" style="display:none;">
            <div class="btn-group" style="margin-bottom: 10px;" id="group-tweetbooks">
                <a class="btn dropdown-toggle" data-toggle="dropdown" href="#">
                    Tweetbooks
                    <span class="caret"></span>
                </a>
                <ul class="dropdown-menu" id="review-tweetbook-dropdown">
                    <li><a href="#" class="holdmenu">
                        <input type="text" id="new-tweetbook-entry" placeholder="Name a new tweetbook">
                        <button type="button" class="btn" id="new-tweetbook-button">Add</button>
                    </a></li>
                    <li class="divider"></li>
                    <div id="review-tweetbook-titles">
                    </div>
                </ul>
            </div><br/>
            
            <div class="btn-group" id="group-background-query" style="margin-bottom: 10px;">
                <a class="btn dropdown-toggle" data-toggle="dropdown" href="#">
                    Background Queries
                    <span class="caret"></span>
                </a>
                <ul class="dropdown-menu" id="review-handles-dropdown">
                    <div id="async-handle-controls">
                    </div>
                </ul>
            </div>
        </div>
        
      </div><!--/row-->
