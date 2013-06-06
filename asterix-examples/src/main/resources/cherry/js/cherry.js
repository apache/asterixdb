$(function() {	
	    
    APIHandler = new AsterixSDK();

    APIqueryTracker = {};
    drilldown_data_map = {};
    drilldown_data_map_vals = {};
    asyncQueryManager = {};
    
    // Review Mode
    review_mode_tweetbooks = [];
    review_mode_handles = [];
    
    $('#drilldown_modal').modal({ show: false});
    $('#explore-mode').click( onLaunchExploreMode );
    $('#review-mode').click( onLaunchReviewMode );
    
    /** UI **/
    map_cells = [];
    map_tweet_markers = [];
    param_placeholder = {};
    
    $("#clear-button").button().click(function () {
        mapWidgetClearMap();
        param_placeholder = {};
        
        map.setZoom(4);
        map.setCenter(new google.maps.LatLng(38.89, -77.03));
        
        $('#query-preview-window').html('');
        $("#metatweetzone").html('');
    });
    
    $("#selection-button").button('toggle');
 
    var dialog = $("#dialog").dialog({
        width: "auto",
        title: "AQL Query"
    }).dialog("close");
    $("#show-query-button")
    	.button()
    	.attr("disabled", true)
    	.click(function (event) {
        	$("#dialog").dialog("open");
    	});
    
    // setup grid sliders
    var updateSliderDisplay = function(event, ui) {
        if (event.target.id == "grid-lat-slider") {
            $("#gridlat").text(""+ui.value);
        } else {
          $("#gridlng").text(""+ui.value);
        }
    };
    
    sliderOptions = {
        max: 20,
        min: .1,
        step: .1,
        value: 2.0,
        slidechange: updateSliderDisplay,
        slide: updateSliderDisplay,
        start: updateSliderDisplay,
        stop: updateSliderDisplay
    };

    $("#gridlat").text(""+sliderOptions.value);
    $("#gridlng").text(""+sliderOptions.value);
    $(".grid-slider").slider(sliderOptions);
    
    // setup datepickers
    var dateOptions = {
        dateFormat: "yy-mm-dd",
        defaultDate: "2012-01-02",
        navigationAsDateFormat: true,
        constrainInput: true
    };
    var start_dp = $("#start-date").datepicker(dateOptions);
    start_dp.val(dateOptions.defaultDate);
    dateOptions['defaultDate'] = "2012-12-31";
    var end_dp= $("#end-date").datepicker(dateOptions);
    end_dp.val(dateOptions.defaultDate);
    
    // This little bit of code manages period checks of the asynchronous query manager,
    // which holds onto handles asynchornously received. We can set the handle update
    // frequency using seconds, and it will let us know when it is ready.
    var intervalID = setInterval( 
        function() {
    		asynchronousQueryIntervalUpdate();
    	}, 
    	asynchronousQueryGetInterval()
    );
    
    // setup map
    onOpenExploreMap();
    var mapOptions = {
        center: new google.maps.LatLng(38.89, 77.03),
        zoom: 4,
        mapTypeId: google.maps.MapTypeId.ROADMAP, // SATELLITE
        streetViewControl: false,
        draggable : false
    };
    map = new google.maps.Map(document.getElementById('map_canvas'), mapOptions);
    
    // setup location autocomplete
    var input = document.getElementById('location-text-box');
    var autocomplete = new google.maps.places.Autocomplete(input);
    autocomplete.bindTo('bounds', map);

    google.maps.event.addListener(autocomplete, 'place_changed', function() {
        var place = autocomplete.getPlace();
        if (place.geometry.viewport) {
            map.fitBounds(place.geometry.viewport);
        } else {
            map.setCenter(place.geometry.location);
            map.setZoom(17);  // Why 17? Because it looks good.
        }
        var address = '';
        if (place.address_components) {
            address = [(place.address_components[0] && place.address_components[0].short_name || ''),
              (place.address_components[1] && place.address_components[1].short_name || ''),
              (place.address_components[2] && place.address_components[2].short_name || '') ].join(' ');
        }
    });
    
    // handle selection rectangle drawing
    shouldDraw = false;
    var startLatLng;
    selectionRect = null;
    var selectionRadio = $("#selection-button");
    var firstClick = true;
    
    google.maps.event.addListener(map, 'mousedown', function (event) {
        // only allow drawing if selection is selected
        if (selectionRadio.hasClass("active")) {
            startLatLng = event.latLng;
            shouldDraw = true;
        }
    });
    
    //triggerUIUpdateOnNewTweetBook({"title" : "Party"});

    google.maps.event.addListener(map, 'mousemove', drawRect);
    function drawRect (event) {
        if (shouldDraw) {
            if (!selectionRect) {
                var selectionRectOpts = {
                    bounds: new google.maps.LatLngBounds(startLatLng, event.latLng),
                    map: map,
                    strokeWeight: 1,
                    strokeColor: "2b3f8c",
                    fillColor: "2b3f8c"
                };
                selectionRect = new google.maps.Rectangle(selectionRectOpts);
                google.maps.event.addListener(selectionRect, 'mouseup', function () {
                    shouldDraw = false;
                    //submitQuery();
                });
            } else {
                if (startLatLng.lng() < event.latLng.lng()) {
                    selectionRect.setBounds(new google.maps.LatLngBounds(startLatLng, event.latLng));
                } else {
                    selectionRect.setBounds(new google.maps.LatLngBounds(event.latLng, startLatLng));
                }
            }
        }
    };
    
    // toggle location search style: by location or by map selection
    $('#selection-button').on('click', function (e) {
        $("#location-text-box").attr("disabled", "disabled");
        if (selectionRect) {
            selectionRect.setMap(map);
        }
    });
    $('#location-button').on('click', function (e) {
        $("#location-text-box").removeAttr("disabled");
        if (selectionRect) {
            selectionRect.setMap(null);
        }
    });
    
    $('.dropdown-menu a.holdmenu').click(function(e) {
        e.stopPropagation();
    });
    
    $('#new-tweetbook-button').on('click', function (e) {
        onCreateNewTweetBook($('#new-tweetbook-entry').val());
        
        $('#new-tweetbook-entry').val($('#new-tweetbook-entry').attr('placeholder'));
    });
    
    // handle ajax calls
    $("#submit-button").button().click(function () {
    	// Clear current map on trigger
    	mapWidgetClearMap();
    	
    	// gather all of the data from the inputs
        var kwterm = $("#keyword-textbox").val();
        var startdp = $("#start-date").datepicker("getDate");
        var enddp = $("#end-date").datepicker("getDate");
        var startdt = $.datepicker.formatDate("yy-mm-dd", startdp)+"T00:00:00Z";
        var enddt = $.datepicker.formatDate("yy-mm-dd", enddp)+"T23:59:59Z";

        var formData = {
            "keyword": kwterm,
            "startdt": startdt,
            "enddt": enddt,
            "gridlat": $("#grid-lat-slider").slider("value"),
            "gridlng": $("#grid-lng-slider").slider("value")
        };

    	// Get Map Bounds
    	var bounds;
        if ($('#selection-button').hasClass("active") && selectionRect) {
            bounds = selectionRect.getBounds();
        } else {
            bounds = map.getBounds();
        }
    	
    	formData["swLat"] = Math.abs(bounds.getSouthWest().lat());
        formData["swLng"] = Math.abs(bounds.getSouthWest().lng());
        formData["neLat"] = Math.abs(bounds.getNorthEast().lat());
        formData["neLng"] = Math.abs(bounds.getNorthEast().lng());
		var formBounds = {
            "ne" : { "lat" : formData["neLat"], "lng" : formData["neLng"]}, 
		    "sw" : { "lat" : formData["swLat"], "lng" : formData["swLng"]}
        };

		var build_cherry_mode = "synchronous";
		if ($('#asbox').is(":checked")) {
		    build_cherry_mode = "asynchronous";
		}
	
        var f = new FLWOGRExpression()
            .bind(new ForClause("$t", null, new AQLClause().set("dataset TweetMessages")))
            .bind(new LetClause("keyword", new AQLClause().set('"' + formData["keyword"] + '"')))
            .bind(new LetClause("region", new AQLClause().set(temporary_rectangle(formBounds))))
            .bind(new WhereClause(new AExpression().set(
                [
		            'spatial-intersect($t.sender-location, $region)',
		            '$t.send-time > datetime("' + formData["startdt"] + '")',
		            '$t.send-time < datetime("' + formData["enddt"] + '")',
		            'contains($t.message-text, $keyword)'
                ].join(" and ")
            )))
            .bind(new AQLClause().set("group by $c := spatial-cell($t.sender-location, create-point(24.5,-125.5), " + formData["gridlat"].toFixed(1) + ", " + formData["gridlng"].toFixed(1) + ") with $t"))
            .bind(new ReturnClause({ "cell" : "$c", "count" : "count($t)" }));
        
        var extra = {
            "payload" : formData,
            "query_string" : "use dataverse twitter;\n"
        };
        
        var a = new AsterixSDK()
            .callback(cherryQuerySyncCallback, "sync")
            .callback(cherryQueryAsyncCallback, "async")
            .send(
                "http://localhost:19002/query", 
                {
                    "query" : "use dataverse twitter;\n" + f.val(),
                    "mode" : build_cherry_mode
                },
                extra        
            );
        
		APIqueryTracker = {
		    "query" : "use dataverse twitter;",// buildCherryQuery.parameters["statements"].join("\n"),
		    "data" : formData
		};
		
		$('#dialog').html(APIqueryTracker["query"]);//.replace("\n", '<br />'));

        if (!$('#asbox').is(":checked")) {
		    $('#show-query-button').attr("disabled", false);
        } else {
            $('#show-query-button').attr("disabled", true);
        }
    });
    
});

function temporary_rectangle(bounds) {
    var lower_left = 'create-point(' + bounds["sw"]["lat"] + ',' + bounds["sw"]["lng"] + ')';
    var upper_right = 'create-point(' + bounds["ne"]["lat"] + ',' + bounds["ne"]["lng"] + ')';
    return 'create-rectangle(' + lower_left + ', ' + upper_right + ')';
}

/** Asynchronous Query Management - Handles & Such **/

/**
* Checks through each asynchronous query to see if they are ready yet
*/
function asynchronousQueryIntervalUpdate() {
    for (var handle_key in asyncQueryManager) {
        if (!asyncQueryManager[handle_key].hasOwnProperty("ready")) { 
            asynchronousQueryGetAPIQueryStatus( asyncQueryManager[handle_key]["handle"], handle_key ); 
        }
    }
}

/**
* Returns current time interval to check for asynchronous query readiness
* @returns  {number}    milliseconds between asychronous query checks
*/
function asynchronousQueryGetInterval() {
    var seconds = 10;
    return seconds * 1000;
}

/**
* Updates UI when an API Query's status is marked ready
* @param    {Object}    res, a result object from the Asterix API
* @param    {object}    extra_info, containing the asynchronous handle's id
*/
function asynchronousQueryAPIStatusReceived (res, extra_info) {
    var handle_outcome = $.parseJSON(res[0]);
    var handle_id = extra_info["handle_id"];
    if (handle_outcome["status"] == "SUCCESS") {
    
        // We don't need to check if this one is ready again, it's not going anywhere...
        // Unless the life cycle of handles has changed drastically
        asyncQueryManager[handle_id]["ready"] = true;
        
        // Make this handle's result look retrievable
        $('#handle_' + handle_id).addClass("label-success");
    }    
}

/**
* Retrieves status of an asynchronous query, using an opaque result handle from API
* @param    {Object}    handle, an object previously returned from an async call
* @param    {number}    handle_id, the integer ID parsed from the handle object
*/
function asynchronousQueryGetAPIQueryStatus (handle, handle_id) {
    var apiQueryStatus = new AsterixCoreAPI()
        .dataverse("twitter")
        .handle(handle)
        .success(asynchronousQueryAPIStatusReceived, true)
        .add_extra("handle_id", handle_id)
        .api_core_query_status();
}

/**
* On-success callback after async API query
* @param    {object}    res, a result object containing an opaque result handle to Asterix
* @param    {object}    extra, a result object containing a query string and query parameters
*/
function cherryQueryAsyncCallback(res, extra) {
    
    // Parse handle, handle id and query from async call result
    var handle = res[0];
    var handle_query = extra["query_string"];
    var handle_id = $.parseJSON(handle)["handle"].toString().split(',')[0];    
    
    // Add to stored map of existing handles
    asyncQueryManager[handle_id] = {
        "handle" : handle,
        "query" : handle_query,
        "data" : extra["payload"]
    };
    
    $('#review-handles-dropdown').append('<a href="#" class="holdmenu"><span class="label" id="handle_' + handle_id + '">Handle ' + handle_id + '</span></a>');
    
    $('#handle_' + handle_id).hover(
        function(){ 
            $('#query-preview-window').html('');
            $('#query-preview-window').html('<br/><br/>' + asyncQueryManager[handle_id]["query"]);
        },
        function() {
            $('#query-preview-window').html('');
        }
    ); 
    
    $('#handle_' + handle_id).on('click', function (e) {
        
        // make sure query is ready to be run
        if (asyncQueryManager[handle_id]["ready"]) {
        
            // Update API Query Tracker and view to reflect this query
            $('#query-preview-window').html('<br/><br/>' + asyncQueryManager[handle_id]["query"]);
            APIqueryTracker = {
                "query" : asyncQueryManager[handle_id]["query"],
                "data"  : asyncQueryManager[handle_id]["data"]
            };
            $('#dialog').html(APIqueryTracker["query"]);
        
            // Generate new Asterix Core API Query
            var asyncResultQuery = new AsterixCoreAPI()
                .dataverse("twitter")
                .handle(asyncQueryManager[handle_id]["handle"])
                .success(cherryQuerySyncCallback, true)
                .add_extra("payload", asyncQueryManager[handle_id]["data"]) // Legacy
		        .add_extra("query_string", asyncQueryManager[handle_id]["query"]) // Legacy
		        .api_core_query_result(); 
        }
    });
}


/** Core Query Management and Drilldown

/**
* Utility Method for parsing a record of this form:
* { "cell": rectangle("22.5,64.5 24.5,66.5"), "count": 5 }
* returns a json object with keys: weight, latSW, lngSW, latNE, lngNE
*/
function getRecord(cell_count_record) {
    var record_representation = {};
    
    var rectangle = cell_count_record.split('")')[0].split('("')[1];
    record_representation["latSW"] = parseFloat(rectangle.split(" ")[0].split(',')[0]);
    record_representation["lngSW"] = parseFloat(rectangle.split(" ")[0].split(',')[1]);
    record_representation["latNE"] = parseFloat(rectangle.split(" ")[1].split(',')[0]);
    record_representation["lngNE"] = parseFloat(rectangle.split(" ")[1].split(',')[1]);
    record_representation["weight"] = parseInt(cell_count_record.split('count": ')[1].split(" ")[0]);
    
    return record_representation;
}

/**
* A spatial data cleaning and mapping call
* @param    {Object}    res, a result object from a cherry geospatial query
* @param    {Object}    extra, extra data passed from the API call - legacy stuff
*/
function cherryQuerySyncCallback(res, extra) {
    records = res["results"];

    var coordinates = [];
    var weights = [];
                
    for (var subrecord in records) {
        for (var record in records[subrecord]) {
            
            var coordinate = getRecord(records[subrecord][record]);
            weights.push(coordinate["weight"]);
            coordinates.push(coordinate);
        }
    }
    triggerUIUpdate(coordinates, extra["payload"], weights);
}

/**
* Triggers a map update based on a set of spatial query result cells
* @param    [Array]     mapPlotData, an array of coordinate and weight objects
* @param    [Array]     params, an object containing original query parameters [LEGACY]
* @param    [Array]     plotWeights, a list of weights of the spatial cells - e.g., number of tweets
*/
function triggerUIUpdate(mapPlotData, params, plotWeights) {
    /** Clear anything currently on the map **/
    mapWidgetClearMap();
    param_placeholder = params;
    
    // Compute data point spread
    var dataBreakpoints = mapWidgetLegendComputeNaturalBreaks(plotWeights);
     
    $.each(mapPlotData, function (m, val) {
    
        // Only map points in data range of top 4 natural breaks
        if (mapPlotData[m].weight > dataBreakpoints[0]) {
        
            // Get color value of legend 
            var mapColor = mapWidgetLegendGetHeatValue(mapPlotData[m].weight, dataBreakpoints);
            var markerRadius = mapWidgetComputeCircleRadius(mapPlotData[m], dataBreakpoints);
            var point_opacity = 1.0; // TODO
           
            var point_center = new google.maps.LatLng(
                (mapPlotData[m].latSW + mapPlotData[m].latNE)/2.0, 
                (mapPlotData[m].lngSW + mapPlotData[m].lngNE)/2.0);
            
            // Create and plot marker
            var map_circle_options = {
                center: point_center,
                radius: markerRadius,
                map: map,
                fillOpacity: point_opacity,
                fillColor: mapColor,
                clickable: true
            };
            var map_circle = new google.maps.Circle(map_circle_options);
            map_circle.val = mapPlotData[m];
            
            // Clicking on a circle drills down map to that value
            google.maps.event.addListener(map_circle, 'click', function (event) {
                onMapPointDrillDown(map_circle.val);     
            });
            
            // Add this marker to global marker cells
            map_cells.push(map_circle);
        }    
    });
    
    // Add a legend to the map
    mapControlWidgetAddLegend(dataBreakpoints);
}

/**
* prepares an Asterix API query to drill down in a rectangular spatial zone
*
* @params {object} marker_borders [LEGACY] a set of bounds for a region from a previous api result
*/
function onMapPointDrillDown(marker_borders) {
    var zoneData = APIqueryTracker["data"]; // TODO: Change how this is managed
    
    var zswBounds = new google.maps.LatLng(marker_borders.latSW, marker_borders.lngNE);
    var zneBounds = new google.maps.LatLng(marker_borders.latNE, marker_borders.lngSW); 
    
    var zoneBounds = new google.maps.LatLngBounds(zswBounds, zneBounds);
    zoneData["swLat"] = zoneBounds.getSouthWest().lat();
    zoneData["swLng"] = -1*zoneBounds.getSouthWest().lng();
    zoneData["neLat"] = zoneBounds.getNorthEast().lat();
    zoneData["neLng"] = -1*zoneBounds.getNorthEast().lng();
    
    mapWidgetClearMap();
    
    var customBounds = new google.maps.LatLngBounds();
    var zoomSWBounds = new google.maps.LatLng(zoneData["swLat"], -1*zoneData["swLng"]);
    var zoomNEBounds = new google.maps.LatLng(zoneData["neLat"], -1*zoneData["neLng"]); 
    customBounds.extend(zoomSWBounds);
    customBounds.extend(zoomNEBounds);
    map.fitBounds(customBounds);
    
    var drilldown_string = ["use dataverse " + "twitter" + ";",
        "for $t in dataset('" + "TweetMessages" + "')",
        "let $keyword := \"" +zoneData["keyword"] + "\"",
        "let $region := polygon(\"", 
           zoneData["neLat"] + "," + zoneData["swLng"] + " ",
           zoneData["swLat"] + "," + zoneData["swLng"] + " ",
           zoneData["swLat"] + "," + zoneData["neLng"] + " ",
           zoneData["neLat"] + "," + zoneData["neLng"] + "\")",
           "where spatial-intersect($t.sender-location, $region) and",
           "$t.send-time > datetime(\"" + zoneData["startdt"] + "\") and $t.send-time < datetime(\"" + zoneData["enddt"] + "\") and",
           "contains($t.message-text, $keyword)",
           "return { \"tweetId\": $t.tweetid, \"tweetText\": $t.message-text, \"tweetLoc\": $t.sender-location}"];
    
    var zQ = new AsterixCoreAPI()
        .dataverse("twitter")
        .statements(drilldown_string)
        .add_extra("payload", zoneData) // Legacy
        .mode("synchronous")
        .success(onTweetbookQuerySuccessPlot, true)
        .add_extra("query_string", drilldown_string.join(" "))
        .add_extra("marker_path", "../img/mobile2.png")
        .add_extra("on_click_marker", onClickTweetbookMapMarker)
        .add_extra("on_clean_result", onCleanTweetbookDrilldown)
        .api_core_query();
}

function triggerUIUpdateOnDropTweetBook(extra_info) {
    // TODO Remove menu entry
    // review-tweetbook-titles.html('')
    // Append each in review_mode_tweetbooks if not same as extra_info["title"]
    // $('#review-tweetbook-titles').append('<li><a href="#">' + extra_info["title"] + '</a></li>');
}

function onDrillDownAtLocation(tO) {

    $('#drilldown_modal_body').append('<div id="drilltweetobj' + tO["tweetEntryId"] + '"></div>');
    
    $('#drilltweetobj' + tO["tweetEntryId"]).append('<p>' + tO["tweetText"] + '</p>');
    
    $('#drilltweetobj' + tO["tweetEntryId"]).append('<input class="textbox" type="text" id="metacomment' + tO["tweetEntryId"] + '">');
    
    if (tO.hasOwnProperty("tweetbookComment")) {
        $('#metacomment' + tO["tweetEntryId"]).val(tO["tweetbookComment"]);
    }
    
    $('#drilltweetobj' + tO["tweetEntryId"]).append('<button title="' + tO["tweetEntryId"] + '" id="meta' + tO["tweetEntryId"] + '">Add Comment to...</button>');
    
    $('#drilltweetobj' + tO["tweetEntryId"]).append('<input class="textbox" type="text" id="tweetbooktarget' + tO["tweetEntryId"] + '">');

    $('#meta' + tO["tweetEntryId"])
        .button()
        .click( function () {
       
            var valid = $('#meta' + tO["tweetEntryId"]).attr('title');
            var valcomment = $("#metacomment" + valid).val();
            var valtext = drilldown_data_map_vals[valid.toString()]["tweetText"];
            var tweetbookname = $("#tweetbooktarget" + valid).val();
            
            //Try to add the tweetbook, if it does not already exist
            onCreateNewTweetBook(tweetbookname);
            
            var apiCall = new AsterixCoreAPI()
                .dataverse("twitter")
                .statements([
                    'delete $l from dataset ' + tweetbookname + ' where $l.id = "' + valid + '";',
                    'insert into dataset ' + tweetbookname + '({ "id" : "' + valid + '", "metacomment" : "' + valcomment + '"});'  
                ])
                .api_core_update();
        });
    
}

function onCreateNewTweetBook(tweetbook_title) {
    
    var newTweetbookAPICall = new AsterixCoreAPI()
        .dataverse("twitter")
        .create_dataset({ 
            "dataset"       : tweetbook_title,
            "type"          : "MetaTweet",
            "primary_key"   : "id"
        })
        .add_extra("title", tweetbook_title)
        .success(triggerUIUpdateOnNewTweetBook, true)
        .api_core_update(); 
        // Possible bug...ERROR 1: Invalid statement: Non-DDL statement DATASET_DECL to the DDL API.
      
    /*var removeTest = new AsterixCoreAPI()
        .dataverse("twitter")
        .drop_dataset("blah")
        .api_core_update(); */
}

function onDropTweetBook(tweetbook_title) {
    var removeTest = new AsterixCoreAPI()
        .dataverse("twitter")
        .drop_dataset(tweetbook_title)
        .success(triggerUIUpdateOnDropTweetBook, true)
        .api_core_update(); 
}

function onTweetbookQuerySuccessPlot (res, extra) {
    var response = $.parseJSON(res[0]);
    var records = response["results"];
    var coordinates = [];
    map_tweet_markers = [];  
    map_tweet_overlays = [];
    drilldown_data_map = {};
    drilldown_data_map_vals = {};
    
    var micon = extra["marker_path"];
    var marker_click_function = extra["on_click_marker"];
    var clean_result_function = extra["on_clean_result"];
    
    coordinates = clean_result_function(records);

    for (var dm in coordinates) {
        var keyLat = coordinates[dm].tweetLat.toString();
        var keyLng = coordinates[dm].tweetLng.toString();
        if (!drilldown_data_map.hasOwnProperty(keyLat)) {
            drilldown_data_map[keyLat] = {}; 
        }
        if (!drilldown_data_map[keyLat].hasOwnProperty(keyLng)) {
            drilldown_data_map[keyLat][keyLng] = []; 
        }
        drilldown_data_map[keyLat][keyLng].push(coordinates[dm]);
        drilldown_data_map_vals[coordinates[dm].tweetEntryId.toString()] = coordinates[dm];  
    }
    
    $.each(drilldown_data_map, function(drillKeyLat, valuesAtLat) {
        $.each(drilldown_data_map[drillKeyLat], function (drillKeyLng, valueAtLng) {
            
            // Get subset of drilldown position on map
            var cposition =  new google.maps.LatLng(parseFloat(drillKeyLat), parseFloat(drillKeyLng));
            
            // Create a marker using the snazzy phone icon
            var map_tweet_m = new google.maps.Marker({
                position: cposition,
                map: map,
                icon: micon,
                clickable: true,
            });
            
            // Open Tweet exploration window on click
            google.maps.event.addListener(map_tweet_m, 'click', function (event) {
                marker_click_function(drilldown_data_map[drillKeyLat][drillKeyLng]);
            });
            
            // Add marker to index of tweets
            map_tweet_markers.push(map_tweet_m); 
            
        });
    });
}

function triggerUIUpdateOnNewTweetBook(extra_info) {
    // Add tweetbook to log
    if (parseInt($.inArray(extra_info["title"], review_mode_tweetbooks)) == -1) {
        review_mode_tweetbooks.push(extra_info["title"]);
        
        // Add menu entry
        $('#review-tweetbook-titles').append('<li><a href="#"><span id="tbook_' + extra_info["title"] + '">' + extra_info["title"] + '</span></a></li>');
    
        // Add on-click behavior
        $("#tbook_" + extra_info["title"]).on('click', function(e) {
            var plotTweetbookQuery = new AsterixCoreAPI()
                .dataverse("twitter")
                .success(onTweetbookQuerySuccessPlot, true)
                .aql_for({"mt": extra_info["title"]})
                .aql_where(["int64($mt.id)%1500 = 0"])
                .aql_return({ "id" : "$mt.id", "location" : "$mt.loc", "comment" : "$mt.metacomment", "tweet" : "$mt.tweet" }) 
                .add_extra("tweetbook_title", extra_info["title"])
                .add_extra("marker_path", "../img/mobile_green2.png")
                .add_extra("on_click_marker", onClickTweetbookMapMarker)
                .add_extra("on_clean_result", onCleanPlotTweetbook)
                .api_core_query();
                
        });
    }
}

function onCleanPlotTweetbook(records) {
    var toPlot = [];
    for (var subrecords = 0; subrecords < records.length; subrecords++) {
        for (var record in records[subrecords]) {
            var tweetbook_element = {
                "tweetEntryId" : parseInt(records[subrecords][record].split(",")[0].split(":")[1].split('"')[1]),
                "tweetLat"     : parseFloat(records[subrecords][record].split("location\": point(\"")[1].split(",")[0]),
                "tweetLng"     : -1*parseFloat(records[subrecords][record].split("location\": point(\"")[1].split(",")[1].split("\"")[0]),
                "tweetText"    : records[subrecords][record].split("tweet\": \"")[1].split("\"")[0],
                "tweetbookComment" : records[subrecords][record].split("comment\": \"")[1].split("\", \"tweet\":")[0]
            };
            toPlot.push(tweetbook_element);
        }
    }
    return toPlot;
}

function onCleanTweetbookDrilldown (rec) {
    var drilldown_cleaned = [];
    for (var subresult = 0; subresult < rec.length; subresult++) {
        for (var entry in rec[subresult]) {
            
            var drill_element = {
                "tweetEntryId" : parseInt(rec[subresult][entry].split(",")[0].split(":")[1].split('"')[1]),
                "tweetText" : rec[subresult][entry].split("tweetText\": \"")[1].split("\", \"tweetLoc\":")[0],
                "tweetLat" : parseFloat(rec[subresult][entry].split("tweetLoc\": point(\"")[1].split(",")[0]),
                "tweetLng" : -1*parseFloat(rec[subresult][entry].split("tweetLoc\": point(\"")[1].split(",")[1].split("\"")[0])
            };
            drilldown_cleaned.push(drill_element);
            
        } 
    }
    return drilldown_cleaned;
}

function onClickTweetbookMapMarker(tweet_arr) {
    $('#drilldown_modal_body').html('');

    // Clear existing display
    $.each(tweet_arr, function (t, valueT) {
        var tweet_obj = tweet_arr[t];
        onDrillDownAtLocation(tweet_obj);
    });
    
    $('#drilldown_modal').modal('show');
}

/** Toggling Review and Explore Modes **/

/**
* Explore mode: Initial map creation and screen alignment
*/
function onOpenExploreMap () {
    var explore_column_height = $('#explore-well').height();   
    $('#map_canvas').height(explore_column_height + "px");
    $('#review-well').height(explore_column_height + "px");
    $('#review-well').css('max-height', explore_column_height + "px");
    var pad = $('#review-well').innerHeight() - $('#review-well').height();
    var prev_window_target = $('#review-well').height() - 20 - $('#group-tweetbooks').innerHeight() - $('#group-background-query').innerHeight() - 2*pad;
    $('#query-preview-window').height(prev_window_target +'px');
}

/**
* Launching explore mode: clear windows/variables, show correct sidebar
*/
function onLaunchExploreMode() {
    $('#review-active').removeClass('active');
    $('#review-well').hide();
    
    $('#explore-active').addClass('active');    
    $('#explore-well').show();
    
    $("#clear-button").trigger("click");
}

/**
* Launching review mode: clear windows/variables, show correct sidebar
*/
function onLaunchReviewMode() {
    $('#explore-active').removeClass('active');
    $('#explore-well').hide();
    $('#review-active').addClass('active');
    $('#review-well').show();
    
    $("#clear-button").trigger("click");
}

/** Map Widget Utility Methods **/

/**
* Plots a legend onto the map, with values in progress bars
* @param    {number Array}  breakpoints, an array of numbers representing natural breakpoints
*/
function mapControlWidgetAddLegend(breakpoints) {
   
    // Retriever colors, lightest to darkest
    var colors = mapWidgetGetColorPalette();
    
    // Initial div structure
    $("#map_canvas_legend").html('<div id="legend-holder"><div id="legend-progress-bar" class="progress"></div><span id="legend-label"></span></div>');

    // Add color scale to legend
    $('#legend-progress-bar').css("width", "200px").html('');
    
    // Add a progress bar for each color
    for (var color in colors) {
        
        // Bar values
        var upperBound = breakpoints[parseInt(color) + 1];
        
        // Create Progress Bar
        $('<div/>')
            .attr("class", "bar")
            .attr("id", "pbar" + color)
            .css("width" , '25.0%')
            .html("< " + upperBound)
            .appendTo('#legend-progress-bar');
        
        $('#pbar' + color).css({
            "background-image" : 'none',
            "background-color" : colors[parseInt(color)]
        });
        
        // Attach a message showing minimum bounds     
        $('#legend-label').html('Regions with at least ' + breakpoints[0] + ' tweets');
        $('#legend-label').css({
            "color" : "black"
        });
    }
    
    // Add legend to map
    map.controls[google.maps.ControlPosition.RIGHT_BOTTOM].push(document.getElementById('legend-holder'));
    $('#map_canvas_legend').show(); 
}

/**
* Clears map elements - legend, plotted items, overlays
*/
function mapWidgetClearMap() {

    if (selectionRect) {
        selectionRect.setMap(null);
        selectionRect = null;
    }
    for (c in map_cells) {
        map_cells[c].setMap(null);
    }
    map_cells = [];
    for (m in map_tweet_markers) {
        map_tweet_markers[m].setMap(null);
    }
    map_tweet_markers = [];
    
    // Remove legend from map
    map.controls[google.maps.ControlPosition.RIGHT_BOTTOM].clear();
}

/**
* Uses jenks algorithm in geostats library to find natural breaks in numeric data
* @param    {number Array} weights of points to plot
* @returns  {number Array} array of natural breakpoints, of which the top 4 subsets will be plotted
*/ 
function mapWidgetLegendComputeNaturalBreaks(weights) {
    var plotDataWeights = new geostats(weights.sort());
    return plotDataWeights.getJenks(6).slice(2, 7);
}

/**
* Computes values for map legend given a value and an array of jenks breakpoints
* @param    {number}        weight of point to plot on map
* @param    {number Array}  breakpoints, an array of 5 points corresponding to bounds of 4 natural ranges
* @returns  {String}        an RGB value corresponding to a subset of data
*/
function mapWidgetLegendGetHeatValue(weight, breakpoints) {

    // Determine into which range the weight falls
    var weightColor = 0;
    if (weight >= breakpoints[3]) {
        weightColor = 3;
    } else if (weight >= breakpoints[2]) {
        weightColor = 2;
    } else if (weight >= breakpoints[1]) {
        weightColor = 1;
    }

    // Get default map color palette
    var colorValues = mapWidgetGetColorPalette();
    return colorValues[weightColor];
}

/**
* Returns an array containing a 4-color palette, lightest to darkest
* External palette source: http://www.colourlovers.com/palette/2763366/s_i_l_e_n_c_e_r
* @returns  {Array}    [colors]
*/
function mapWidgetGetColorPalette() {
    return [ 
        "rgb(115,189,158)", 
        "rgb(74,142,145)", 
        "rgb(19,93,96)", 
        "rgb(7,51,46)"
    ];  
}

/**
* Computes radius for a given data point from a spatial cell
* @param    {Object}    keys => ["latSW" "lngSW" "latNE" "lngNE" "weight"]
* @returns  {number}    radius between 2 points in metres
*/
function mapWidgetComputeCircleRadius(spatialCell, breakpoints) {
    
    var weight = spatialCell.weight;
    // Compute weight color
    var weightColor = 0.25;
    if (weight >= breakpoints[3]) {
        weightColor = 1.0;
    } else if (weight >= breakpoints[2]) {
        weightColor = 0.75;
    } else if (weight >= breakpoints[1]) {
        weightColor = 0.5;
    }

    // Define Boundary Points
    var point_center = new google.maps.LatLng((spatialCell.latSW + spatialCell.latNE)/2.0, (spatialCell.lngSW + spatialCell.lngNE)/2.0);
    var point_left = new google.maps.LatLng((spatialCell.latSW + spatialCell.latNE)/2.0, spatialCell.lngSW);
    var point_top = new google.maps.LatLng(spatialCell.latNE, (spatialCell.lngSW + spatialCell.lngNE)/2.0);
    
    // TODO not actually a weight color :)
    return weightColor * 1000 * Math.min(distanceBetweenPoints_(point_center, point_left), distanceBetweenPoints_(point_center, point_top));
}

/** External Utility Methods **/

/**
 * Calculates the distance between two latlng locations in km.
 * @see http://www.movable-type.co.uk/scripts/latlong.html
 *
 * @param {google.maps.LatLng} p1 The first lat lng point.
 * @param {google.maps.LatLng} p2 The second lat lng point.
 * @return {number} The distance between the two points in km.
 * @private
*/
function distanceBetweenPoints_(p1, p2) {
  if (!p1 || !p2) {
    return 0;
  }

  var R = 6371; // Radius of the Earth in km
  var dLat = (p2.lat() - p1.lat()) * Math.PI / 180;
  var dLon = (p2.lng() - p1.lng()) * Math.PI / 180;
  var a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(p1.lat() * Math.PI / 180) * Math.cos(p2.lat() * Math.PI / 180) *
    Math.sin(dLon / 2) * Math.sin(dLon / 2);
  var c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  var d = R * c;
  return d;
};
