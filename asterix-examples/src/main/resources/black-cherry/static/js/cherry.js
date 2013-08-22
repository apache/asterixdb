$(function() {	
	
	// Connection to AsterixDB - Just one needed!   
	A = new AsterixDBConnection().dataverse("twitter");
	
    // Following this is some stuff specific to the Black Cherry demo
    // This is not necessary for working with AsterixDB
    APIqueryTracker = {};
    drilldown_data_map = {};
    drilldown_data_map_vals = {};
    asyncQueryManager = {};
    
    review_mode_tweetbooks = [];
    review_mode_handles = [];
    
    map_cells = [];
    map_tweet_markers = [];
    
    // UI Elements - Modals & perspective tabs
    $('#drilldown_modal').modal('hide');
    $('#explore-mode').click( onLaunchExploreMode );
    $('#review-mode').click( onLaunchReviewMode );
   
    // UI Elements - A button to clear current map and query data
    $("#clear-button").button().click(function () {
        mapWidgetResetMap();
        
        $('#query-preview-window').html('');
        $("#metatweetzone").html('');
    });
    
    // UI Elements - Query setup
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
    
    // UI Element - Grid sliders
    var updateSliderDisplay = function(event, ui) {
        if (event.target.id == "grid-lat-slider") {
            $("#gridlat").text(""+ui.value);
        } else {
          $("#gridlng").text(""+ui.value);
        }
    };
    
    sliderOptions = {
        max: 10,
        min: 1.5,
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
    
    // UI Elements - Date Pickers
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
    
    // UI Elements - Creates map and location auto-complete
    onOpenExploreMap();
    var mapOptions = {
        center: new google.maps.LatLng(38.89, -77.03),
        zoom: 4,
        mapTypeId: google.maps.MapTypeId.ROADMAP,
        streetViewControl: false,
        draggable : false
    };
    map = new google.maps.Map(document.getElementById('map_canvas'), mapOptions);

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
    
    // UI Elements - Selection Rectangle Drawing
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
    
    // UI Elements - Toggle location search style by location or by map selection
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
    
    // UI Elements - Tweetbook Management
    $('.dropdown-menu a.holdmenu').click(function(e) {
        e.stopPropagation();
    });
    
    $('#new-tweetbook-button').on('click', function (e) {
        onCreateNewTweetBook($('#new-tweetbook-entry').val());
        
        $('#new-tweetbook-entry').val($('#new-tweetbook-entry').attr('placeholder'));
    });
     
    // UI Element - Query Submission
    $("#submit-button").button().click(function () {
    	// Clear current map on trigger
 
        $("#submit-button").attr("disabled", true);
    	
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

		var build_cherry_mode = "synchronous";
		
		if ($('#asbox').is(":checked")) {
		    build_cherry_mode = "asynchronous";
		}
	
        var f = buildAQLQueryFromForm(formData);
        
        if (build_cherry_mode == "synchronous") {
            A.query(f.val(), cherryQuerySyncCallback, build_cherry_mode);
        } else {
            A.query(f.val(), cherryQueryAsyncCallback, build_cherry_mode);
        }
    
		APIqueryTracker = {
		    "query" : "use dataverse twitter;\n" + f.val(),
		    "data" : formData
		};
		
		$('#dialog').html(APIqueryTracker["query"]);

        if (!$('#asbox').is(":checked")) {
		    $('#show-query-button').attr("disabled", false);
        } else {
            $('#show-query-button').attr("disabled", true);
        }
    });
});


function buildAQLQueryFromForm(parameters) {

    var bounds = {
        "ne" : { "lat" : parameters["neLat"], "lng" : -1*parameters["neLng"]}, 
		"sw" : { "lat" : parameters["swLat"], "lng" : -1*parameters["swLng"]}
    };
    
    var rectangle = 
        new FunctionExpression("create-rectangle",
            new FunctionExpression("create-point", bounds["sw"]["lat"], bounds["sw"]["lng"]),
            new FunctionExpression("create-point", bounds["ne"]["lat"], bounds["ne"]["lng"]));
        

    var aql = new FLWOGRExpression()
        .ForClause("$t", new AExpression("dataset TweetMessagesShifted"))
        .LetClause("$keyword", new AExpression('"' + parameters["keyword"] + '"'))
        .LetClause("$region", rectangle)
        .WhereClause().and(
            new FunctionExpression("spatial-intersect", "$t.sender-location", "$region"),
            new AExpression('$t.send-time > datetime("' + parameters["startdt"] + '")'),
            new AExpression('$t.send-time < datetime("' + parameters["enddt"] + '")'),
            new FunctionExpression("contains", "$t.message-text", "$keyword")
        )
        .GroupClause(
            "$c",
            new FunctionExpression("spatial-cell", "$t.sender-location", 
                new FunctionExpression("create-point", "24.5", "-125.5"), 
                parameters["gridlat"].toFixed(1), parameters["gridlng"].toFixed(1)),
            "with", 
                "$t"
        )
        .ReturnClause({ "cell" : "$c", "count" : "count($t)" });     

    return aql;
}

/** Asynchronous Query Management **/


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
* Retrieves status of an asynchronous query, using an opaque result handle from API
* @param    {Object}    handle, an object previously returned from an async call
* @param    {number}    handle_id, the integer ID parsed from the handle object
*/
function asynchronousQueryGetAPIQueryStatus (handle, handle_id) {

    A.query_status( 
        {
            "handle" : JSON.stringify(handle)
        },
        function (res) {
            if (res["status"] == "SUCCESS") {
                // We don't need to check if this one is ready again, it's not going anywhere...
                // Unless the life cycle of handles has changed drastically
                asyncQueryManager[handle_id]["ready"] = true;
            
                // Indicate success. 
                $('#handle_' + handle_id).removeClass("btn-disabled").prop('disabled', false).addClass("btn-success");
            }
        }    
     );
}


/**
* On-success callback after async API query
* @param    {object}    res, a result object containing an opaque result handle to Asterix
*/
function cherryQueryAsyncCallback(res) {
    
    // Parse handle, handle id and query from async call result
    var handle_query = APIqueryTracker["query"];
    var handle = res;
    var handle_id = res["handle"].toString().split(',')[0]; 
    
    // Add to stored map of existing handles
    asyncQueryManager[handle_id] = {
        "handle" : handle,
        "query" : handle_query,
        "data" : APIqueryTracker["data"]
    };
    
    // Create a container for this async query handle    
    $('<div/>')
        .css("margin-left", "1em")
        .css("margin-bottom", "1em")
        .css("display", "block")
        .attr({
            "class" : "btn-group",
            "id" : "async_container_" + handle_id
        })
        .appendTo("#async-handle-controls");
    
    // Adds the main button for this async handle
    var handle_action_button = '<button class="btn btn-disabled" id="handle_' + handle_id + '">Handle ' + handle_id + '</button>';
    $('#async_container_' + handle_id).append(handle_action_button);
    $('#handle_' + handle_id).prop('disabled', true);
    $('#handle_' + handle_id).on('click', function (e) {

        // make sure query is ready to be run
        if (asyncQueryManager[handle_id]["ready"]) {
        
            APIqueryTracker = {
                "query" : asyncQueryManager[handle_id]["query"],
                "data"  : asyncQueryManager[handle_id]["data"]
            };
            $('#dialog').html(APIqueryTracker["query"]);
        
            // Generate new Asterix Core API Query
            A.query_result(
                { "handle" : JSON.stringify(asyncQueryManager[handle_id]["handle"]) },
                cherryQuerySyncCallback
            );
        }
    });
    
    // Adds a removal button for this async handle
    var asyncDeleteButton = addDeleteButton(
        "trashhandle_" + handle_id,
        "async_container_" + handle_id,
        function (e) {
            $('#async_container_' + handle_id).remove();
            delete asyncQueryManager[handle_id];
        }
    );
    $("#submit-button").attr("disabled", false);
}


/**
* returns a json object with keys: weight, latSW, lngSW, latNE, lngNE
*
* { "cell": { rectangle: [{ point: [22.5, 64.5]}, { point: [24.5, 66.5]}]}, "count": { int64: 5 }}
*/
function getRecord(cell_count_record) {
    // This is a really hacky way to pull out the digits, but it works for now. 
    var values = cell_count_record.replace("int64","").match(/[-+]?[0-9]*\.?[0-9]+/g);
    var record_representation = {};
    
    record_representation["latSW"] = parseFloat(values[0]);
    record_representation["lngSW"] = parseFloat(values[1]);
    record_representation["latNE"] = parseFloat(values[2]);
    record_representation["lngNE"] = parseFloat(values[3]);
    record_representation["weight"] = parseInt(values[4]);
    
    return record_representation;
}

/**
* A spatial data cleaning and mapping call
* @param    {Object}    res, a result object from a cherry geospatial query
*/
function cherryQuerySyncCallback(res) {
    
    records = res["results"];
    
    if (typeof res["results"][0] == "object") {
        records = res["results"][0];
    }

    var coordinates = [];
    var weights = [];
                
    for (var subrecord in records) {
        var coordinate = getRecord(records[subrecord]);
        weights.push(coordinate["weight"]);
        coordinates.push(coordinate);
    }
    
    triggerUIUpdate(coordinates, weights);
    $("#submit-button").attr("disabled", false);
}

/**
* Triggers a map update based on a set of spatial query result cells
* @param    [Array]     mapPlotData, an array of coordinate and weight objects
* @param    [Array]     params, an object containing original query parameters [LEGACY]
* @param    [Array]     plotWeights, a list of weights of the spatial cells - e.g., number of tweets
*/
function triggerUIUpdate(mapPlotData, plotWeights) {
    /** Clear anything currently on the map **/
    mapWidgetClearMap();
    
    // Compute data point spread
    var dataBreakpoints = mapWidgetLegendComputeNaturalBreaks(plotWeights);
     
    $.each(mapPlotData, function (m, val) {
    
        // Only map points in data range of top 4 natural breaks
        if (mapPlotData[m].weight > dataBreakpoints[0]) {
        
            // Get color value of legend 
            var mapColor = mapWidgetLegendGetHeatValue(mapPlotData[m].weight, dataBreakpoints);
            var markerRadius = mapWidgetComputeCircleRadius(mapPlotData[m], dataBreakpoints);
            var point_opacity = 1.0;
           
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
    var zoneData = APIqueryTracker["data"];
    
    var zswBounds = new google.maps.LatLng(marker_borders.latSW, marker_borders.lngSW);
    var zneBounds = new google.maps.LatLng(marker_borders.latNE, marker_borders.lngNE);
    
    var zoneBounds = new google.maps.LatLngBounds(zswBounds, zneBounds);
    zoneData["swLat"] = zoneBounds.getSouthWest().lat();
    zoneData["swLng"] = zoneBounds.getSouthWest().lng();
    zoneData["neLat"] = zoneBounds.getNorthEast().lat();
    zoneData["neLng"] = zoneBounds.getNorthEast().lng();
    var zB = {
        "sw" : {
            "lat" : zoneBounds.getSouthWest().lat(),
            "lng" : zoneBounds.getSouthWest().lng()
        },
        "ne" : {
            "lat" : zoneBounds.getNorthEast().lat(),
            "lng" : zoneBounds.getNorthEast().lng()
        }
    };
    
    mapWidgetClearMap();
    
    var customBounds = new google.maps.LatLngBounds();
    var zoomSWBounds = new google.maps.LatLng(zoneData["swLat"], zoneData["swLng"]);
    var zoomNEBounds = new google.maps.LatLng(zoneData["neLat"], zoneData["neLng"]); 
    customBounds.extend(zoomSWBounds);
    customBounds.extend(zoomNEBounds);
    map.fitBounds(customBounds);
  
    var df = getDrillDownQuery(zoneData, zB);

    APIqueryTracker = {
        "query_string" : "use dataverse twitter;\n" + df.val(),
        "marker_path" : "static/img/mobile2.png",
        "on_clean_result" : onCleanTweetbookDrilldown,
    };
        
    A.query(df.val(), onTweetbookQuerySuccessPlot);
}

function getDrillDownQuery(parameters, bounds) {

    var zoomRectangle = new FunctionExpression("create-rectangle",
        new FunctionExpression("create-point", bounds["sw"]["lat"], bounds["sw"]["lng"]),
        new FunctionExpression("create-point", bounds["ne"]["lat"], bounds["ne"]["lng"]));
        
    var drillDown = new FLWOGRExpression()
        .ForClause("$t", new AExpression("dataset TweetMessagesShifted"))
        .LetClause("$keyword", new AExpression('"' + parameters["keyword"] + '"'))
        .LetClause("$region", zoomRectangle)
        .WhereClause().and(
            new FunctionExpression('spatial-intersect', '$t.sender-location', '$region'),
            new AExpression().set('$t.send-time > datetime("' + parameters["startdt"] + '")'),
            new AExpression().set('$t.send-time < datetime("' + parameters["enddt"] + '")'),
            new FunctionExpression('contains', '$t.message-text', '$keyword')
        )
        .ReturnClause({
            "tweetId" : "$t.tweetid", 
            "tweetText" : "$t.message-text",
            "tweetLoc" : "$t.sender-location"
        });
        
    return drillDown;
}


function addTweetbookCommentDropdown(appendToDiv) {

    // Creates a div to manage a radio button set of chosen tweetbooks
    $('<div/>')
        .attr("class","btn-group chosen-tweetbooks")
        .attr("data-toggle", "buttons-radio")
        .css("margin-bottom", "10px")
        .attr("id", "metacomment-tweetbooks")
        .appendTo(appendToDiv); 
    
    var highlighted = "";
    if (APIqueryTracker.hasOwnProperty("active_tweetbook")) {
        highlighted = APIqueryTracker["active_tweetbook"];
    }
    
    // For each existing tweetbook from review mode, adds a radio button option. 
    $('#metacomment-tweetbooks').append('<input type="hidden" id="target-tweetbook" value="" />');
    for (var rmt in review_mode_tweetbooks) {
    
        var tweetbook_option = '<button type="button" class="btn">' + review_mode_tweetbooks[rmt] + '</button>';
                
        if (review_mode_tweetbooks[rmt] == highlighted) {
            tweetbook_option = '<button type="button" class="btn btn-info">' + review_mode_tweetbooks[rmt] + '</button>';   
        }  
                
        $('#metacomment-tweetbooks').append(tweetbook_option + '<br/>');
    }
    
    // Creates a button + input combination to add tweet comment to new tweetbook
    var new_tweetbook_option = '<button type="button" class="btn" id="new-tweetbook-target-m"></button>' + 
        '<input type="text" id="new-tweetbook-entry-m" placeholder="Add to new tweetbook..."><br/>';
    $('#metacomment-tweetbooks').append(new_tweetbook_option);
    
    $("#new-tweetbook-entry-m").keyup(function() {
        $("#new-tweetbook-target-m").val($("#new-tweetbook-entry-m").val());
        $("#new-tweetbook-target-m").text($("#new-tweetbook-entry-m").val());
    });
    
    // There is a hidden input (id = target-tweetbook) which is used to track the value
    // of the tweetbook to which the comment on this tweet will be added.
    $(".chosen-tweetbooks .btn").click(function() {
        $("#target-tweetbook").val($(this).text()); 
    });
}

function onDrillDownAtLocation(tO) {

    var tweetId = tO["tweetEntryId"];
    var tweetText = tO["tweetText"];
    
    var tweetContainerId = '#drilldown_modal_body';
    var tweetDiv = '<div id="drilltweetobj' + tweetId + '"></div>';
    
    $(tweetContainerId).empty();
    $(tweetContainerId).append(tweetDiv);
    $('#drilltweetobj' + tweetId).append('<p>Tweet #' + tweetId + ": " + tweetText + '</p>');
   
    // Add comment field
    $('#drilltweetobj' + tweetId).append('<input class="textbox" type="text" id="metacomment' + tweetId + '">');
    
    if (tO.hasOwnProperty("tweetComment")) {
        $("#metacomment" + tweetId).val(tO["tweetComment"]);
        
        var deleteThisComment = addDeleteButton(
            "deleteLiveComment_" + tweetId,
            "drilltweetobj" + tweetId,
            function () {
               
                // TODO Maybe this should fire differnetly if another tweetbook is selected?
               
                // Send comment deletion to asterix 
                var deleteTweetCommentOnId = '"' + tweetId + '"';
                var toDelete = new DeleteStatement(
                    "$mt",
                    APIqueryTracker["active_tweetbook"],
                    new AExpression("$mt.tweetid = " + deleteTweetCommentOnId.toString())
                );
                A.update(
                    toDelete.val()
                );
                
                // Hide comment from map
                $('#drilldown_modal').modal('hide');
                
                // Replot tweetbook
                onPlotTweetbook(APIqueryTracker["active_tweetbook"]);
            }
        );
    }
     
    addTweetbookCommentDropdown('#drilltweetobj' + tweetId);
    
    $('#drilltweetobj' + tweetId).append('<br/><button type="button" class="btn" id="add-metacomment">Save Comment</button>');
    
    $('#add-metacomment').button().click(function () {
        var save_metacomment_target_tweetbook = $("#target-tweetbook").val();
        var save_metacomment_target_comment = '"' + $("#metacomment" + tweetId).val() + '"';
        var save_metacomment_target_tweet = '"' + tweetId + '"';
    
        if (save_metacomment_target_tweetbook.length == 0) {
            alert("Please choose a tweetbook.");
            
        } else {
        
            if (!(existsTweetbook(save_metacomment_target_tweetbook))) {
                onCreateNewTweetBook(save_metacomment_target_tweetbook);
            }
            
            var toDelete = new DeleteStatement(
                "$mt",
                save_metacomment_target_tweetbook,
                new AExpression("$mt.tweetid = " + save_metacomment_target_tweet.toString())
            );
            
            A.update(toDelete.val());
            
            var toInsert = new InsertStatement(
                save_metacomment_target_tweetbook,
                { 
                    "tweetid" : save_metacomment_target_tweet.toString(), 
                    "comment-text" : save_metacomment_target_comment 
                }
            );
            
            // Insert query to add metacomment to said tweetbook dataset
            A.update(toInsert.val(), function () { alert("Test"); });
            
            // TODO Some stress testing of error conditions might be good here...
            onPlotTweetbook(APIqueryTracker["active_tweetbook"]);
            var successMessage = "Saved comment on <b>Tweet #" + tweetId + 
                "</b> in dataset <b>" + save_metacomment_target_tweetbook + "</b>.";
            addSuccessBlock(successMessage, 'drilltweetobj' + tweetId);
        }
    });
    
    // Set width of tweetbook buttons
    $(".chosen-tweetbooks .btn").css("width", "200px");
    $(".chosen-tweetbooks .btn").css("height", "2em");
}


/**
* Adds a new tweetbook entry to the menu and creates a dataset of type TweetbookEntry.
*/
function onCreateNewTweetBook(tweetbook_title) {
    
    var tweetbook_title = tweetbook_title.split(' ').join('_');

    A.ddl(
        "create dataset " + tweetbook_title + "(TweetbookEntry) primary key tweetid;",
        function () {}
    );
    
    if (!(existsTweetbook(tweetbook_title))) {
        review_mode_tweetbooks.push(tweetbook_title);
        addTweetBookDropdownItem(tweetbook_title);
    }
}


function onDropTweetBook(tweetbook_title) {

    // AQL Call
    A.ddl(
        "drop dataset " + tweetbook_title + " if exists;",
        function () {}
    );
    
    // Removes tweetbook from review_mode_tweetbooks
    var remove_position = $.inArray(tweetbook_title, review_mode_tweetbooks);
    if (remove_position >= 0) review_mode_tweetbooks.splice(remove_position, 1);
    
    // Clear UI with review tweetbook titles
    $('#review-tweetbook-titles').html('');
    for (r in review_mode_tweetbooks) {
        addTweetBookDropdownItem(review_mode_tweetbooks[r]);
    }
}


function addTweetBookDropdownItem(tweetbook) {
    // Add placeholder for this tweetbook    
    $('<div/>')
        .css("padding-left", "1em")
        .attr({
            "class" : "btn-group",
            "id" : "rm_holder_" + tweetbook
        }).appendTo("#review-tweetbook-titles");
    $("#review-tweetbook-titles").append('<br/>');
    
    // Add plotting button for this tweetbook
    var plot_button = '<button class="btn" id="rm_plotbook_' + tweetbook + '">' + tweetbook + '</button>';
    $("#rm_holder_" + tweetbook).append(plot_button);
    $("#rm_plotbook_" + tweetbook).on('click', function(e) {
        onPlotTweetbook(tweetbook);
    });
    
    // Add trash button for this tweetbook
    var onTrashTweetbookButton = addDeleteButton(
        "rm_trashbook_" + tweetbook,
        "rm_holder_" + tweetbook,
        function(e) {
            onDropTweetBook(tweetbook);
        }
    );
}


function onPlotTweetbook(tweetbook) {
    
    // Clear map for this one
    mapWidgetResetMap();

    var plotTweetQuery = new FLWOGRExpression()
        .ForClause("$t", new AExpression("dataset TweetMessagesShifted"))
        .ForClause("$m", new AExpression("dataset " + tweetbook))
        .WhereClause(new AExpression("$m.tweetid = $t.tweetid"))
        .ReturnClause({
            "tweetId" : "$m.tweetid",
            "tweetText" : "$t.message-text",
            "tweetLoc" : "$t.sender-location",
            "tweetCom" : "$m.comment-text"
        });
          
    APIqueryTracker = {
        "query_string" : "use dataverse twitter;\n" + plotTweetQuery.val(),
        "marker_path" : "static/img/mobile_green2.png",
        "on_clean_result" : onCleanPlotTweetbook,
        "active_tweetbook" : tweetbook
    };
        
    A.query(plotTweetQuery.val(), onTweetbookQuerySuccessPlot);     
}


function onTweetbookQuerySuccessPlot (res) {

    var records = res["results"];
    
    var coordinates = [];
    map_tweet_markers = [];  
    map_tweet_overlays = [];
    drilldown_data_map = {};
    drilldown_data_map_vals = {};
    
    var micon = APIqueryTracker["marker_path"];
    var marker_click_function = onClickTweetbookMapMarker;
    var clean_result_function = APIqueryTracker["on_clean_result"];
    
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


function existsTweetbook(tweetbook) {
    if (parseInt($.inArray(tweetbook, review_mode_tweetbooks)) == -1) {
        return false;
    } else {
        return true;
    }
}


function onCleanPlotTweetbook(records) {
    var toPlot = [];

    // An entry looks like this:
    // { "tweetId": "273589", "tweetText": " like verizon the network is amazing", "tweetLoc": { point: [37.78, 82.27]}, "tweetCom": "hooray comments" }
    
    for (var entry in records) {
    
        var points = records[entry].split("point:")[1].match(/[-+]?[0-9]*\.?[0-9]+/g);
        
        var tweetbook_element = {
            "tweetEntryId"  : parseInt(records[entry].split(",")[0].split(":")[1].split('"')[1]),
            "tweetText"     : records[entry].split("tweetText\": \"")[1].split("\", \"tweetLoc\":")[0],
            "tweetLat"      : parseFloat(points[0]),
            "tweetLng"      : parseFloat(points[1]),
            "tweetComment"  : records[entry].split("tweetCom\": \"")[1].split("\"")[0]
        };
        toPlot.push(tweetbook_element);
    }
    
    return toPlot;
}


function onCleanTweetbookDrilldown (rec) {

    var drilldown_cleaned = [];
    
    for (var entry = 0; entry < rec.length; entry++) {
   
        // An entry looks like this:
        // { "tweetId": "105491", "tweetText": " hate verizon its platform is OMG", "tweetLoc": { point: [30.55, 71.44]} }
        var points = rec[entry].split("point:")[1].match(/[-+]?[0-9]*\.?[0-9]+/g);
        
        var drill_element = {
            "tweetEntryId" : parseInt(rec[entry].split(",")[0].split(":")[1].replace('"', '')),
            "tweetText" : rec[entry].split("tweetText\": \"")[1].split("\", \"tweetLoc\":")[0],
            "tweetLat" : parseFloat(points[0]),
            "tweetLng" : parseFloat(points[1])
        };
        drilldown_cleaned.push(drill_element);
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


/** Icon / Interface Utility Methods **/

/** 
* Creates a delete icon button using default trash icon
* @param    {String}    id, id for this element
* @param    {String}    attachTo, id string of an element to which I can attach this button.
* @param    {Function}  onClick, a function to fire when this icon is clicked
*/
function addDeleteButton(iconId, attachTo, onClick) {
    // Icon structure
    var trashIcon = '<button class="btn" id="' + iconId + '"><i class="icon-trash"></i></button>';
    
    $('#' + attachTo).append(trashIcon);
    
    // On Click behavior
    $('#' + iconId).on('click', onClick);
}


/**
* Creates a success message and attaches it to a div with provided ID.
* @param    {String}    message, a message to post
* @param    {String}    appendTarget, a target div to which to append the alert
*/
function addSuccessBlock(message, appendTarget) {

    $('<div/>')
        .attr("class", "alert alert-success")
        .html('<button type="button" class="close" data-dismiss="alert">&times;</button>' + message)
        .appendTo('#' + appendTarget);
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
            "margin-top" : 0,
            "color" : "black"
        });
    }
    
    // Add legend to map
    map.controls[google.maps.ControlPosition.LEFT_BOTTOM].push(document.getElementById('legend-holder'));
    $('#map_canvas_legend').show(); 
}

/**
* Clears ALL map elements - legend, plotted items, overlays
*/
function mapWidgetResetMap() {

    if (selectionRect) {
        selectionRect.setMap(null);
        selectionRect = null;
    }
    
    mapWidgetClearMap();
    
    // Reset map center and zoom
    map.setCenter(new google.maps.LatLng(38.89, -77.03));
    map.setZoom(4);
}

function mapWidgetClearMap() {

    // Remove previously plotted data/markers
    for (c in map_cells) {
        map_cells[c].setMap(null);
    }
    map_cells = [];
    for (m in map_tweet_markers) {
        map_tweet_markers[m].setMap(null);
    }
    map_tweet_markers = [];

    // Remove legend from map
    map.controls[google.maps.ControlPosition.LEFT_BOTTOM].clear();
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
