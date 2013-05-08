/** 
* Asterix Core API
* @returns  {AsterixCoreAPI}    AsterixCoreAPI
*/
function AsterixCoreAPI() {
    this.parameters = {
        "statements"    : [],
        "mode"          : "synchronous"
    };
    this.ui_callback_on_success = function() {};
    this.ui_callback_on_success_async = function() {};
    this.on_error = function() {};
    this.extra = {};
    return this;
}

/** Parameter Management **/

/**
* Sets the dataverse of this Asterix API call
* @param    {String}            dataverse
* @returns  {AsterixCoreAPI}    AsterixCoreAPI 
*/
AsterixCoreAPI.prototype.dataverse = function (dataverse) {
    this.dataverse = dataverse;
    return this;
}

/**
* Set the on-success callback of the api call
* @param    {Function}          on_success
* @param    {Boolean}           is_synchronous
* @returns  {AsterixCoreAPI}    AsterixCoreAPI 
*/
AsterixCoreAPI.prototype.success = function(on_success, is_synchronous) {
    if (is_synchronous) {
        this.ui_callback_on_success = on_success;
    } else {
        this.ui_callback_on_success_async = on_success;
    }
    
    return this;
}

/**
* Set the on-error callback of the api call
* @param    {Function}          on_error
* @returns  {AsterixCoreAPI}    AsterixCoreAPI 
*/
AsterixCoreAPI.prototype.error = function(on_error) {
    this.on_error = on_error;
    return this;
}

/**
* Sets a parameter key, value
* @param    {String}            param_key
* @param    {Object}            param_value
* @returns  {AsterixCoreAPI}    AsterixCoreAPI  
*/
AsterixCoreAPI.prototype.parameter = function (param_key, param_value) {
    this.parameters[param_key] = param_value;
    return this;
}

/**
* Sets query statements
* @param    {Array}             statements
* @returns  {AsterixCoreAPI}    this API object
*/
AsterixCoreAPI.prototype.statements = function (statements) {
    this.parameters["statements"] = statements;
    return this;
}

/**
* Pushes a single query statement
* @param    {String}            statement containing one line of a query
* @returns  {AsterixCoreAPI}    this API object
*/
AsterixCoreAPI.prototype.add_statement = function (statement) {
    this.parameters["statements"].push(statement);
    return this;
}

/**
* Sets synchronization mode
* @param    {String}            sync, either "synchronous" or "asynchronous"
* @returns  {AsterixCoreAPI}    this API object
*/
AsterixCoreAPI.prototype.mode = function(sync) {
    this.parameters["mode"] = sync;
    return this;
}

/**
* Extra data to run on success
* @param    {String}            extra_key
* @param    {Object}            extra_value
* @returns  {AsterixCoreAPI}    this API object
*/
AsterixCoreAPI.prototype.add_extra = function(extra_key, extra_value) {
    this.extra[extra_key] = extra_value;
    return this;
}

/**
* Create a dataset of a given type with a given key (optional)
* @param    {Object}            param: keys "dataset", "type", "primary_key", 
* @returns  {AsterixCoreAPI}    this API object
*/
AsterixCoreAPI.prototype.create_dataset = function(param) {
    // Parse create object
    var create_statement = 'create dataset ' + param["dataset"] + '(' + param["type"] + ')';
    if (param.hasOwnProperty("primary_key")) {
        create_statement += ' primary key ' + param["primary_key"] + ';';
    }
    
    // Add to statements
    this.parameters["statements"].push(create_statement);
    
    return this;
}

/**
* Drop a dataset of a given name
* @param    {String}            dataset
* @returns  {AsterixCoreAPI}    this API object
*/
AsterixCoreAPI.prototype.drop_dataset = function(dataset) {
    var drop_statement = 'drop dataset ' + dataset + ';';
    this.parameters["statements"].push(drop_statement);
    return this;
}

/**
* Reference a query given a query object
* @param    {Object}            async_handle, an opaque handle from a prior asynchronous query
* @returns  {AsterixCoreAPI}    this API object 
*/
AsterixCoreAPI.prototype.handle = function(async_handle) {
    this.query_handle = async_handle;
    return this;
}

/**
* @param    {Object}            for : {"var" in "dataset"}
* @returns  {AsterixCoreAPI}    this API object
*/
AsterixCoreAPI.prototype.aql_for = function(for_object) {
    var for_statement = "for $"; 
    for (var key in for_object) {
        for_statement += key + " in dataset " + for_object[key];
    }
    
    this.parameters["statements"].push(for_statement);
    return this;
}

/**
* @param    {Object}            let : {"key" => "values"}
* @returns  {AsterixCoreAPI}    this API object
*
* TODO This one needs work - hacky
*/
AsterixCoreAPI.prototype.aql_let = function(let_object) {
    var let_statement = "";
    
    for (var var_name in let_object) {
        let_statement = 'let $' + var_name + ' := ' + let_object[var_name];
        this.parameters["statements"].push(let_statement);
    }

    return this;
}

/**
* @param    {Array}             where_object : [statements]
* @returns  {AsterixCoreAPI}    this API object
*
* TODO Fix me
*/
AsterixCoreAPI.prototype.aql_where = function(where_object) {
    this.parameters["statements"].push("where " + where_object.join(" and "));
    return this;    
}

/**
* @param    {Object}            groupby, a map { "groupby" , "with" }
* @returns  {AsterixCoreAPI}    this API object
*/
AsterixCoreAPI.prototype.aql_groupby = function(groupby_object) {
    var groupby_statement = "group by " + groupby_object["groupby"];
    groupby_statement += " with " + groupby_object["with"];
    
    this.parameters["statements"].push(groupby_statement);
    
    return this;
}

/**
* Prepares a return statement of keys and value variables
* @param    {Object}            return { "keys" => "values" }
* @returns  {AsterixCoreAPI}    this API object
*/
AsterixCoreAPI.prototype.aql_return = function(return_object) {
    var components = [];
    for (var key in return_object) {
        components.push('"' + key + '" : ' + return_object[key]);
    }
    
    var return_statement = 'return { ' + components.join(', ') + ' }'; 
    this.parameters["statements"].push(return_statement);
    
    return this;
}

/** API Calls: Query API **/

/**
* Query
*
* @rest-parameter   {String, REQUIRED}  query, query string to pass to ASTERIX
* @rest-parameter   {String}            mode, whether a call is "synchronous" or "asynchronous"
* @rest-result      {Object}
*                       {
*                           @result     <result as a string, if mode = synchronous>
*                           @error-code [<code>, <message>] if an error occurs
*                           @handle:    <opaqure result handle, if mode = asynchronous>
*                       }              
* @endpoint         {Asterix REST API}  /query
*/
AsterixCoreAPI.prototype.api_core_query = function () {
    
    var api = this;
    var use_dataverse = "use dataverse " + api.dataverse + ";\n";
    var callbacks = {
        "sync" : api.ui_callback_on_success,
        "async" : api.ui_callback_on_success_async
    };
    var json = {
        "endpoint" : "http://localhost:19101/query",
        "query" : use_dataverse + api.parameters["statements"].join("\n"),
        "mode" : api.parameters["mode"]
    };

    api.api_helper_proxy_handler(json, callbacks, api.extra);
}

/**
* Query Status
*
* @rest-parameter   {Object, REQUIRED}  handle, opaque result handle from previous asynchronous query call
* @rest-result      {Object}
*                       {
*                           @status: ("RUNNING" | "SUCCESS" | "ERROR" )
*                       }
* @endpoint         {Asterix REST API}  /query/status
*/
AsterixCoreAPI.prototype.api_core_query_status = function () {

    var api = this;
    
    var use_dataverse = "use dataverse " + api.dataverse + ";\n";
    
    var callbacks = {
        "sync" : this.ui_callback_on_success
    };
    
    var json = {
        "endpoint"  : "http://localhost:19101/query/status",
        "handle"    : api.query_handle
    };
    
    api.api_helper_proxy_handler(json, callbacks, api.extra);
}

/**
* Query Result
*
* @rest-parameter   {Object, REQUIRED}  handle, opaque result handle from previous asynchronous query call
* @rest-parameter   {String}            mode, "synchronous" by default, "asynchronous" if not
* @rest-result      {Object}
*                       {
*                           @result     <result as a string, if mode = synchronous or asynchrous result available
*                                           {} if mode is asynchronous and results not available >
*                           @error-code [<code>, <message>] if an error occurs
*                       }
* @endpoint         {Asterix REST API}  /query/result
*/
AsterixCoreAPI.prototype.api_core_query_result = function () {
    
    var api = this;
    
    var use_dataverse = "use dataverse " + api.dataverse + ";\n";
    
    var callbacks = {
        "sync" : this.ui_callback_on_success
    };
    
    var json = {
        "endpoint" : "http://localhost:19101/query/result",
        "handle" : api.query_handle      
    };
    
    api.api_helper_proxy_handler(json, callbacks, api.extra);
}

/** API Calls - Update API **/

/**
* Update
*
* @rest-parameter 
*/
AsterixCoreAPI.prototype.api_core_update = function () {

    var api = this;
    
    var dataverse_statement = 'use dataverse ' + this.dataverse + '; ';
    
    var json = {
        "endpoint" : "http://localhost:19101/update",
        "statements" : dataverse_statement + this.parameters["statements"].join(" ")
    };
    
   $.ajax({
        type : 'POST',
        url: "ajaxFacadeCherry.php",
        data: json,
        dataType: "json",
        success: function(data) {

            // Update API special case
            if (data == "") {
                api.ui_callback_on_success(api.extra);
                
            } else {
                var response = $.parseJSON(data[0]);
                
                if (response && response["error-code"]) {
                
                    api.api_helper_default_on_error(response["error-code"][0], response["error-code"][1]);
                
                } else if (response && response["results"]) {
                
                    alert("Response: " + response["results"]);
                
                } else if (!response) {
                
                    api.api_helper_default_on_error(4, "Update API Call Error");
                
                }
            }
        }
    
    });
}

/** API Calls - DDL API **/
    
/**
* DDL
*
* @rest-parameter   {Object, REQUIRED}  DDL
* @rest-result      {Object}
*                       {
*                           @result:    <array of results, one per DDL statement in input>
*                       }
* @endpoint         {Asterix REST API}  /ddl
*/
AsterixCoreAPI.prototype.api_core_ddl = function () {
    var api = this;
    
    var dataverse_statement = 'use dataverse ' + this.dataverse + '; ';
    
    var json = {
        "endpoint" : "http://localhost:19101/ddl",
        "ddl" : dataverse_statement + this.parameters["statements"].join(" ")
    };
    
    var callback = {
        "sync" : api.ui_callback_on_success
    };
    
    api.api_helper_proxy_handler(json, callback, api.extra);
}

/** API Utilities **/

/**
* API Helper - Proxy Handler [TEMPORARY]
*
* @param    {Object}    json, the JSON object containing the parameters and endpoint for this API call
* @param    {Objct}     callback, the on-success callbacks for this handler
*               {
*                   "sync" : default callback
*                   "async" : non-default callback
*               }
* @param    {Object}    extra, any extra stuff passed from UI [TEMPORARY]
*/
AsterixCoreAPI.prototype.api_helper_proxy_handler = function(json, callback, extra) {
    
    /*var callbacks = {
        "sync" : api.ui_callback_on_success,
        "async" : api.ui_callback_on_success_async
    };
    var json = {
        "endpoint" : "http://localhost:19101/query",
        "query" : use_dataverse + api.parameters["statements"].join("\n"),
        "mode" : api.parameters["mode"]
    };*/
    var as = new AsterixSDK();
    api = this;

    var branch = function(response) {
        
        if (response && response["error-code"]) {
            
            api.api_helper_default_on_error( response["error-code"][0], response["error-code"][1] );     
            
        } else if (response && response["results"]) {
            var fn_callback = callback["sync"];
            fn_callback(response, extra);
            
        } else if (response["handle"]) {
            
            var fn_callback = callback["async"];
            fn_callback(response, extra);
            
        } else if (response["status"]) {
                
            var fn_callback = callback["sync"];
            fn_callback(response, extra);
        }
    };

    var c = {
        "onSend" : function() {
            return {
                "endpoint" : json["endpoint"],
                "apiData" : {
                    "query" : json["query"],
                    "mode" : json["mode"]
                },
                "callback" : branch
            };
        }
    };
    as.send(c);
}

/**
* API Helper - Error Handler
* Currently does an alert, but a more graceful notification would be better.
* 
* @param    {Number}    error_code, ( 1 | 2 | 99 ) denoting type of error
* @param    {String}    error_message, an informative message about the error
*/
AsterixCoreAPI.prototype.api_helper_default_on_error = function (error_code, error_message) {
    alert("ERROR " + error_code + ": " + error_message);       
}

/**
* API Helper - Spatial/Polygon Creator
* 
* @param    {Object}    bounds, {"ne" => { "lat", "lng" }, "sw" => {"lat", "lng"} }
* @returns  {String}    query string form of a polygon
*
* TODO this is kind of hacky :/
*/
AsterixCoreAPI.prototype.rectangle = function(bounds) {
   var lower_left = 'create-point(' + bounds["sw"]["lat"] + ',' + bounds["sw"]["lng"] + ')';
   var upper_right = 'create-point(' + bounds["ne"]["lat"] + ',' + bounds["ne"]["lng"] + ')';

   var rectangle_statement = 'create-rectangle(' + lower_left + ', ' + upper_right + ')';
   this.parameters["statements"].push(rectangle_statement);
    
   return this;
}

AsterixCoreAPI.prototype.api_helper_polygon_to_statement = function(bounds) {
    var polygon = [];
    polygon.push([bounds["ne"]["lat"] + "," + bounds["sw"]["lng"]]);
    polygon.push([bounds["sw"]["lat"] + "," + bounds["sw"]["lng"]]);
    polygon.push([bounds["sw"]["lat"] + "," + bounds["ne"]["lng"]]);
    polygon.push([bounds["ne"]["lat"] + "," + bounds["ne"]["lng"]]);
    
    var polygon_statement = 'polygon("' + polygon.join(" ") + '")';
    return polygon_statement;
}
