// AsterixSDK 
// Core Object for REST API communication
// Handles callbacks to client applications, communication to REST API, and 
// endpoint selection. Initializes an RPC consumer object.
// 
// Usage:
// var a = new AsterixSDK();
function AsterixSDK() {
    
    // Asterix SDK request handler initialization
    // TODO Depending on configuration, may need multiples of these...
    this.xhr = new easyXDM.Rpc({
        remote: "http://localhost:19101/sdk/static/client.html"
    }, {
        remote: {
            post: {}
        }
    })
    
    // Asterix SDK => send
    // Posts a message containing an API endpoint, json data,
    // and a UI callback function.
    //
    // @param handler [Asterix REST Controller], a handler object
    // that provides REST request information. 
    //
    // Anticipated Usage:
    //
    // var a = AsterixSDK();
    // var e = Expression;
    // var h = AsterixRestController.bindExpression(e);
    // a.send(h);
    this.send = function(handler) {
        var api = handler.onSend();
        this.xhr.post(
            api["endpoint"],
            api["apiData"],
            api["callback"]
        );
    };

    // Asterix SDK => requestHandler
    // Handlers remote requests to Asterix REST API
    // using the easyXDM RPC protocol
    // Usually should only be called by client side, could be overridden
    // TODO Get rid of jQuery ajax notation in favor of xmlhttprequest pure js
    this.requestHandler = function() {
        var rpc = new easyXDM.Rpc({}, {
            local: {
                post: {
                    method: function(url, data, fn, fnError){
                        $.ajax({
                            type : 'GET',
                            url : url,
                            data : data,
                            dataType : "json",
                            success : function(res) {
                                fn(res);
                            }
                        });
                    }
                }
            }
        });
    }
}

// Asterix REST Controller
//
// Asterix REST Controller
// Handles interactions with remote database using a request handler
function AsterixRESTController() {
   
    // bindExpression
    // Prepares an expression for query the asterix API
    // TODO: Future expressions will do some kind of parsing on queries, for now
    // a keyword is used to return the appropriate handler
    // keywords: ddl, update, query, query_status, query_result
    //
    // @param expression [AsterixExpression], an expression or one of its
    // subclasses
    // @param handler [AsterixRESTController], a handler to pass to the SDK
    // remote method call
    this.bindExpression = function(expression, handler_type) {
        
        // TODO Expression handler

        // Parse handler, to be replaced with something more elegant
        switch (handler_type)
        {
            case "ddl":
                break;
            case "update":
                break;
            case "query":
                break;
            case "query_status":
                break;
            case "query_result":
                break;
            default:
                this.onHandlerInitError();
        }
    };

    // onHandlerInitError
    // Method for handling expression binding errors in the controller.
    this.onHandlerInitError = function() {
        alert("AsterixSDK / AsterixRESTController / bindExpressionError: Could not determine api endpoint, incorrect keyword");
    }

    // REST DDL API
    this.DDL = function(data, callback) {
        this.endpoint = "http://localhost:19101/ddl";
        this.apiData = data;
        this.callback = callback;
    };

}

AsterixRESTController.prototype.Update = function(json, data, callback) {
    var endpoint = "http://localhost:19101/update";
}


AsterixRESTController.prototype.Query = function(json, data, callback) {
    var endpoint = "http://localhost:19101/query";
}


AsterixRESTController.prototype.QueryStatus = function(json, data, callback) {
    var endpoint = "http://localhost:19101/query/status";
}


AsterixRESTController.prototype.QueryResult = function(json, data, callback) {
    var endpoint = "http://localhost:19101/query/result";
}




function AsterixSDKJQueryHandler(json, endpoint, callback) {
    $.ajax({
        type : 'GET',
        url : endpoint,
        data : json,
        dataType : "json",
        success : function(data) {
            alert(JSON.stringify(data));
        }
    });
}


function AsterixExpression() {}


// Create expressions
// Create types, dataverses, indexes
CreateExpression.prototype = new AsterixExpression();
CreateExpression.prototype.constructor = CreateExpression;
function CreateExpression(statement) {
    this.value = statement;
    this.send = this.onSend;
}
CreateExpression.prototype.onSend = function () {
    var arc = new AsterixRESTController();
    myThis = this;
    arc.DDL( 
        { "ddl" : myThis.value },
        function () {}
    );
    return arc;
};


// ForClause
//
// Grammar:
// "for" Variable ( "at" Variable )? "in" ( Expression )
//
// @param for_variable [String], REQUIRED, first variable in clause 
// @param at_variable [String], NOT REQUIRED, first variable in clause
// @param expression [AsterixExpression], REQUIRED, expression to evaluate
//
// Doesn't need Expression syntax
// ForExpression.prototype = new AsterixExpression();
// ForExpression.prototype.constructor = ForExpression;
function ForClause(for_variable, at_variable, expression) {
    
    // Parse for and expression
    this.variable = for_variable;
    this.expression = expression;

    // at_variable is optional, check if defined
    this.at = typeof at_variable ? a : null;

    // TODO Error handling
    this.toString = function() {
     
    };
}
 

/*
// ************************
// Asterix Query Manager
// ************************

// FOR REVIEW: This is not a good name, but I have no idea what to name it.
function AsterixCoreJS() {
    this.Expression = AQLExpression;
    this.FLWOGR = AQLFLWOGR;
    this.IfThenElse = AQLIfThenElse;
}

// ********************************
// Asterix REST API Communication
// ********************************
function AsterixAPIRequest() {
   var xmlhttp = new XMLHttpRequest();
   xmlhttp.open("GET", "http://localhost:19101/query", true);
   xmlhttp.setRequestHeader('Content-Type', 'application/json');
   xmlhttp.send(JSON.stringify({"query" : "use dataverse dne;"}));
   var serverResponse = xmlhttp.responseText;
   alert(serverResponse);
}

// ********************
// Asterix Expressions
// ********************

function AsterixExpression() {

    // PRIVATE MEMBERS
    
    // PROTECTED METHODS
    // * May be overridden with prototypes
    // * May access private members
    // * May not be changed
   
    // PUBLIC
}

// PUBLIC EXPRESSION PROPERTIES
AsterixExpression.prototype.bindTo = function() {

}

// For some AsterixExpression extension foo:
// foo.prototype = new AsterixExpression();
// foo.prototype.constructor = foo;
// function foo() { // Set properties here }
// foo.prototype.anotherMethod = function() { // Another example function }

// AsterixFLWOGR
// Inherits From: AsterixExpression
//
// FLWOGR   ::= ( ForClause | LetClause ) ( Clause )* "return" Expression
AQLFLWOGR.prototype = new AsterixExpression();
AQLFLWOGR.prototype.constructor = AQLFLWOGR;
function AQLFLWOGR() {

}

// AsterixIfThenElse
// Inherits From: AsterixExpression
//
// IfThenElse   ::= "if" <LEFTPAREN> Expression <RIGHTPAREN> "then" 
// Expression "else" Expression
AQLIfThenElse.prototype = new AsterixExpression();
AQLIfThenElse.prototype.constructor = AQLIfThenElse;
function AQLIfThenElse() {

}*/
