function AsterixSDK() {
    
    // Local RPC Handler for REST API
    this.xhr = new easyXDM.Rpc({
        remote: "http://localhost:19101/sdk/static/client.html"
    }, {
        remote: {
            post: {}
        }
    });
}


AsterixSDK.prototype.send = function(handler) {
    var api = handler.onSend();
    this.xhr.post(
        api["endpoint"],
        api["apiData"],
        api["callback"]
    );
}


/**
* Asterix SDK / requestHandler
*
* Talks to 
*/
AsterixSDK.prototype.requestHandler = function() {
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


// Asterix REST Controller
// Handles interactions with remote database using a request handler
function AsterixRESTController() {
    
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


// For expressions
// TODO Is this correct name?
// TODO Probably query API
ForExpression.prototype = new AsterixExpression();
ForExpression.prototype.constructor = ForExpression;
function ForExpression() {}
ForExpression.prototype.onSend = function () {

}

// 

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
