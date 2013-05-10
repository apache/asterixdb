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
    });
    

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
    myThis = this;
    this.callbacks = {
        "sync" : function() { alert("default sync"); },
        "async" : function() {}
    };
    this.send = function(handler, cb) {
        myThis.callbacks = cb;
        this.handler = handler;
        this.extras = handler["extras"];
        this.xhr.post(
            handler["endpoint"],
            handler["apiData"],
            this.branch          
        );
    };


    // TODO DOC
    this.branch = function(response) {
        if (response && response["error-code"]) {
           
            alert("Error [Code" + response["error-code"][0] + "]: " + response["error-code"][1]);
            
        } else if (response && response["results"]) {
            var fn_callback = myThis.callbacks["sync"];
            fn_callback(response, myThis.extras);
            
        } else if (response["handle"]) {
            
            var fn_callback = this.callbacks["async"];
            fn_callback(response, extra);
            
        } else if (response["status"]) {
                
            var fn_callback = this.callbacks["sync"];
            fn_callback(response, extra);
        }
    }

        
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


    // Asterix SDK => bindingHandler
    // AsterixExpression form handler where a new REST API point is bound. Takes as input any
    // AsterixExpression, each of which is bindable.
    this.bindingHandler = new AsterixExpression();
    this.bind = this.bindingHandler.bind;
}

// AsterixExpression => Base AQL Syntax Class
//
// Components: 
//      boundTo -> a "next" expression
function AsterixExpression() {
    this.init();
    return this;
}

AsterixExpression.prototype.init = function () {
    this.boundTo = {};
    this.clauses = [];
    this.ui_callback_on_success = function() {};
    this.ui_callback_on_success_async = function() {};
};

// AsterixExpression => bind
//
// Binds a "next" AsterixExpression to this one.
//
// @param expression [AsterixExpression] MUST be instanceof AsterixExpression
// TODO Check if it has a bind method or things will get messy.
AsterixExpression.prototype.bind = function(expression) {
    // If expression is an AsterixExpression, it becomes base
    if (expression instanceof AsterixExpression) {
        this.boundTo = expression;
    } else if (expression instanceof AsterixClause) {
        this.clauses.push(expression.val());
    }
    return this;
};

// AsterixExpression => send
//
// @param arc [AsterixRESTController]
// TODO: Arc parameter is temporary
AsterixExpression.prototype.send = function(arc) {
    // Hackiest of hacks
    var g = new AsterixSDK();
    g.send(arc, arc["callback"]);
};

// AsterixExpression => clear
//
// Clears clauses array
AsterixExpression.prototype.clear = function() {
    this.clauses.length = 0;
    return this;
};

// AsterixExpression => value
//
// Retrieves string value of current expression
AsterixExpression.prototype.val = function() {
    return this.clauses.join("\n"); 
};

// AsterixExpression => success
//
// @param method [FUNCTION]
// @param isSynchronous [BOOLEAN]
AsterixExpression.prototype.success = function(fn, isSync) {
    if (isSync) {
        this.ui_callback_on_success = fn;
    } else { 
        this.ui_callback_on_success_async = fn;
    }
    return this;
};

// AsterixExpression => set
// Pushes statements onto the clause stack
//
// TODO typecheck
// @param statements_arr [ARRAY]
AsterixExpression.prototype.set = function(statements_arr) {
    for (var i = 0; i < statements_arr.length; i++) {
        this.clauses.push(statements_arr[i]);
    }
    return this;
};

function CreateExpression() { 
    AsterixExpression.prototype.init.call(this);
    return this; 
} // "create" ( "type" | "nodegroup" | "external" <DATASET> | <DATASET> | "index" | "dataverse" 


function FLWOGRExpression() { 
    AsterixExpression.prototype.init.call(this);
    return this; 
} // ( ForClause | LetClause ) ( Clause )* "return" Expression


function LegacyExpression() { 
    AsterixExpression.prototype.init.call(this);
    return this; 
} // Legacy for old AsterixAPI version: handle hardcoded strings. Will phase out.

inherit(CreateExpression, AsterixExpression);
inherit(FLWOGRExpression, AsterixExpression);
inherit(LegacyExpression, AsterixExpression);

CreateExpression.prototype.send = function() {
    myThis = this;
    var arc = new AsterixRESTController();
    arc.DDL(
        { "ddl" : myThis.val() },
        {
            "sync" : function () {}, // TODO This should be a default value out of the AsterixExpression base
            "async" : function () {}
        }
    );

    // Call AsterixExpression core send method
    AsterixExpression.prototype.send.call(this, arc);
};

// LegacyExpression
// Handles hardcoded query strings from prior versions
// of this API. Can be useful for debugging.
LegacyExpression.prototype.send = function(endpoint, json) {
    
    var arc = {
        "endpoint" : endpoint,
        "apiData" : json, 
        "callback" : {
            "sync" : this.ui_callback_on_success,
            "async" : this.ui_callback_on_success_async
        },
        "extras" : this.extras
    };

    AsterixExpression.prototype.send.call(this, arc);
}

LegacyExpression.prototype.extra = function(extras) {
    this.extras = extras;
    return this;
}

//
// Clauses
//
function AsterixClause() {
    this.clause = "";
    return this;
}

AsterixClause.prototype.val = function() {
    return this.clause;
};

AsterixClause.prototype.set = function(clause) {
    this.clause = clause;
    return this;
};

function ForClause() {}         // "for" Variable ( "at" Variable )? "in" ( Expression )
function LetClause() {}         // "let" Variable ":=" Expression
function WhereClause() {}       // "where" Expression
function OrderbyClause() {}     // ( "order" "by" Expression ( ( "asc" ) | ( "desc" ) )? ( "," Expression ( ( "asc" ) | ( "desc" ) )? )* )
function GroupClause() {}       //  "group" "by" ( Variable ":=" )? Expression ( "," ( Variable ":=" )? Expression )* 
                                //      ( "decor" Variable ":=" Expression ( "," "decor" Variable ":=" Expression )* )? "with" VariableRef ( "," VariableRef )*
function LimitClause() {}       // "limit" Expression ( "offset" Expression )?
function DistinctClause() {}    // "distinct" "by" Expression ( "," Expression )*

inherit(ForClause, AsterixClause);
inherit(LetClause, AsterixClause);
inherit(WhereClause, AsterixClause);
inherit(OrderbyClause, AsterixClause);
inherit(GroupClause, AsterixClause);
inherit(LimitClause, AsterixClause);
inherit(DistinctClause, AsterixClause);

ForClause.prototype.set = function(clause) {
//ForClause.prototype.set = function(for_variable, at_variable, expression) {
    AsterixClause.prototype.set.call(this, clause); 
    return this;
};

LetClause.prototype.set = function(clause) {
    AsterixClause.prototype.set.call(this, clause); 
    return this;
};

WhereClause.prototype.set = function(clause) {
    AsterixClause.prototype.set.call(this, clause); 
    return this;
};

OrderbyClause.prototype.set = function(clause) {
    AsterixClause.prototype.set.call(this, clause); 
    return this;
};

GroupClause.prototype.set = function(clause) {
    AsterixClause.prototype.set.call(this, clause); 
    return this;
};

LimitClause.prototype.set = function(clause) {
    AsterixClause.prototype.set.call(this, clause); 
    return this;
};

DistinctClause.prototype.set = function(clause)  {
    AsterixClause.prototype.set.call(this, clause); 
    return this;
};




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


//var a = AsterixSDK();

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
 

///////////////
// Utilities //
///////////////

// Inherit with the proxy pattern
// Source: https://gist.github.com/jeremyckahn/5552373
function inherit(inherits, inheritsFrom) {
    function proxy() {};
    proxy.prototype = inheritsFrom.prototype;
    inherits.prototype = new proxy();
}

/*
// AsterixIfThenElse
// Inherits From: AsterixExpression
//
// IfThenElse   ::= "if" <LEFTPAREN> Expression <RIGHTPAREN> "then" 
// Expression "else" Expression
AQLIfThenElse.prototype = new AsterixExpression();
AQLIfThenElse.prototype.constructor = AQLIfThenElse;
function AQLIfThenElse() {

}*/
