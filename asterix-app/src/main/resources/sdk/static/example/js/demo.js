$(document).ready(function() {

    // 0A - Exact-Match Lookup
    $('#run0a').click(function () {
        $('#result0a').html('');
        var expression0a = new FLWOGRExpression({
            "dataverse" : "TinySocial",
            "success"   : function(res) {
                            $('#result0a').html(res["results"]);
                          } 
            })
            .bind( new ForClause("user", null, new AsterixExpression().set(["dataset FacebookUsers"])) )
            .bind( new WhereClause(new BooleanExpression("$user.id = 8")) )
            .bind({ "return" : new AsterixExpression().set(["$user"]) });
        
        expression0a.run();
    });

    // 0B - Range Scan
    $("#run0b").click(function() {
        $('#result0b').html('');

        var expression0b = new FLWOGRExpression({
            "dataverse" : "TinySocial",
            "success"   : function(res) {
                              alert(JSON.stringify(res));
                              $('#result0b').html(res["results"]);
                          },
            })
            .bind( new ForClause("user", null, new AsterixExpression().set(["dataset FacebookUsers"])) )
            .bind( new WhereClause( new BooleanExpression("AND", new BooleanExpression(">=", "$user.id", 2), new BooleanExpression("<=", "$user.id", 4)) ) )
            .bind( new ReturnClause("$user") );
        alert(expression0b.val());
        expression0b.run();

    });

    // 1 - Other Query Filters
    $("#run1").click(function() {
        $('#result1').html('');

        var expression1 = new FLWOGRExpression({
            "dataverse" : "TinySocial",
            "success"   : function(res) {
                            alert(JSON.stringify(res));
                            $('#result1').html(res["results"]);
                          }
        });
        alert("EXPRESSION 1 " + expression1.val());
    });

    // 2A - Equijoin
    $("#run2a").click(function() {
        $('#result2a').html('');

        var expression2a = new FLWOGRExpression({
            "dataverse" : "TinySocial",
            "success"   : function(res) {
                            alert(JSON.stringify(res));
                            $('#result2a').html(res["results"]);
                          }
        });
        alert("EXPRESSION 2a " + expression2a.val());
    });

    // 2B - Index Join
    $("#run2b").click(function() {
        $('#result2b').html('');

        var expression2b = new FLWOGRExpression({
            "dataverse" : "TinySocial",
            "success"   : function(res) {
                            alert(JSON.stringify(res));
                            $('#result2b').html(res["results"]);
                          }
        });
        alert("EXPRESSION 2b " + expression2b.val());
    });

    // 3 - Nested Outer Join
    $("#run3").click(function() {
        $('#result3').html('');

        var expression3 = new FLWOGRExpression({
            "dataverse" : "TinySocial",
            "success"   : function(res) {
                            alert(JSON.stringify(res));
                            $('#result3').html(res["results"]);
                          }
        });
        alert("EXPRESSION 3 " + expression3.val());
    });
    
    // 4 - Theta Join
    $("#run4").click(function() {
        $('#result4').html('');

        var expression4 = new FLWOGRExpression({
            "dataverse" : "TinySocial",
            "success"   : function(res) {
                            alert(JSON.stringify(res));
                            $('#result4').html(res["results"]);
                          }
        });
        alert("EXPRESSION 4 " + expression4.val());
    });

    // 5 - Fuzzy Join
    $("#run5").click(function() {
        $('#result5').html('');

        var expression5 = new FLWOGRExpression({
            "dataverse" : "TinySocial",
            "success"   : function(res) {
                            alert(JSON.stringify(res));
                            $('#result5').html(res["results"]);
                          }
        });
        alert("EXPRESSION 5 " + expression5.val());
    });

    // 6 - Existential Quantification
    $("#run6").click(function() {
        $('#result6').html('');

        var expression6 = new FLWOGRExpression({
            "dataverse" : "TinySocial",
            "success"   : function(res) {
                            alert(JSON.stringify(res));
                            $('#result6').html(res["results"]);
                          }
        });
        alert("EXPRESSION 6 " + expression6.val());
    });

    // 7 - Universal Quantification
    $("#run7").click(function() {
        $('#result7').html('');

        var expression7 = new FLWOGRExpression({
            "dataverse" : "TinySocial",
            "success"   : function(res) {
                            alert(JSON.stringify(res));
                            $('#result7').html(res["results"]);
                          }
        });
        alert("EXPRESSION 7 " + expression7.val());
    });

    // 8 - Simple Aggregation
    $('#run8').click(function () {

        // Option 1: Simple, Object Syntax     
        $('#result8').html('');   
        var expression8 = new FunctionExpression({
            "function"      : "count",
            "expression"    : new ForClause(
                                "fbu", null, new AsterixExpression().set(["dataset FacebookUsers"])
                              ).bind(
                                {"return" : new AsterixExpression().set(["$fbu"])}
                              ),
            "dataverse"     : "TinySocial",
            "success"       : function(res) {
                                $('#result8').html(res["results"]);
                              }
        });
        expression8.run();
    });

    // 9a - Grouping & Aggregation
    $("#run9a").click(function() {
        $('#result9a').html('');

        var expression9a = new FLWOGRExpression({
            "dataverse" : "TinySocial",
            "success"   : function(res) {
                            alert(JSON.stringify(res));
                            $('#result9a').html(res["results"]);
                          }
        });
        alert("EXPRESSION 9a " + expression9a.val());
    });

    // 9b - Hash-bAsed Grouping & Aggregation
    $("#run9b").click(function() {
        $('#result9b').html('');

        var expression9b = new FLWOGRExpression({
            "dataverse" : "TinySocial",
            "success"   : function(res) {
                            alert(JSON.stringify(res));
                            $('#result9b').html(res["results"]);
                          }
        });
        alert("EXPRESSION 9b " + expression9b.val());
    });
    
    // 10 - Grouping and Limits
    $("#run10").click(function() {
        $('#result10').html('');

        var expression10 = new FLWOGRExpression({
            "dataverse" : "TinySocial",
            "success"   : function(res) {
                            alert(JSON.stringify(res));
                            $('#result10').html(res["results"]);
                          }
        })
        .bind( new ForClause("t", null, new AsterixExpression().set(["dataset TweetMessages"])))
        //.bind( new GroupClause() );
        .bind( new LetClause(
            "c", 
            new FunctionExpression(
                { 
                    "function" : "count",
                    "expression" : new AsterixExpression().set(["$t"])
                }
            )
        ))
        .bind( new OrderbyClause( new AExpression().set("$c"), "desc" ) )
        .bind( new LimitClause(new AsterixExpression().set(["3"])) )
        .bind( new ReturnClause(
            {
                "user" : "$uid",
                "count" : "$c"
            }
        ));

        alert("EXPRESSION 10 \n" + expression10.val());
    });

    // 11 - Left Outer Fuzzy Join
    $("#run11").click(function() {
        $('#result11').html('');

        var expression11 = new FLWOGRExpression({
            "dataverse" : "TinySocial",
            "success"   : function(res) {
                            alert(JSON.stringify(res));
                            $('#result11').html(res["results"]);
                          }
        });
        alert("EXPRESSION 11 " + expression11.val());
    });

});
