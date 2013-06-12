$(document).ready(function() {

    // 0A - Exact-Match Lookup
    $('#run0a').click(function () {
        $('#result0a').html('');
        var expression0a = new FLWOGRExpression({
            "success"   : function(res) {
                            $('#result0a').html(res["results"]);
                          } 
            })
            .bind( new ForClause("user", null, new AExpression().set("dataset FacebookUsers")) )
            .bind( new WhereClause(new BooleanExpression("$user.id = 8")) )
            .bind({ "return" : new AExpression().set("$user") });
        
        expression0a.run();
    });

    // 0B - Range Scan
    $("#run0b").click(function() {
        $('#result0b').html('');

        var expression0b = new FLWOGRExpression({
            "success"   : function(res) {
                              alert(JSON.stringify(res));
                              $('#result0b').html(res["results"]);
                          },
            })
            .bind( new ForClause("user", null, new AExpression().set("dataset FacebookUsers")) )
            .bind( new WhereClause( new BooleanExpression("AND", new BooleanExpression(">=", "$user.id", 2), new BooleanExpression("<=", "$user.id", 4)) ) )
            .bind( new ReturnClause("$user") );
        alert(expression0b.val());
        expression0b.run();

    });

    // 1 - Other Query Filters
    $("#run1").click(function() {
        $('#result1').html('');

        var expression1 = new FLWOGRExpression({
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
            "success"   : function(res) {
                            alert(JSON.stringify(res));
                            $('#result6').html(res["results"]);
                          }
        })
        .bind( new ForClause (
            "fbu", 
            null, 
            new AQLClause().set("dataset FacebookUsers")
        ))
        .bind( new WhereClause ( 
            new QuantifiedExpression (
                "some" , 
                {"$e" : new AQLClause().set("$fbu.employment") },
                new AQLClause().set("is-null($e.end-date)")
            )
        ))
        .bind( new ReturnClause( new AQLClause().set("$fbu") ));
        alert("EXPRESSION 6 " + expression6.val());
    });

    // 7 - Universal Quantification
    $("#run7").click(function() {
        $('#result7').html('');

        var expression7 = new FLWOGRExpression({
            "success"   : function(res) {
                            alert(JSON.stringify(res));
                            $('#result7').html(res["results"]);
                          }
        })
        .bind( new ForClause (
            "fbu", 
            null, 
            new AQLClause().set("dataset FacebookUsers")
        ))
        .bind( new WhereClause ( 
            new QuantifiedExpression (
                "every" , 
                {"$e" : new AQLClause().set("$fbu.employment") },
                new AQLClause().set("not(is-null($e.end-date))")
            )
        ))
        .bind(new ReturnClause( new AQLClause().set("$fbu") ));
        alert("EXPRESSION 7 " + expression7.val());
    });

    // 8 - Simple Aggregation
    $('#run8').click(function () {
    
        $('#result8').html('');   

        var expression8 = new FunctionExpression(
            "count",
            new ForClause("fbu", null, new AQLClause().set("dataset FacebookUsers"))
                .bind( new ReturnClause("$fbu") )
        );
        
        var success8 = function(res) {
            $('#result8').html(res["results"]);
        };
        expression8.run(success8);
    });

    // 9a - Grouping & Aggregation
    $("#run9a").click(function() {
        $('#result9a').html('');

        var expression9a = new FLWOGRExpression()
            .bind( new ForClause("t", null, new AExpression().set("dataset TweetMessages")))
            .bind( new GroupClause("uid", new AExpression().set("$t.user.screen-name"), "with", "t") )
            .bind( new ReturnClause(
                {
                    "user" : "$uid",
                    "count" : new FunctionExpression("count", new AExpression().set("$t"))
                }
            ));

        var success9a = function(res) {
            $('#result9a').html(res["results"]);
        };
        expression9a.run(success9a);
    });

    // 9b - Hash-based Grouping & Aggregation
    $("#run9b").click(function() {
        $('#result9b').html('');

        var expression9b = new FLWOGRExpression()
            .bind( new ForClause("t", null, new AExpression().set("dataset TweetMessages"))) 
            .bind( new AQLClause().set("/*+ hash*/"))  
            .bind( new GroupClause("uid", new AExpression().set("$t.user.screen-name"), "with", "t") )
            .bind( new ReturnClause(
                {
                    "user" : "$uid",
                    "count" : new FunctionExpression("count", new AExpression().set("$t"))
                }
            ));
        
        var success9b = function(res) {
            $('#result9b').html(res["results"]);
        };
        expression9b.run(success9b);
    });
    
    // 10 - Grouping and Limits
    $("#run10").click(function() {
        $('#result10').html('');

        var expression10 = new FLWOGRExpression()
        .bind( new ForClause("t", null, new AExpression().set("dataset TweetMessages")))
        .bind( new GroupClause("uid", new AExpression().set("$t.user.screen-name"), "with", "t") )
        .bind( new LetClause(
            "c", 
            new FunctionExpression("count", new AExpression().set("$t"))
        ))
        .bind( new OrderbyClause( new AExpression().set("$c"), "desc" ) )
        .bind( new LimitClause(new AExpression().set("3")) )
        .bind( new ReturnClause(
            {
                "user" : "$uid",
                "count" : "$c"
            }
        ));

        var success10 = function(res) {
            $('#result10').html(res["results"]);
        };
        expression10.run(success10);
    });

    // 11 - Left Outer Fuzzy Join
    $("#run11").click(function() {
        $('#result11').html('');

        var expression11 = new FLWOGRExpression({
            "success"   : function(res) {
                            $('#result11').html(res["results"]);
                          }
        })
        .bind( new SetStatement( "simfunction", "jaccard" ))
        .bind( new SetStatement( "simthreshold", "0.3"))
        .bind( new ForClause( "t", null, new AExpression().set("dataset TweetMessages") ))
        .bind( new ReturnClause({
            "tweet"         : new AExpression().set("$t"),       
            "similar-tweets": new FLWOGRExpression()
                                .bind( new ForClause( "t2", null, new AExpression().set("dataset TweetMessages") ))
                                .bind( new AQLClause().set("where $t2.referred-topics ~= $t.referred-topics and $t2.tweetid != $t.tweetid") )
                                .bind( new ReturnClause("$t2.referred-topics"))
        })); 
        
        var success11 = function(res) {
            $('#result11').html(res["results"]);
        };
        expression11.run(success11);
    });

    //$('#run0a').trigger('click');
    //$('#run0b').trigger('click');
    //$('#run1').trigger('click');
    //$('#run2a').trigger('click');
    //$('#run2b').trigger('click');
    //$('#run3').trigger('click');
    //$('#run4').trigger('click');
    //$('#run5').trigger('click');
    //$('#run6').trigger('click');
    //$('#run7').trigger('click');
    $('#run8').trigger('click');
    $('#run9a').trigger('click');
    $('#run9b').trigger('click');
    $('#run10').trigger('click');
    $('#run11').trigger('click');
});
