$(document).ready(function() {

    // 0A - Exact-Match Lookup
    $('#run0a').click(function () {
        $('#result0a').html('');
        
        var expression0a = new FLWOGRExpression()
            .bind( new ForClause("user", null, new AExpression().set("dataset FacebookUsers")))
            .bind( new WhereClause(new AExpression().set("$user.id = 8")))
            .ReturnClause("$user");
        
        var success0a = function(res) {
            $('#result0a').html(res["results"]);
        };
        expression0a.run(success0a);
    });

    // 0B - Range Scan
    $("#run0b").click(function() {
        $('#result0b').html('');

        var expression0b = new FLWOGRExpression()
            .bind( new ForClause("user", null, new AExpression().set("dataset FacebookUsers")))
            .bind( new WhereClause().and(
                new AExpression().set("$user.id >= 2"), 
                new AExpression().set("$user.id <= 4")
            ))
            .ReturnClause("$user");
            
        var success0b = function(res) {
            $('#result0b').html(res["results"]);
        };
        alert(expression0b.val());
        expression0b.run(success0b);

    });

    // 1 - Other Query Filters
    $("#run1").click(function() {
        $('#result1').html('');

        var expression1 = new FLWOGRExpression()
            .bind( new ForClause("user", null, new AExpression().set("dataset FacebookUsers")))
            .bind( new WhereClause( new AExpression().set("$user.user-since >= datetime('2010-07-22T00:00:00') and $user.user-since <= datetime('2012-07-29T23:59:59')")))
            .ReturnClause("$user");
        
        var success1 = function(res) {
            $('#result1').html(res["results"]);
        };
        expression1.run(success1);
    });
        
       //     "similar-tweets": new FLWOGRExpression()
       //                         .bind( new ForClause( "t2", null, new AExpression().set("dataset TweetMessages") ))
     //                           .bind( new AQLClause().set("where $t2.referred-topics ~= $t.referred-topics and $t2.tweetid != $t.tweetid") )
      //                          .bind( new ReturnClause("$t2.referred-topics"))
      //  })); 
        
    // 2A - Equijoin
    $("#run2a").click(function() {
        $('#result2a').html('');

        var expression2a = new FLWOGRExpression()
            .bind( new ForClause ("user", null, new AQLClause().set("dataset FacebookUsers")))
            .bind( new ForClause ("message", null, new AQLClause().set("dataset FacebookMessages")))
            .bind( new WhereClause (new AExpression().set("$message.author-id = $user.id")))
            .ReturnClause(
                {
                    "uname" : "$user.name",
                    "message" : "$message.message"
                }
            );
        
        var success2a = function(res) {
            $('#result2a').html(res["results"]);
        };
        expression2a.run(success2a);
    });

    // 2B - Index Join
    $("#run2b").click(function() {
        $('#result2b').html('');

        var expression2b = new FLWOGRExpression()
            .bind( new ForClause ("user", null, new AQLClause().set("dataset FacebookUsers")))
            .bind( new ForClause ("message", null, new AQLClause().set("dataset FacebookMessages")))
            .bind( new WhereClause (new AExpression().set("$message.author-id /*+ indexnl */  = $user.id")))
            .ReturnClause(
                {
                    "uname" : "$user.name",
                    "message" : "$message.message"
                }
            );
        
        var success2b = function(res) {
            $('#result2b').html(res["results"]);
        };
        expression2b.run(success2b);
    });

    // 3 - Nested Outer Join
    $("#run3").click(function() {
        $('#result3').html('');

        var expression3messages = new FLWOGRExpression()
            .bind( new ForClause("message", null, new AExpression().set("dataset FacebookMessages")))
            .bind( new WhereClause(new AExpression().set("$message.author-id = $user.id")))
            .ReturnClause("$message.message");

        var expression3 = new FLWOGRExpression()
            .bind( new ForClause ("user", null, new AQLClause().set("dataset FacebookUsers")))
            .ReturnClause({
                "uname": "$user.name",
                "messages" : expression3messages
            });
            
        var success3 = function(res) {
            $('#result3').html(res["results"]);
        };
        expression3.run(success3);
    });
    
    // 4 - Theta Join
    $("#run4").click(function() {
        $('#result4').html('');

        var expression4messages = new FLWOGRExpression()
            .bind( new ForClause( "t2", null, new AExpression().set("dataset TweetMessages")))
            .bind( new WhereClause( new AExpression().set("spatial-distance($t.sender-location, $t2.sender-location) <= 1")))
            .ReturnClause({ "msgtxt" : "$t2.message-text" });
            
        var expression4 = new FLWOGRExpression()
            .bind( new ForClause( "t", null, new AExpression().set("dataset TweetMessages") ))
            .ReturnClause({
                "message" : "$t.message-text",
                "nearby-messages" : expression4messages
            });
        
        var success4 = function(res) {
            $('#result4').html(res["results"]);
        };
        expression4.run(success4);
    });

    // 5 - Fuzzy Join
    $("#run5").click(function() {
        $('#result5').html('');

        var similarUsersExpression = new FLWOGRExpression()
            .bind( new ForClause ("t", null, new AQLClause().set("dataset TweetMessages")))
            .bind( new LetClause ("tu", new AExpression().set("$t.user")))
            .bind( new WhereClause (new AExpression().set("$tu.name ~= $fbu.name")))
            .ReturnClause({
                "twitter-screenname": "$tu.screen-name",
                "twitter-name": "$tu.name"
            });

        var expression5 = new FLWOGRExpression()
            .bind( new SetStatement( "simfunction", "edit-distance" ))
            .bind( new SetStatement( "simthreshold", "3"))
            .bind( new ForClause ("fbu", null, new AQLClause().set("dataset FacebookUsers")))
            .ReturnClause(
                {
                    "id" : "$fbu.id",
                    "name" : "$fbu.name",
                    "similar-users" : similarUsersExpression
                }
            );
        
        var success5 = function (res) {
            $('#result5').html(res["results"]);
        };
        expression5.run(success5);
    });

    // 6 - Existential Quantification
    $("#run6").click(function() {
        $('#result6').html('');

        var expression6 = new FLWOGRExpression()
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
        .ReturnClause("$fbu");
        
        var success6 = function(res) {
            $('#result6').html(res["results"]);
        };
        
        expression6.run(success6);
    });

    // 7 - Universal Quantification
    $("#run7").click(function() {
        $('#result7').html('');

        var expression7 = new FLWOGRExpression()
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
        .ReturnClause("$fbu");
        
        var success7 = function(res) {
            $('#result7').html(res["results"]);
        };
        expression7.run(success7);
    });

    // 8 - Simple Aggregation
    $('#run8').click(function () {
    
        $('#result8').html('');   

        var expression8 = new FunctionExpression(
            "count",
            new ForClause("fbu", null, new AQLClause().set("dataset FacebookUsers"))
                .ReturnClause("$fbu")
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
            .ReturnClause(
                {
                    "user" : "$uid",
                    "count" : new FunctionExpression("count", new AExpression().set("$t"))
                }
            );

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
            .ReturnClause(
                {
                    "user" : "$uid",
                    "count" : new FunctionExpression("count", new AExpression().set("$t"))
                }
            );
        
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
        .ReturnClause(
            {
                "user" : "$uid",
                "count" : "$c"
            }
        );

        var success10 = function(res) {
            $('#result10').html(res["results"]);
        };
        expression10.run(success10);
    });

    // 11 - Left Outer Fuzzy Join
    $("#run11").click(function() {
        $('#result11').html('');

        var expression11 = new FLWOGRExpression()
        .bind( new SetStatement( "simfunction", "jaccard" ))
        .bind( new SetStatement( "simthreshold", "0.3"))
        .bind( new ForClause( "t", null, new AExpression().set("dataset TweetMessages") ))
        .ReturnClause({
            "tweet"         : new AExpression().set("$t"),       
            "similar-tweets": new FLWOGRExpression()
                                .bind( new ForClause( "t2", null, new AExpression().set("dataset TweetMessages") ))
                                .bind( new AQLClause().set("where $t2.referred-topics ~= $t.referred-topics and $t2.tweetid != $t.tweetid") )
                                .ReturnClause("$t2.referred-topics")
        }); 
        
        var success11 = function(res) {
            $('#result11').html(res["results"]);
        };
        expression11.run(success11);
    });

    $('#run0a').trigger('click');
    $('#run0b').trigger('click');
    $('#run1').trigger('click');
    $('#run2a').trigger('click');
    $('#run2b').trigger('click');
    $('#run3').trigger('click');
    $('#run4').trigger('click');
    $('#run5').trigger('click');
    $('#run6').trigger('click');
    $('#run7').trigger('click');
    $('#run8').trigger('click');
    $('#run9a').trigger('click');
    $('#run9b').trigger('click');
    $('#run10').trigger('click');
    $('#run11').trigger('click');

});
