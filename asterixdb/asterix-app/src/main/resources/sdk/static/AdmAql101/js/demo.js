/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
$(document).ready(function() {

    var A = new AsterixDBConnection().dataverse("TinySocial");

    function addResult(dom, res) {
        for (i in res) {
            $(dom).append(res[i] + "<br/>");
        }
    }

    // 0A - Exact-Match Lookup
    $('#run0a').click(function () {
        $('#result0a').html('');

        var expression0a = new FLWOGRExpression()
            .ForClause("$user", new AExpression("dataset FacebookUsers"))
            .WhereClause(new AExpression("$user.id = 8"))
            .ReturnClause("$user");

        var success0a = function(res) {
            addResult('#result0a', res["results"]);
        };

        A.query(expression0a.val(), success0a);
    });

    // 0B - Range Scan
    $("#run0b").click(function() {
        $('#result0b').html('');

        var expression0b = new FLWOGRExpression()
            .ForClause("$user", new AExpression("dataset FacebookUsers"))
            .WhereClause().and(new AExpression("$user.id >= 2"), new AExpression("$user.id <= 4"))
            .ReturnClause("$user");

        var success0b = function(res) {
            addResult('#result0b', res["results"]);
        };

        A.query(expression0b.val(), success0b);
    });

    // 1 - Other Query Filters
    $("#run1").click(function() {
        $('#result1').html('');

        var expression1 = new FLWOGRExpression()
            .ForClause("$user", new AExpression("dataset FacebookUsers"))
            .WhereClause().and(
                new AExpression("$user.user-since >= datetime('2010-07-22T00:00:00')"),
                new AExpression("$user.user-since <= datetime('2012-07-29T23:59:59')")
            )
            .ReturnClause("$user");

        var success1 = function(res) {
            addResult('#result1', res["results"]);
        };
        A.query(expression1.val(), success1);
    });

    // 2A - Equijoin
    $("#run2a").click(function() {
        $('#result2a').html('');

        var expression2a = new FLWOGRExpression()
            .ForClause ("$user", new AExpression("dataset FacebookUsers"))
            .ForClause ("$message", new AExpression("dataset FacebookMessages"))
            .WhereClause(new AExpression("$message.author-id = $user.id"))
            .ReturnClause({
                "uname" : "$user.name",
                "message" : "$message.message"
            });

        var success2a = function(res) {
            addResult('#result2a', res["results"]);
        };
        A.query(expression2a.val(), success2a);
    });

    // 2B - Index Join
    $("#run2b").click(function() {
        $('#result2b').html('');

        var expression2b = new FLWOGRExpression()
            .ForClause ("$user", new AExpression("dataset FacebookUsers"))
            .ForClause ("$message", new AExpression("dataset FacebookMessages"))
            .WhereClause(new AExpression("$message.author-id /*+ indexnl */  = $user.id"))
            .ReturnClause({
                    "uname" : "$user.name",
                    "message" : "$message.message"
            });

        var success2b = function(res) {
            addResult('#result2b', res["results"]);
        };
        A.query(expression2b.val(), success2b);
    });

    // 3 - Nested Outer Join
    $("#run3").click(function() {
        $('#result3').html('');

        var expression3messages = new FLWOGRExpression()
            .ForClause("$message", new AExpression("dataset FacebookMessages"))
            .WhereClause(new AExpression("$message.author-id = $user.id"))
            .ReturnClause("$message.message");

        var expression3 = new FLWOGRExpression()
            .ForClause ("$user", new AExpression("dataset FacebookUsers"))
            .ReturnClause({
                "uname": "$user.name",
                "messages" : expression3messages
            });

        var success3 = function(res) {
            addResult('#result3', res["results"]);
        };
        A.query(expression3.val(), success3);
    });

    // 4 - Theta Join
    $("#run4").click(function() {
        $('#result4').html('');

        var expression4messages = new FLWOGRExpression()
            .ForClause( "$t2", new AExpression("dataset TweetMessages"))
            .WhereClause( new AExpression("spatial-distance($t.sender-location, $t2.sender-location) <= 1"))
            .ReturnClause({ "msgtxt" : "$t2.message-text" });

        var expression4 = new FLWOGRExpression()
            .ForClause( "$t", new AExpression("dataset TweetMessages"))
            .ReturnClause({
                "message" : "$t.message-text",
                "nearby-messages" : expression4messages
            });

        var success4 = function(res) {
            addResult('#result4', res["results"]);
        };
        A.query(expression4.val(), success4);
    });

    // 5 - Fuzzy Join
    $("#run5").click(function() {
        $('#result5').html('');

        var similarUsersExpression = new FLWOGRExpression()
            .ForClause("$t", new AExpression("dataset TweetMessages"))
            .LetClause ("$tu", new AExpression("$t.user"))
            .WhereClause(new AExpression("$tu.name ~= $fbu.name"))
            .ReturnClause({
                "twitter-screenname": "$tu.screen-name",
                "twitter-name": "$tu.name"
            });

        var expression5 = new FLWOGRExpression()
            .ForClause ("$fbu", new AExpression("dataset FacebookUsers"))
            .ReturnClause(
                {
                    "id" : "$fbu.id",
                    "name" : "$fbu.name",
                    "similar-users" : similarUsersExpression
                }
            );

        var success5 = function (res) {
            addResult('#result5', res["results"]);
        };

        var simfunction = new SetStatement( "simfunction", "edit-distance" );
        var simthreshold = new SetStatement( "simthreshold", "3");

        A.query(
            [ simfunction.val() , simthreshold.val() , expression5.val() ],
            success5
        );
    });

    // 6 - Existential Quantification
    $("#run6").click(function() {
        $('#result6').html('');

        var expression6 = new FLWOGRExpression()
            .ForClause ("$fbu", new AQLClause().set("dataset FacebookUsers"))
            .WhereClause(
                new QuantifiedExpression (
                    "some" ,
                    {"$e" : new AExpression("$fbu.employment") },
                    new FunctionExpression("is-null", new AExpression("$e.end-date"))
                )
            )
            .ReturnClause("$fbu");

        var success6 = function(res) {
            addResult('#result6',res["results"]);
        };

        A.query(expression6.val(), success6);
    });

    // 7 - Universal Quantification
    $("#run7").click(function() {
        $('#result7').html('');

        var expression7 = new FLWOGRExpression()
            .ForClause("$fbu", new AExpression("dataset FacebookUsers"))
            .WhereClause(
                new QuantifiedExpression (
                    "every" ,
                    {"$e" : new AExpression("$fbu.employment") },
                    new FunctionExpression("not", new FunctionExpression("is-null", new AExpression("$e.end-date")))
                )
            )
            .ReturnClause("$fbu");

        var success7 = function(res) {
            addResult('#result7', res["results"]);
        };
        A.query(expression7.val(), success7);
    });

    // 8 - Simple Aggregation
    $('#run8').click(function () {

        $('#result8').html('');

        var expression8 = new FunctionExpression(
            "count",
            new FLWOGRExpression()
                .ForClause("$fbu", new AExpression("dataset FacebookUsers"))
                .ReturnClause("$fbu")
        );

        var success8 = function(res) {
            addResult('#result8', res["results"]);
        };
        A.query(expression8.val(), success8);
    });

    // 9a - Grouping & Aggregation
    $("#run9a").click(function() {
        $('#result9a').html('');

        var expression9a = new FLWOGRExpression()
            .ForClause("$t", new AExpression("dataset TweetMessages"))
            .GroupClause("$uid", new AExpression("$t.user.screen-name"), "with", "$t")
            .ReturnClause(
                {
                    "user" : "$uid",
                    "count" : new FunctionExpression("count", new AExpression("$t"))
                }
            );

        var success9a = function(res) {
            addResult('#result9a', res["results"]);
        };
        A.query(expression9a.val(), success9a);
    });

    // 9b - Hash-based Grouping & Aggregation
    $("#run9b").click(function() {
        $('#result9b').html('');

        var expression9b = new FLWOGRExpression()
            .ForClause("$t", new AExpression("dataset TweetMessages"))
            .AQLClause("/*+ hash*/")
            .GroupClause("$uid", new AExpression("$t.user.screen-name"), "with", "$t")
            .ReturnClause(
                {
                    "user" : "$uid",
                    "count" : new FunctionExpression("count", new AExpression("$t"))
                }
            );

        var success9b = function(res) {
            addResult('#result9b', res["results"]);
        };
        A.query(expression9b.val(), success9b);
    });

    // 10 - Grouping and Limits
    $("#run10").click(function() {
        $('#result10').html('');

        var expression10 = new FLWOGRExpression()
            .ForClause("$t", new AExpression("dataset TweetMessages"))
            .GroupClause("$uid", new AExpression("$t.user.screen-name"), "with", "$t")
            .LetClause("$c", new FunctionExpression("count", new AExpression("$t")))
            .OrderbyClause( new AExpression("$c"), "desc" )
            .LimitClause(new AExpression("3"))
            .ReturnClause(
                {
                    "user" : "$uid",
                    "count" : "$c"
                }
            );

        var success10 = function(res) {
            addResult('#result10', res["results"]);
        };
        A.query(expression10.val(), success10);
    });

    // 11 - Left Outer Fuzzy Join
    $("#run11").click(function() {
        $('#result11').html('');

    var expression11 = new FLWOGRExpression()
        .ForClause( "$t", new AExpression("dataset TweetMessages"))
        .ReturnClause({
            "tweet"         : new AExpression("$t"),
            "similar-tweets": new FLWOGRExpression()
                                .ForClause( "$t2", new AExpression("dataset TweetMessages"))
                                .WhereClause().and(
                                    new AExpression("$t2.referred-topics ~= $t.referred-topics"),
                                    new AExpression("$t2.tweetid != $t.tweetid")
                                 )
                                .ReturnClause("$t2.referred-topics")
        });

        var success11 = function(res) {
            addResult('#result11', res["results"]);
        };

        var simfunction = new SetStatement( "simfunction", "jaccard" );
        var simthreshold = new SetStatement( "simthreshold", "0.3");
        A.query(
            [ simfunction.val(), simthreshold.val(), expression11.val()],
            success11
        );

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
