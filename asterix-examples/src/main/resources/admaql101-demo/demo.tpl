<!DOCTYPE html>
<!--
 ! Licensed to the Apache Software Foundation (ASF) under one
 ! or more contributor license agreements.  See the NOTICE file
 ! distributed with this work for additional information
 ! regarding copyright ownership.  The ASF licenses this file
 ! to you under the Apache License, Version 2.0 (the
 ! "License"); you may not use this file except in compliance
 ! with the License.  You may obtain a copy of the License at
 !
 !   http://www.apache.org/licenses/LICENSE-2.0
 !
 ! Unless required by applicable law or agreed to in writing,
 ! software distributed under the License is distributed on an
 ! "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 ! KIND, either express or implied.  See the License for the
 ! specific language governing permissions and limitations
 ! under the License.
 !-->
<html>
<head>
    <title>AsterixDB TinySocial Demo</title>

    <style>
        .pretty-printed {
            background-color: #eeeeee;
            margin-bottom: 1em;
        }

        .how-to-run {
            background-color: #c8c8c8;
            margin-bottom: 1em;
        }

        .result-output {
            background-color: #BED8E5;
            margin-bottom: 1em;
            overflow-x: scroll;
            overflow-y: none;
        }

        body {
            font-family : "Helvetica";
            margin-bottom: 1em;
        }
    </style>
    
    <link href="static/css/bootstrap.min.css" rel="stylesheet">

    <script src="static/js/jquery.min.js"></script>
    <script src="static/js/bootstrap.min.js"></script>
    <script src="static/js/asterix-sdk-stable.js"></script>
    <script src="static/js/demo.js"></script>
</head>
<body>
  <div class="container">
    <h1>AQL: Querying TinySocial AsterixDB</h1>
    
    <h2>Query 0-A - Exact-Match Lookup</h2>
    <div class="sample-query">

        <div class="pretty-printed"><pre>
    use dataverse TinySocial;

    for $user in dataset FacebookUsers
    where $user.id = 8
    return $user;
        </pre></div>

        <div class="how-to-run"><pre><code class="javascript">
        var expression0a = new FLWOGRExpression()
            .ForClause("$user", new AExpression("dataset FacebookUsers"))
            .WhereClause(new AExpression("$user.id = 8"))
            .ReturnClause("$user");
        </code></pre></div>

        <div class="result-output" id="result0a">
        </div>

        <button id="run0a">Run #0-A</button>
    </div>
    <hr/>
    <h2>Query 0-B - Range Scan</h2>
    <div class="sample-query">
        
        <div class="pretty-printed"><pre>
    use dataverse TinySocial;

    for $user in dataset FacebookUsers
    where $user.id >= 2 and $user.id <= 4
    return $user;
        </pre></div>
        
        <div class="how-to-run"><pre><code class="javascript">
        var expression0b = new FLWOGRExpression()
            .ForClause("$user", new AExpression("dataset FacebookUsers"))
            .WhereClause().and(new AExpression("$user.id >= 2"), new AExpression("$user.id <= 4"))
            .ReturnClause("$user");       
        </code></pre></div>

        <div class="result-output" id="result0b">
        </div>

        <button id="run0b">Run #0-B</button>
    </div>
    <hr/>

    <h2>Query 1 - Other Query Filters</h2>
    <div class="sample-query">
        <div class="pretty-printed"><pre>
    use dataverse TinySocial;

    for $user in dataset FacebookUsers
    where $user.user-since >= datetime('2010-07-22T00:00:00')
    and $user.user-since <= datetime('2012-07-29T23:59:59')
    return $user;
        </pre></div>
        
        <div class="how-to-run"><pre><code class="javascript">
        var expression1 = new FLWOGRExpression()
            .ForClause("$user", new AExpression("dataset FacebookUsers"))
            .WhereClause().and(
                new AExpression("$user.user-since >= datetime('2010-07-22T00:00:00')"), 
                new AExpression("$user.user-since <= datetime('2012-07-29T23:59:59')")
            )
            .ReturnClause("$user");
        </code></pre></div>

        <div class="result-output" id="result1">
        </div>

        <button id="run1">Run #1</button>
    </div>
    <hr/>

    <h2>Query 2-A - Equijoin</h2>
    <div class="sample-query">
        <div class="pretty-printed"><pre>
    use dataverse TinySocial;

    for $user in dataset FacebookUsers
    for $message in dataset FacebookMessages
    where $message.author-id = $user.id 
    return {
        "uname": $user.name,
        "message": $message.message
    };
        </pre></div>
        
        <div class="how-to-run"><pre><code class="javascript">
        var expression2a = new FLWOGRExpression()
            .ForClause ("$user", new AExpression("dataset FacebookUsers"))
            .ForClause ("$message", new AExpression("dataset FacebookMessages"))
            .WhereClause(new AExpression("$message.author-id = $user.id"))
            .ReturnClause({
                "uname" : "$user.name",
                "message" : "$message.message"
            });
        </code></pre></div>

        <div class="result-output" id="result2a">
        </div>

        <button id="run2a">Run #2-A</button>
    </div>
    <hr/>

    <h2>Query 2-B - Index join</h2>
    <div class="sample-query">
        <div class="pretty-printed"><pre>
    use dataverse TinySocial;

    for $user in dataset FacebookUsers
    for $message in dataset FacebookMessages
    where $message.author-id /*+ indexnl */  = $user.id
    return {
        "uname": $user.name,
        "message": $message.message
    };
        </pre></div>
        
        <div class="how-to-run"><pre><code class="javascript">
        var expression2b = new FLWOGRExpression()
            .ForClause ("$user", new AExpression("dataset FacebookUsers"))
            .ForClause ("$message", new AExpression("dataset FacebookMessages"))
            .WhereClause(new AExpression("$message.author-id /*+ indexnl */  = $user.id"))
            .ReturnClause({
                    "uname" : "$user.name",
                    "message" : "$message.message"
            });
        </code></pre></div>

        <div class="result-output" id="result2b">
        </div>

        <button id="run2b">Run #2-B</button>
    </div>
    <hr/>

    <h2>Query 3 - Nested Outer Join</h2>
    <div class="sample-query">
        <div class="pretty-printed"><pre>
    use dataverse TinySocial;

    for $user in dataset FacebookUsers
    return {
        "uname": $user.name,
        "messages": for $message in dataset FacebookMessages
                    where $message.author-id = $user.id
                    return $message.message
    };
        </pre></div>
        
        <div class="how-to-run"><pre><code class="javascript">
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
        </code></pre></div>

        <div class="result-output" id="result3">
        </div>

        <button id="run3">Run #3</button>
    </div>
    <hr/>

    <h2>Query 4 - Theta Join</h2>
    <div class="sample-query">
        <div class="pretty-printed"><pre>
    use dataverse TinySocial;

    for $t in dataset TweetMessages
    return {
        "message": $t.message-text,
        "nearby-messages": for $t2 in dataset TweetMessages
                           where spatial-distance($t.sender-location, $t2.sender-location) <= 1
                           return { "msgtxt":$t2.message-text}
    };
        </pre></div>
        
        <div class="how-to-run"><pre><code class="javascript">
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
        </code></pre></div>

        <div class="result-output" id="result4">
        </div>

        <button id="run4">Run #4</button>
    </div>
    <hr/>

    <h2>Query 5 - Fuzzy Join</h2>
    <div class="sample-query">
        <div class="pretty-printed"><pre>
    use dataverse TinySocial;

    set simfunction "edit-distance";
    set simthreshold "3";

    for $fbu in dataset FacebookUsers
    return {
        "id": $fbu.id,
        "name": $fbu.name,
        "similar-users": for $t in dataset TweetMessages
                         let $tu := $t.user
                         where $tu.name ~= $fbu.name
                        return {
                            "twitter-screenname": $tu.screen-name,
                            "twitter-name": $tu.name
                        }
    };
        </pre></div>
        
        <div class="how-to-run"><pre><code class="javascript">
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
        </code></pre></div>

        <div class="result-output" id="result5">
        </div>

        <button id="run5">Run #5</button>
    </div>
    <hr/>

    <h2>Query 6 - Existential Quantification</h2>
    <div class="sample-query">
        <div class="pretty-printed"><pre>
    use dataverse TinySocial;

    for $fbu in dataset FacebookUsers
    where (some $e in $fbu.employment satisfies is-null($e.end-date)) 
    return $fbu;
        </pre></div>
        
        <div class="how-to-run"><pre><code class="javascript">
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
        </code></pre></div>

        <div class="result-output" id="result6">
        </div>

        <button id="run6">Run #6</button>
    </div>
    <hr/>

    <h2>Query 7 - Universal Quantification</h2>
    <div class="sample-query">
        <div class="pretty-printed"><pre>
    use dataverse TinySocial;

    for $fbu in dataset FacebookUsers
    where (every $e in $fbu.employment satisfies not(is-null($e.end-date))) 
    return $fbu;
        </pre></div>
        
        <div class="how-to-run"><pre><code class="javascript">
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
        </code></pre></div>

        <div class="result-output" id="result7">
        </div>

        <button id="run7">Run #7</button>
    </div>
    <hr/>

    <h2>Query 8 - Simple Aggregation</h2>
    <div class="sample-query">
        <div class="pretty-printed"><pre>
    use dataverse TinySocial;

    count(for $fbu in dataset FacebookUsers return $fbu);
        </pre></div>
        
        <div class="how-to-run"><pre><code class="javascript">
        var expression8 = new FunctionExpression(
            "count",
            new FLWOGRExpression()
                .ForClause("$fbu", new AExpression("dataset FacebookUsers"))
                .ReturnClause("$fbu")
        );
        </code></pre></div>

        <div class="result-output" id="result8">
        </div>

        <button id="run8">Run #8</button>
    </div>
    <hr/>

    <h2>Query 9-A - Grouping and Aggregation</h2>
    <div class="sample-query">
        <div class="pretty-printed"><pre>
    use dataverse TinySocial;

    for $t in dataset TweetMessages
    group by $uid := $t.user.screen-name with $t
    return {
        "user": $uid,
        "count": count($t)
    };
        </pre></div>
        
        <div class="how-to-run"><pre><code class="javascript">
        var expression9a = new FLWOGRExpression()
            .ForClause("$t", new AExpression("dataset TweetMessages"))
            .GroupClause("$uid", new AExpression("$t.user.screen-name"), "with", "$t")
            .ReturnClause(
                {
                    "user" : "$uid",
                    "count" : new FunctionExpression("count", new AExpression("$t"))
                }
            );
        </code></pre></div>

        <div class="result-output" id="result9a">
        </div>

        <button id="run9a">Run #9-A</button>
    </div>
    <hr/>

    <h2>Query 9-B - (Hash-Based) Grouping and Aggregation</h2>
    <div class="sample-query">
        <div class="pretty-printed"><pre>
    use dataverse TinySocial;

    for $t in dataset TweetMessages
    /*+ hash*/
    group by $uid := $t.user.screen-name with $t
    return {
        "user": $uid,
        "count": count($t)
    };
        </pre></div>
        
        <div class="how-to-run"><pre><code class="javascript">
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
        </code></pre></div>

        <div class="result-output" id="result9b">
        </div>

        <button id="run9b">Run #9-B</button>
    </div>
    <hr/>

    <h2>Query 10 - Grouping and Limits</h2>
    <div class="sample-query">
        <div class="pretty-printed"><pre>
    use dataverse TinySocial;

    for $t in dataset TweetMessages
    group by $uid := $t.user.screen-name with $t
    let $c := count($t)
    order by $c desc
    limit 3
    return {
        "user": $uid,
        "count": $c
    };
        </pre></div>
        
        <div class="how-to-run"><pre><code class="javascript">
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
        </code></pre></div>

        <div class="result-output" id="result10">
        </div>

        <button id="run10">Run #10</button>
    </div>
    <hr/>

    <h2>Query 11 - Left Outer Fuzzy Join</h2>
    <div class="sample-query">
        <div class="pretty-printed"><pre>
    use dataverse TinySocial;

    set simfunction "jaccard";
    set simthreshold "0.3";

    for $t in dataset TweetMessages
    return {             
        "tweet": $t,       
        "similar-tweets": for $t2 in dataset TweetMessages
        where  $t2.referred-topics ~= $t.referred-topics
        and $t2.tweetid != $t.tweetid
        return $t2.referred-topics
    };
        </pre></div>
        
        <div class="how-to-run"><pre><code class="javascript">
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
        </code></pre></div>

        <div class="result-output" id="result11">
        </div>

        <button id="run11">Run #11</button>
    </div>
  </div>
</body>
</html>
