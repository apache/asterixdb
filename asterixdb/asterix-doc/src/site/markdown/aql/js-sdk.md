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

# AsterixDB Javascript SDK #

## Obtaining and Including ##
[Download](http://asterixdb.ics.uci.edu/download/bindings/asterix-sdk-stable.js) the javascript SDK and include it
like any other javascript library by adding the following line in the
appropriate HTML file:

	<script src="path/to/asterix-sdk-stable.js"></script>

## Interactive Demos
There are two interactive demos that are available for download. Both of the
demos illustrate how the javascript API would be used in an application:

* [Tweetbook Demo](http://asterixdb.ics.uci.edu/download/demos/tweetbook-demo.zip): a contrived geo-spatial
  application dealing with artificial Tweets allowing spatial, temporal, and
  keyword-based filtering.
* [ADM/AQL 101 Demo](http://asterixdb.ics.uci.edu/download/demos/admaql101-demo.zip): an interactive version of
  all of the examples that are provided in the following section.

## The javascript SDK: by example ##
In this section, we explore how to form AQL queries using the javascript SDK. The queries from
[AsterixDB 101: An ADM and AQL Primer](primer.html) are used as examples here. For each AQL statement, 
the equivalent javascript expression is shown below it, followed by the results of executing the query.

### Query 0-A - Exact-Match Lookup ###
###### AQL
	use dataverse TinySocial;

	for $user in dataset FacebookUsers
    where $user.id = 8
    return $user;

###### JS

	var expression0a = new FLWOGRExpression()
		.ForClause("$user", new AExpression("dataset FacebookUsers"))
        .WhereClause(new AExpression("$user.id = 8"))
        .ReturnClause("$user");

###### Results

	{ "id": { int32: 8 } , "alias": "Nila", "name": "NilaMilliron", "user-since": { datetime: 1199182200000}, "friend-ids": { unorderedlist: [{ int32: 3 } ]}, "employment": { orderedlist: [{ "organization-name": "Plexlane", "start-date": { date: 1267315200000}, "end-date": null } ]} }

### Query 0-B - Range Scan ###
###### AQL
	use dataverse TinySocial;

    for $user in dataset FacebookUsers
    where $user.id >= 2 and $user.id <= 4
    return $user;

###### JS

	var expression0b = new FLWOGRExpression()
    	.ForClause("$user", new AExpression("dataset FacebookUsers"))
		.WhereClause().and(new AExpression("$user.id >= 2"), new AExpression("$user.id <= 4"))
		.ReturnClause("$user");

###### Results

	{ "id": { int32: 2 } , "alias": "Isbel", "name": "IsbelDull", "user-since": { datetime: 1295691000000}, "friend-ids": { unorderedlist: [{ int32: 1 } , { int32: 4 } ]}, "employment": { orderedlist: [{ "organization-name": "Hexviafind", "start-date": { date: 1272326400000}, "end-date": null } ]} }
	{ "id": { int32: 3 } , "alias": "Emory", "name": "EmoryUnk", "user-since": { datetime: 1341915000000}, "friend-ids": { unorderedlist: [{ int32: 1 } , { int32: 5 } , { int32: 8 } , { int32: 9 } ]}, "employment": { orderedlist: [{ "organization-name": "geomedia", "start-date": { date: 1276732800000}, "end-date": { date: 1264464000000} } ]} }
	{ "id": { int32: 4 } , "alias": "Nicholas", "name": "NicholasStroh", "user-since": { datetime: 1293444600000}, "friend-ids": { unorderedlist: [{ int32: 2 } ]}, "employment": { orderedlist: [{ "organization-name": "Zamcorporation", "start-date": { date: 1275955200000}, "end-date": null } ]} }

### Query 1 - Other Query Filters ###
###### AQL
	use dataverse TinySocial;

    for $user in dataset FacebookUsers
    where $user.user-since >= datetime('2010-07-22T00:00:00')
    and $user.user-since <= datetime('2012-07-29T23:59:59')
    return $user;

###### JS

	var expression1 = new FLWOGRExpression()
		.ForClause("$user", new AExpression("dataset FacebookUsers"))
        .WhereClause().and(
			new AExpression("$user.user-since >= datetime('2010-07-22T00:00:00')"),
			new AExpression("$user.user-since <= datetime('2012-07-29T23:59:59')")
        ).ReturnClause("$user");

###### Results

	{ "id": { int32: 2 } , "alias": "Isbel", "name": "IsbelDull", "user-since": { datetime: 1295691000000}, "friend-ids": { unorderedlist: [{ int32: 1 } , { int32: 4 } ]}, "employment": { orderedlist: [{ "organization-name": "Hexviafind", "start-date": { date: 1272326400000}, "end-date": null } ]} }
	{ "id": { int32: 10 } , "alias": "Bram", "name": "BramHatch", "user-since": { datetime: 1287223800000}, "friend-ids": { unorderedlist: [{ int32: 1 } , { int32: 5 } , { int32: 9 } ]}, "employment": { orderedlist: [{ "organization-name": "physcane", "start-date": { date: 1181001600000}, "end-date": { date: 1320451200000} } ]} }
	{ "id": { int32: 3 } , "alias": "Emory", "name": "EmoryUnk", "user-since": { datetime: 1341915000000}, "friend-ids": { unorderedlist: [{ int32: 1 } , { int32: 5 } , { int32: 8 } , { int32: 9 } ]}, "employment": { orderedlist: [{ "organization-name": "geomedia", "start-date": { date: 1276732800000}, "end-date": { date: 1264464000000} } ]} }
	{ "id": { int32: 4 } , "alias": "Nicholas", "name": "NicholasStroh", "user-since": { datetime: 1293444600000}, "friend-ids": { unorderedlist: [{ int32: 2 } ]}, "employment": { orderedlist: [{ "organization-name": "Zamcorporation", "start-date": { date: 1275955200000}, "end-date": null } ]} }

### Query 2-A - Equijoin ###
###### AQL
	use dataverse TinySocial;

    for $user in dataset FacebookUsers
    for $message in dataset FacebookMessages
    where $message.author-id = $user.id
    return {
        "uname": $user.name,
        "message": $message.message
    };

###### JS

	var expression2a = new FLWOGRExpression()
		.ForClause ("$user", new AExpression("dataset FacebookUsers"))
		.ForClause ("$message", new AExpression("dataset FacebookMessages"))
		.WhereClause(new AExpression("$message.author-id = $user.id"))
		.ReturnClause({
			"uname" : "$user.name",
			"message" : "$message.message"
		});

###### Results

	{ "uname": "MargaritaStoddard", "message": " dislike x-phone its touch-screen is horrible" }
	{ "uname": "MargaritaStoddard", "message": " like ccast the 3G is awesome:)" }
	{ "uname": "MargaritaStoddard", "message": " can't stand product-w the touch-screen is terrible" }
	{ "uname": "MargaritaStoddard", "message": " can't stand acast the network is horrible:(" }
	{ "uname": "MargaritaStoddard", "message": " can't stand acast its plan is terrible" }
	{ "uname": "IsbelDull", "message": " like product-y the plan is amazing" }
	{ "uname": "IsbelDull", "message": " like product-z its platform is mind-blowing" }
	{ "uname": "WoodrowNehling", "message": " love acast its 3G is good:)" }
	{ "uname": "BramHatch", "message": " dislike x-phone the voice-command is bad:(" }
	{ "uname": "BramHatch", "message": " can't stand product-z its voicemail-service is OMG:(" }
	{ "uname": "EmoryUnk", "message": " love product-b its shortcut-menu is awesome:)" }
	{ "uname": "EmoryUnk", "message": " love ccast its wireless is good" }
	{ "uname": "WillisWynne", "message": " love product-b the customization is mind-blowing" }
	{ "uname": "SuzannaTillson", "message": " like x-phone the voicemail-service is awesome" }
	{ "uname": "VonKemble", "message": " dislike product-b the speed is horrible" }

### Query 2-B - Index join ###
###### AQL
	use dataverse TinySocial;

    for $user in dataset FacebookUsers
    for $message in dataset FacebookMessages
    where $message.author-id /*+ indexnl */  = $user.id
    return {
        "uname": $user.name,
        "message": $message.message
    };

###### JS

	var expression2b = new FLWOGRExpression()
		.ForClause ("$user", new AExpression("dataset FacebookUsers"))
		.ForClause ("$message", new AExpression("dataset FacebookMessages"))
		.WhereClause(new AExpression("$message.author-id /*+ indexnl */  = $user.id"))
		.ReturnClause({
			"uname" : "$user.name",
			"message" : "$message.message"
		});

###### Results

	{ "uname": "MargaritaStoddard", "message": " dislike x-phone its touch-screen is horrible" }
	{ "uname": "MargaritaStoddard", "message": " like ccast the 3G is awesome:)" }
	{ "uname": "MargaritaStoddard", "message": " can't stand product-w the touch-screen is terrible" }
	{ "uname": "MargaritaStoddard", "message": " can't stand acast the network is horrible:(" }
	{ "uname": "MargaritaStoddard", "message": " can't stand acast its plan is terrible" }
	{ "uname": "IsbelDull", "message": " like product-y the plan is amazing" }
	{ "uname": "IsbelDull", "message": " like product-z its platform is mind-blowing" }
	{ "uname": "WoodrowNehling", "message": " love acast its 3G is good:)" }
	{ "uname": "BramHatch", "message": " dislike x-phone the voice-command is bad:(" }
	{ "uname": "BramHatch", "message": " can't stand product-z its voicemail-service is OMG:(" }
	{ "uname": "EmoryUnk", "message": " love product-b its shortcut-menu is awesome:)" }
	{ "uname": "EmoryUnk", "message": " love ccast its wireless is good" }
	{ "uname": "WillisWynne", "message": " love product-b the customization is mind-blowing" }
	{ "uname": "SuzannaTillson", "message": " like x-phone the voicemail-service is awesome" }
	{ "uname": "VonKemble", "message": " dislike product-b the speed is horrible" }

### Query 3 - Nested Outer Join ###
###### AQL
	use dataverse TinySocial;

    for $user in dataset FacebookUsers
    return {
        "uname": $user.name,
        "messages": for $message in dataset FacebookMessages
                    where $message.author-id = $user.id
                    return $message.message
    };

###### JS

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

###### Results

	{ "uname": "MargaritaStoddard", "messages": { orderedlist: [" dislike x-phone its touch-screen is horrible", " like ccast the 3G is awesome:)", " can't stand product-w the touch-screen is terrible", " can't stand acast the network is horrible:(", " can't stand acast its plan is terrible" ]} }
	{ "uname": "IsbelDull", "messages": { orderedlist: [" like product-y the plan is amazing", " like product-z its platform is mind-blowing" ]} }
	{ "uname": "NilaMilliron", "messages": { orderedlist: [ ]} }
	{ "uname": "WoodrowNehling", "messages": { orderedlist: [" love acast its 3G is good:)" ]} }
	{ "uname": "BramHatch", "messages": { orderedlist: [" dislike x-phone the voice-command is bad:(", " can't stand product-z its voicemail-service is OMG:(" ]} }
	{ "uname": "EmoryUnk", "messages": { orderedlist: [" love product-b its shortcut-menu is awesome:)", " love ccast its wireless is good" ]} }
	{ "uname": "WillisWynne", "messages": { orderedlist: [" love product-b the customization is mind-blowing" ]} }
	{ "uname": "SuzannaTillson", "messages": { orderedlist: [" like x-phone the voicemail-service is awesome" ]} }
	{ "uname": "NicholasStroh", "messages": { orderedlist: [ ]} }
	{ "uname": "VonKemble", "messages": { orderedlist: [" dislike product-b the speed is horrible" ]} }

### Query 4 - Theta Join ###
###### AQL
	use dataverse TinySocial;

    for $t in dataset TweetMessages
    return {
        "message": $t.message-text,
        "nearby-messages": for $t2 in dataset TweetMessages
                           where spatial-distance($t.sender-location, $t2.sender-location) <= 1
                           return { "msgtxt":$t2.message-text}
    };

###### JS

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

###### Results

	{ "message": " hate ccast its voice-clarity is OMG:(", "nearby-messages": { orderedlist: [{ "msgtxt": " hate ccast its voice-clarity is OMG:(" }, { "msgtxt": " like product-w the speed is good:)" } ]} }
	{ "message": " like x-phone the voice-clarity is good:)", "nearby-messages": { orderedlist: [{ "msgtxt": " like x-phone the voice-clarity is good:)" } ]} }
	{ "message": " like product-y the platform is good", "nearby-messages": { orderedlist: [{ "msgtxt": " like product-y the platform is good" } ]} }
	{ "message": " love product-z its customization is good:)", "nearby-messages": { orderedlist: [{ "msgtxt": " love product-z its customization is good:)" } ]} }
	{ "message": " like product-y the voice-command is amazing:)", "nearby-messages": { orderedlist: [{ "msgtxt": " like product-y the voice-command is amazing:)" } ]} }
	{ "message": " like product-w the speed is good:)", "nearby-messages": { orderedlist: [{ "msgtxt": " hate ccast its voice-clarity is OMG:(" }, { "msgtxt": " like product-w the speed is good:)" } ]} }
	{ "message": " love ccast its voicemail-service is awesome", "nearby-messages": { orderedlist: [{ "msgtxt": " love ccast its voicemail-service is awesome" } ]} }
	{ "message": " can't stand product-w its speed is terrible:(", "nearby-messages": { orderedlist: [{ "msgtxt": " can't stand product-w its speed is terrible:(" } ]} }
	{ "message": " like product-z the shortcut-menu is awesome:)", "nearby-messages": { orderedlist: [{ "msgtxt": " like product-z the shortcut-menu is awesome:)" } ]} }
	{ "message": " can't stand x-phone its platform is terrible", "nearby-messages": { orderedlist: [{ "msgtxt": " can't stand x-phone its platform is terrible" } ]} }
	{ "message": " like ccast its shortcut-menu is awesome:)", "nearby-messages": { orderedlist: [{ "msgtxt": " like ccast its shortcut-menu is awesome:)" } ]} }
	{ "message": " like product-b the voice-command is mind-blowing:)", "nearby-messages": { orderedlist: [{ "msgtxt": " like product-b the voice-command is mind-blowing:)" } ]} }

### Query 5 - Fuzzy Join ###
###### AQL
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

###### JS

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

###### Results

	{ "id": { int32: 1 } , "name": "MargaritaStoddard", "similar-users": { orderedlist: [ ]} }
	{ "id": { int32: 2 } , "name": "IsbelDull", "similar-users": { orderedlist: [ ]} }
	{ "id": { int32: 8 } , "name": "NilaMilliron", "similar-users": { orderedlist: [{ "twitter-screenname": "NilaMilliron_tw", "twitter-name": "Nila Milliron" } ]} }
	{ "id": { int32: 9 } , "name": "WoodrowNehling", "similar-users": { orderedlist: [ ]} }
	{ "id": { int32: 10 } , "name": "BramHatch", "similar-users": { orderedlist: [ ]} }
	{ "id": { int32: 3 } , "name": "EmoryUnk", "similar-users": { orderedlist: [ ]} }
	{ "id": { int32: 6 } , "name": "WillisWynne", "similar-users": { orderedlist: [ ]} }
	{ "id": { int32: 7 } , "name": "SuzannaTillson", "similar-users": { orderedlist: [ ]} }
	{ "id": { int32: 4 } , "name": "NicholasStroh", "similar-users": { orderedlist: [ ]} }
	{ "id": { int32: 5 } , "name": "VonKemble", "similar-users": { orderedlist: [ ]} }

### Query 6 - Existential Quantification ###
###### AQL

	use dataverse TinySocial;

    for $fbu in dataset FacebookUsers
    where (some $e in $fbu.employment satisfies is-null($e.end-date))
    return $fbu;

###### JS

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

###### Results

	{ "id": { int32: 1 } , "alias": "Margarita", "name": "MargaritaStoddard", "user-since": { datetime: 1345457400000}, "friend-ids": { unorderedlist: [{ int32: 2 } , { int32: 3 } , { int32: 6 } , { int32: 10 } ]}, "employment": { orderedlist: [{ "organization-name": "Codetechno", "start-date": { date: 1154822400000}, "end-date": null } ]} }
	{ "id": { int32: 2 } , "alias": "Isbel", "name": "IsbelDull", "user-since": { datetime: 1295691000000}, "friend-ids": { unorderedlist: [{ int32: 1 } , { int32: 4 } ]}, "employment": { orderedlist: [{ "organization-name": "Hexviafind", "start-date": { date: 1272326400000}, "end-date": null } ]} }
	{ "id": { int32: 8 } , "alias": "Nila", "name": "NilaMilliron", "user-since": { datetime: 1199182200000}, "friend-ids": { unorderedlist: [{ int32: 3 } ]}, "employment": { orderedlist: [{ "organization-name": "Plexlane", "start-date": { date: 1267315200000}, "end-date": null } ]} }
	{ "id": { int32: 6 } , "alias": "Willis", "name": "WillisWynne", "user-since": { datetime: 1105956600000}, "friend-ids": { unorderedlist: [{ int32: 1 } , { int32: 3 } , { int32: 7 } ]}, "employment": { orderedlist: [{ "organization-name": "jaydax", "start-date": { date: 1242345600000}, "end-date": null } ]} }
	{ "id": { int32: 7 } , "alias": "Suzanna", "name": "SuzannaTillson", "user-since": { datetime: 1344334200000}, "friend-ids": { unorderedlist: [{ int32: 6 } ]}, "employment": { orderedlist: [{ "organization-name": "Labzatron", "start-date": { date: 1303171200000}, "end-date": null } ]} }
	{ "id": { int32: 4 } , "alias": "Nicholas", "name": "NicholasStroh", "user-since": { datetime: 1293444600000}, "friend-ids": { unorderedlist: [{ int32: 2 } ]}, "employment": { orderedlist: [{ "organization-name": "Zamcorporation", "start-date": { date: 1275955200000}, "end-date": null } ]} }
	{ "id": { int32: 5 } , "alias": "Von", "name": "VonKemble", "user-since": { datetime: 1262686200000}, "friend-ids": { unorderedlist: [{ int32: 3 } , { int32: 6 } , { int32: 10 } ]}, "employment": { orderedlist: [{ "organization-name": "Kongreen", "start-date": { date: 1290816000000}, "end-date": null } ]} }

### Query 7 - Universal Quantification ###
###### AQL

	use dataverse TinySocial;

    for $fbu in dataset FacebookUsers
    where (every $e in $fbu.employment satisfies not(is-null($e.end-date)))
    return $fbu;

###### JS

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

###### Results

	{ "id": { int32: 9 } , "alias": "Woodrow", "name": "WoodrowNehling", "user-since": { datetime: 1127211000000}, "friend-ids": { unorderedlist: [{ int32: 3 } , { int32: 10 } ]}, "employment": { orderedlist: [{ "organization-name": "Zuncan", "start-date": { date: 1050969600000}, "end-date": { date: 1260662400000} } ]} }
	{ "id": { int32: 10 } , "alias": "Bram", "name": "BramHatch", "user-since": { datetime: 1287223800000}, "friend-ids": { unorderedlist: [{ int32: 1 } , { int32: 5 } , { int32: 9 } ]}, "employment": { orderedlist: [{ "organization-name": "physcane", "start-date": { date: 1181001600000}, "end-date": { date: 1320451200000} } ]} }
	{ "id": { int32: 3 } , "alias": "Emory", "name": "EmoryUnk", "user-since": { datetime: 1341915000000}, "friend-ids": { unorderedlist: [{ int32: 1 } , { int32: 5 } , { int32: 8 } , { int32: 9 } ]}, "employment": { orderedlist: [{ "organization-name": "geomedia", "start-date": { date: 1276732800000}, "end-date": { date: 1264464000000} } ]} }

### Query 8 - Simple Aggregation ###
###### AQL

	use dataverse TinySocial;

    count(for $fbu in dataset FacebookUsers return $fbu);

###### JS

	var expression8 = new FunctionExpression(
        "count",
        new FLWOGRExpression()
            .ForClause("$fbu", new AExpression("dataset FacebookUsers"))
            .ReturnClause("$fbu")
    );

###### Results

	{ int64: 10 }

### Query 9-A - Grouping and Aggregation ###
###### AQL

	use dataverse TinySocial;

    for $t in dataset TweetMessages
    group by $uid := $t.user.screen-name with $t
    return {
        "user": $uid,
        "count": count($t)
    };

###### JS

	var expression9a = new FLWOGRExpression()
        .ForClause("$t", new AExpression("dataset TweetMessages"))
        .GroupClause("$uid", new AExpression("$t.user.screen-name"), "with", "$t")
        .ReturnClause(
            {
                "user" : "$uid",
                "count" : new FunctionExpression("count", new AExpression("$t"))
            }
        );

###### Results

	{ "user": "ColineGeyer@63", "count": { int64: 3 } }
	{ "user": "OliJackson_512", "count": { int64: 1 } }
	{ "user": "NilaMilliron_tw", "count": { int64: 1 } }
	{ "user": "ChangEwing_573", "count": { int64: 1 } }
	{ "user": "NathanGiesen@211", "count": { int64: 6 } }

### Query 9-B - (Hash-Based) Grouping and Aggregation ###
###### AQL

	use dataverse TinySocial;

    for $t in dataset TweetMessages
    /*+ hash*/
    group by $uid := $t.user.screen-name with $t
    return {
        "user": $uid,
        "count": count($t)
    };

###### JS

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

###### Results

	{ "user": "ColineGeyer@63", "count": { int64: 3 } }
	{ "user": "OliJackson_512", "count": { int64: 1 } }
	{ "user": "NilaMilliron_tw", "count": { int64: 1 } }
	{ "user": "ChangEwing_573", "count": { int64: 1 } }
	{ "user": "NathanGiesen@211", "count": { int64: 6 } }

### Query 10 - Grouping and Limits ###
###### AQL

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


###### JS

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

###### Results

	{ "user": "NathanGiesen@211", "count": { int64: 6 } }
	{ "user": "ColineGeyer@63", "count": { int64: 3 } }
	{ "user": "NilaMilliron_tw", "count": { int64: 1 } }

### Query 11 - Left Outer Fuzzy Join ###
###### AQL

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

###### JS

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

###### Results

	{ "tweet": { "tweetid": "10", "user": { "screen-name": "ColineGeyer@63", "lang": "en", "friends_count": { int32: 121 } , "statuses_count": { int32: 362 } , "name": "Coline Geyer", "followers_count": { int32: 17159 } }, "sender-location": { point: [29.15, 76.53]}, "send-time": { datetime: 1201342200000}, "referred-topics": { unorderedlist: ["ccast", "voice-clarity" ]}, "message-text": " hate ccast its voice-clarity is OMG:(" }, "similar-tweets": { orderedlist: [{ unorderedlist: ["x-phone", "voice-clarity" ]}, { unorderedlist: ["ccast", "shortcut-menu" ]}, { unorderedlist: ["ccast", "voicemail-service" ]} ]} }
	{ "tweet": { "tweetid": "6", "user": { "screen-name": "ColineGeyer@63", "lang": "en", "friends_count": { int32: 121 } , "statuses_count": { int32: 362 } , "name": "Coline Geyer", "followers_count": { int32: 17159 } }, "sender-location": { point: [47.51, 83.99]}, "send-time": { datetime: 1273227000000}, "referred-topics": { unorderedlist: ["x-phone", "voice-clarity" ]}, "message-text": " like x-phone the voice-clarity is good:)" }, "similar-tweets": { orderedlist: [{ unorderedlist: ["ccast", "voice-clarity" ]}, { unorderedlist: ["x-phone", "platform" ]} ]} }
	{ "tweet": { "tweetid": "7", "user": { "screen-name": "ChangEwing_573", "lang": "en", "friends_count": { int32: 182 } , "statuses_count": { int32: 394 } , "name": "Chang Ewing", "followers_count": { int32: 32136 } }, "sender-location": { point: [36.21, 72.6]}, "send-time": { datetime: 1314267000000}, "referred-topics": { unorderedlist: ["product-y", "platform" ]}, "message-text": " like product-y the platform is good" }, "similar-tweets": { orderedlist: [{ unorderedlist: ["x-phone", "platform" ]}, { unorderedlist: ["product-y", "voice-command" ]} ]} }
	{ "tweet": { "tweetid": "1", "user": { "screen-name": "NathanGiesen@211", "lang": "en", "friends_count": { int32: 39339 } , "statuses_count": { int32: 473 } , "name": "Nathan Giesen", "followers_count": { int32: 49416 } }, "sender-location": { point: [47.44, 80.65]}, "send-time": { datetime: 1209204600000}, "referred-topics": { unorderedlist: ["product-z", "customization" ]}, "message-text": " love product-z its customization is good:)" }, "similar-tweets": { orderedlist: [{ unorderedlist: ["product-z", "shortcut-menu" ]} ]} }
	{ "tweet": { "tweetid": "12", "user": { "screen-name": "OliJackson_512", "lang": "en", "friends_count": { int32: 445 } , "statuses_count": { int32: 164 } , "name": "Oli Jackson", "followers_count": { int32: 22649 } }, "sender-location": { point: [24.82, 94.63]}, "send-time": { datetime: 1266055800000}, "referred-topics": { unorderedlist: ["product-y", "voice-command" ]}, "message-text": " like product-y the voice-command is amazing:)" }, "similar-tweets": { orderedlist: [{ unorderedlist: ["product-y", "platform" ]}, { unorderedlist: ["product-b", "voice-command" ]} ]} }
	{ "tweet": { "tweetid": "3", "user": { "screen-name": "NathanGiesen@211", "lang": "en", "friends_count": { int32: 39339 } , "statuses_count": { int32: 473 } , "name": "Nathan Giesen", "followers_count": { int32: 49416 } }, "sender-location": { point: [29.72, 75.8]}, "send-time": { datetime: 1162635000000}, "referred-topics": { unorderedlist: ["product-w", "speed" ]}, "message-text": " like product-w the speed is good:)" }, "similar-tweets": { orderedlist: [{ unorderedlist: ["product-w", "speed" ]} ]} }
	{ "tweet": { "tweetid": "9", "user": { "screen-name": "NathanGiesen@211", "lang": "en", "friends_count": { int32: 39339 } , "statuses_count": { int32: 473 } , "name": "Nathan Giesen", "followers_count": { int32: 49416 } }, "sender-location": { point: [36.86, 74.62]}, "send-time": { datetime: 1342865400000}, "referred-topics": { unorderedlist: ["ccast", "voicemail-service" ]}, "message-text": " love ccast its voicemail-service is awesome" }, "similar-tweets": { orderedlist: [{ unorderedlist: ["ccast", "voice-clarity" ]}, { unorderedlist: ["ccast", "shortcut-menu" ]} ]} }
	{ "tweet": { "tweetid": "5", "user": { "screen-name": "NathanGiesen@211", "lang": "en", "friends_count": { int32: 39339 } , "statuses_count": { int32: 473 } , "name": "Nathan Giesen", "followers_count": { int32: 49416 } }, "sender-location": { point: [40.09, 92.69]}, "send-time": { datetime: 1154686200000}, "referred-topics": { unorderedlist: ["product-w", "speed" ]}, "message-text": " can't stand product-w its speed is terrible:(" }, "similar-tweets": { orderedlist: [{ unorderedlist: ["product-w", "speed" ]} ]} }
	{ "tweet": { "tweetid": "8", "user": { "screen-name": "NathanGiesen@211", "lang": "en", "friends_count": { int32: 39339 } , "statuses_count": { int32: 473 } , "name": "Nathan Giesen", "followers_count": { int32: 49416 } }, "sender-location": { point: [46.05, 93.34]}, "send-time": { datetime: 1129284600000}, "referred-topics": { unorderedlist: ["product-z", "shortcut-menu" ]}, "message-text": " like product-z the shortcut-menu is awesome:)" }, "similar-tweets": { orderedlist: [{ unorderedlist: ["ccast", "shortcut-menu" ]}, { unorderedlist: ["product-z", "customization" ]} ]} }
	{ "tweet": { "tweetid": "11", "user": { "screen-name": "NilaMilliron_tw", "lang": "en", "friends_count": { int32: 445 } , "statuses_count": { int32: 164 } , "name": "Nila Milliron", "followers_count": { int32: 22649 } }, "sender-location": { point: [37.59, 68.42]}, "send-time": { datetime: 1205057400000}, "referred-topics": { unorderedlist: ["x-phone", "platform" ]}, "message-text": " can't stand x-phone its platform is terrible" }, "similar-tweets": { orderedlist: [{ unorderedlist: ["x-phone", "voice-clarity" ]}, { unorderedlist: ["product-y", "platform" ]} ]} }
	{ "tweet": { "tweetid": "2", "user": { "screen-name": "ColineGeyer@63", "lang": "en", "friends_count": { int32: 121 } , "statuses_count": { int32: 362 } , "name": "Coline Geyer", "followers_count": { int32: 17159 } }, "sender-location": { point: [32.84, 67.14]}, "send-time": { datetime: 1273745400000}, "referred-topics": { unorderedlist: ["ccast", "shortcut-menu" ]}, "message-text": " like ccast its shortcut-menu is awesome:)" }, "similar-tweets": { orderedlist: [{ unorderedlist: ["product-z", "shortcut-menu" ]}, { unorderedlist: ["ccast", "voice-clarity" ]}, { unorderedlist: ["ccast", "voicemail-service" ]} ]} }
	{ "tweet": { "tweetid": "4", "user": { "screen-name": "NathanGiesen@211", "lang": "en", "friends_count": { int32: 39339 } , "statuses_count": { int32: 473 } , "name": "Nathan Giesen", "followers_count": { int32: 49416 } }, "sender-location": { point: [39.28, 70.48]}, "send-time": { datetime: 1324894200000}, "referred-topics": { unorderedlist: ["product-b", "voice-command" ]}, "message-text": " like product-b the voice-command is mind-blowing:)" }, "similar-tweets": { orderedlist: [{ unorderedlist: ["product-y", "voice-command" ]} ]} }
