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

# AsterixDB 101: An ADM and AQL Primer #

## Welcome to AsterixDB! ##
This document introduces the main features of AsterixDB's data model (ADM) and query language (AQL) by example.
The example is a simple scenario involving (synthetic) sample data modeled after data from the social domain.
This document describes a set of sample ADM datasets, together with a set of illustrative AQL queries,
to introduce you to the "AsterixDB user experience".
The complete set of steps required to create and load a handful of sample datasets, along with runnable queries
and the expected results for each query, are included.

This document assumes that you are at least vaguely familiar with AsterixDB and why you might want to use it.
Most importantly, it assumes you already have a running instance of AsterixDB and that you know how to query
it using AsterixDB's basic web interface.
For more information on these topics, you should go through the steps in
[Installing Asterix Using Managix](../install.html)
before reading this document and make sure that you have a running AsterixDB instance ready to go.
To get your feet wet, you should probably start with a simple local installation of AsterixDB on your favorite
machine, accepting all of the default settings that Managix offers.
Later you can graduate to trying AsterixDB on a cluster, its real intended home (since it targets Big Data).
(Note: With the exception of specifying the correct locations where you put the source data for this example,
there should no changes needed in your ADM or AQL statements to run the examples locally and/or to run them
on a cluster when you are ready to take that step.)

As you read through this document, you should try each step for yourself on your own AsterixDB instance.
Once you have reached the end, you will be fully armed and dangerous, with all the basic AsterixDB knowledge
that you'll need to start down the path of modeling, storing, and querying your own semistructured data.

## ADM: Modeling Semistructured Data in AsterixDB ##
In this section you will learn all about modeling Big Data using
ADM, the data model of the AsterixDB BDMS.

### Dataverses, Datatypes, and Datasets ###
The top-level organizing concept in the AsterixDB world is the _dataverse_.
A dataverse---short for "data universe"---is a place (similar to a database in a relational DBMS) in which
to create and manage the types, datasets, functions, and other artifacts for a given AsterixDB application.
When you start using an AsterixDB instance for the first time, it starts out "empty"; it contains no data
other than the AsterixDB system catalogs (which live in a special dataverse called the Metadata dataverse).
To store your data in AsterixDB, you will first create a dataverse and then you use it for the _datatypes_
and _datasets_ for managing your own data.
A datatype tells AsterixDB what you know (or more accurately, what you want it to know) a priori about one
of the kinds of data instances that you want AsterixDB to hold for you.
A dataset is a collection of data instances of a datatype,
and AsterixDB makes sure that the data instances that you put in it conform to its specified type.
Since AsterixDB targets semistructured data, you can use _open_ datatypes and tell it as little or as
much as you wish about your data up front; the more you tell it up front, the less information it will
have to store repeatedly in the individual data instances that you give it.
Instances of open datatypes are permitted to have additional content, beyond what the datatype says,
as long as they at least contain the information prescribed by the datatype definition.
Open typing allows data to vary from one instance to another and it leaves wiggle room for application
evolution in terms of what might need to be stored in the future.
If you want to restrict data instances in a dataset to have only what the datatype says, and nothing extra,
you can define a _closed_ datatype for that dataset and AsterixDB will keep users from storing objects
that have extra data in them.
Datatypes are open by default unless you tell AsterixDB otherwise.
Let's put these concepts to work

Our little sample scenario involves information about users of two hypothetical social networks,
Gleambook and Chirp, and their messages.
We'll start by defining a dataverse called "TinySocial" to hold our datatypes and datasets.
The AsterixDB data model (ADM) is essentially a superset of JSON---it's what you get by extending
JSON with more data types and additional data modeling constructs borrowed from object databases.
The following shows how we can create the TinySocial dataverse plus a set of ADM types for modeling
Chirp users, their Chirps, Gleambook users, their users' employment information, and their messages.
(Note: Keep in mind that this is just a tiny and somewhat silly example intended for illustrating
some of the key features of AsterixDB. :-))


        drop dataverse TinySocial if exists;
        create dataverse TinySocial;
        use dataverse TinySocial;

        create type ChirpUserType as {
            screenName: string,
            lang: string,
            friendsCount: int,
            statusesCount: int,
            name: string,
            followersCount: int
        };

        create type ChirpMessageType as closed {
            chirpId: string,
            user: ChirpUserType,
            senderLocation: point?,
            sendTime: datetime,
            referredTopics: {{ string }},
            messageText: string
        };

        create type EmploymentType as {
            organizationName: string,
            startDate: date,
            endDate: date?
        };

        create type GleambookUserType as {
            id: int,
            alias: string,
            name: string,
            userSince: datetime,
            friendIds: {{ int }},
            employment: [EmploymentType]
        };

        create type GleambookMessageType as {
            messageId: int,
            authorId: int,
            inResponseTo: int?,
            senderLocation: point?,
            message: string
        };

The first three lines above tell AsterixDB to drop the old TinySocial dataverse, if one already
exists, and then to create a brand new one and make it the focus of the statements that follow.
The first _create type_ statement creates a datatype for holding information about Chirp users.
It is a object type with a mix of integer and string data, very much like a (flat) relational tuple.
The indicated fields are all mandatory, but because the type is open, additional fields are welcome.
The second statement creates a datatype for Chirp messages; this shows how to specify a closed type.
Interestingly (based on one of Chirp's APIs), each Chirp message actually embeds an instance of the
sending user's information (current as of when the message was sent), so this is an example of a nested
object in ADM.
Chirp messages can optionally contain the sender's location, which is modeled via the senderLocation
field of spatial type _point_; the question mark following the field type indicates its optionality.
An optional field is like a nullable field in SQL---it may be present or missing, but when it's present,
its value's data type will conform to the datatype's specification.
The sendTime field illustrates the use of a temporal primitive type, _datetime_.
Lastly, the referredTopics field illustrates another way that ADM is richer than the relational model;
this field holds a bag (*a.k.a.* an unordered list) of strings.
Since the overall datatype definition for Chirp messages says "closed", the fields that it lists are
the only fields that instances of this type will be allowed to contain.
The next two _create type_ statements create a object type for holding information about one component of
the employment history of a Gleambook user and then a object type for holding the user information itself.
The Gleambook user type highlights a few additional ADM data model features.
Its friendIds field is a bag of integers, presumably the Gleambook user ids for this user's friends,
and its employment field is an ordered list of employment objects.
The final _create type_ statement defines a type for handling the content of a Gleambook message in our
hypothetical social data storage scenario.

Before going on, we need to once again emphasize the idea that AsterixDB is aimed at storing
and querying not just Big Data, but Big _Semistructured_ Data.
This means that most of the fields listed in the _create type_ statements above could have been
omitted without changing anything other than the resulting size of stored data instances on disk.
AsterixDB stores its information about the fields defined a priori as separate metadata, whereas
the information about other fields that are "just there" in instances of open datatypes is stored
with each instance---making for more bits on disk and longer times for operations affected by
data size (e.g., dataset scans).
The only fields that _must_ be specified a priori are the primary key fields of each dataset.

### Creating Datasets and Indexes ###

Now that we have defined our datatypes, we can move on and create datasets to store the actual data.
(If we wanted to, we could even have several named datasets based on any one of these datatypes.)
We can do this as follows, utilizing the DDL capabilities of AsterixDB.



        use dataverse TinySocial;

        create dataset GleambookUsers(GleambookUserType)
            primary key id;

        create dataset GleambookMessages(GleambookMessageType)
            primary key messageId;

        create dataset ChirpUsers(ChirpUserType)
            primary key screenName;

        create dataset ChirpMessages(ChirpMessageType)
            primary key chirpId
            hints(cardinality=100);

        create index gbUserSinceIdx on GleambookUsers(userSince);
        create index gbAuthorIdx on GleambookMessages(authorId) type btree;
        create index gbSenderLocIndex on GleambookMessages(senderLocation) type rtree;
        create index gbMessageIdx on GleambookMessages(message) type keyword;

        for $ds in dataset Metadata.Dataset return $ds;
        for $ix in dataset Metadata.Index return $ix;

The DDL statements above create four datasets for holding our social data in the TinySocial
dataverse: GleambookUsers, GleambookMessages, ChirpUsers, and ChirpMessages.
The first _create dataset_ statement creates the GleambookUsers data set.
It specifies that this dataset will store data instances conforming to GleambookUserType and that
it has a primary key which is the id field of each instance.
The primary key information is used by AsterixDB to uniquely identify instances for the purpose
of later lookup and for use in secondary indexes.
Each AsterixDB dataset is stored (and indexed) in the form of a B+ tree on primary key;
secondary indexes point to their indexed data by primary key.
In AsterixDB clusters, the primary key is also used to hash-partition (*a.k.a.* shard) the
dataset across the nodes of the cluster.
The next three _create dataset_ statements are similar.
The last one illustrates an optional clause for providing useful hints to AsterixDB.
In this case, the hint tells AsterixDB that the dataset definer is anticipating that the
ChirpMessages dataset will contain roughly 100 objects; knowing this can help AsterixDB
to more efficiently manage and query this dataset.
(AsterixDB does not yet gather and maintain data statistics; it will currently, abitrarily,
assume a cardinality of one million objects per dataset in the absence of such an optional
definition-time hint.)

The _create dataset_ statements above are followed by four more DDL statements, each of which
creates a secondary index on a field of one of the datasets.
The first one indexes the GleambookUsers dataset on its userSince field.
This index will be a B+ tree index; its type is unspecified and _btree_ is the default type.
The other three illustrate how you can explicitly specify the desired type of index.
In addition to btree, _rtree_ and inverted _keyword_ indexes are supported by AsterixDB.
Indexes can also have composite keys, and more advanced text indexing is available as well
(ngram(k), where k is the desired gram length).

### Querying the Metadata Dataverse ###

The last two statements above show how you can use queries in AQL to examine the AsterixDB
system catalogs and tell what artifacts you have created.
Just as relational DBMSs use their own tables to store their catalogs, AsterixDB uses
its own datasets to persist descriptions of its datasets, datatypes, indexes, and so on.
Running the first of the two queries above will list all of your newly created datasets,
and it will also show you a full list of all the metadata datasets.
(You can then explore from there on your own if you are curious)
These last two queries also illustrate one other factoid worth knowing:
AsterixDB allows queries to span dataverses by allowing the optional use
of fully-qualified dataset names (i.e., _dataversename.datasetname_)
to reference datasets that live in a dataverse other than the one that
was named in the most recently executed _use dataverse_ directive.

## Loading Data Into AsterixDB ##
Okay, so far so good---AsterixDB is now ready for data, so let's give it some data to store.
Our next task will be to insert some sample data into the four datasets that we just defined.
Here we will load a tiny set of objects, defined in ADM format (a superset of JSON), into each dataset.
In the boxes below you can see insert statements with a list of the objects to be inserted. The files
themselves are also linked.
Take a few minutes to look carefully at each of the sample data sets.
This will give you a better sense of the nature of the data that we are about to load and query.
We should note that ADM format is a textual serialization of what AsterixDB will actually store;
when persisted in AsterixDB, the data format will be binary and the data in the predefined fields
of the data instances will be stored separately from their associated field name and type metadata.

[Chirp Users](../data/chu.adm)

        use dataverse TinySocial;

        insert into dataset ChirpUsers
        ([
        {"screenName":"NathanGiesen@211","lang":"en","friendsCount":18,"statusesCount":473,"name":"Nathan Giesen","followersCount":49416},
        {"screenName":"ColineGeyer@63","lang":"en","friendsCount":121,"statusesCount":362,"name":"Coline Geyer","followersCount":17159},
        {"screenName":"NilaMilliron_tw","lang":"en","friendsCount":445,"statusesCount":164,"name":"Nila Milliron","followersCount":22649},
        {"screenName":"ChangEwing_573","lang":"en","friendsCount":182,"statusesCount":394,"name":"Chang Ewing","followersCount":32136}
        ]);


[Chirp Messages](../data/chm.adm)

        use dataverse TinySocial;

        insert into dataset ChirpMessages
        ([
        {"chirpId":"1","user":{"screenName":"NathanGiesen@211","lang":"en","friendsCount":39339,"statusesCount":473,"name":"Nathan Giesen","followersCount":49416},"senderLocation":point("47.44,80.65"),"sendTime":datetime("2008-04-26T10:10:00"),"referredTopics":{{"product-z","customization"}},"messageText":" love product-z its customization is good:)"},
        {"chirpId":"2","user":{"screenName":"ColineGeyer@63","lang":"en","friendsCount":121,"statusesCount":362,"name":"Coline Geyer","followersCount":17159},"senderLocation":point("32.84,67.14"),"sendTime":datetime("2010-05-13T10:10:00"),"referredTopics":{{"ccast","shortcut-menu"}},"messageText":" like ccast its shortcut-menu is awesome:)"},
        {"chirpId":"3","user":{"screenName":"NathanGiesen@211","lang":"en","friendsCount":39339,"statusesCount":473,"name":"Nathan Giesen","followersCount":49416},"senderLocation":point("29.72,75.8"),"sendTime":datetime("2006-11-04T10:10:00"),"referredTopics":{{"product-w","speed"}},"messageText":" like product-w the speed is good:)"},
        {"chirpId":"4","user":{"screenName":"NathanGiesen@211","lang":"en","friendsCount":39339,"statusesCount":473,"name":"Nathan Giesen","followersCount":49416},"senderLocation":point("39.28,70.48"),"sendTime":datetime("2011-12-26T10:10:00"),"referredTopics":{{"product-b","voice-command"}},"messageText":" like product-b the voice-command is mind-blowing:)"},
        {"chirpId":"5","user":{"screenName":"NathanGiesen@211","lang":"en","friendsCount":39339,"statusesCount":473,"name":"Nathan Giesen","followersCount":49416},"senderLocation":point("40.09,92.69"),"sendTime":datetime("2006-08-04T10:10:00"),"referredTopics":{{"product-w","speed"}},"messageText":" can't stand product-w its speed is terrible:("},
        {"chirpId":"6","user":{"screenName":"ColineGeyer@63","lang":"en","friendsCount":121,"statusesCount":362,"name":"Coline Geyer","followersCount":17159},"senderLocation":point("47.51,83.99"),"sendTime":datetime("2010-05-07T10:10:00"),"referredTopics":{{"x-phone","voice-clarity"}},"messageText":" like x-phone the voice-clarity is good:)"},
        {"chirpId":"7","user":{"screenName":"ChangEwing_573","lang":"en","friendsCount":182,"statusesCount":394,"name":"Chang Ewing","followersCount":32136},"senderLocation":point("36.21,72.6"),"sendTime":datetime("2011-08-25T10:10:00"),"referredTopics":{{"product-y","platform"}},"messageText":" like product-y the platform is good"},
        {"chirpId":"8","user":{"screenName":"NathanGiesen@211","lang":"en","friendsCount":39339,"statusesCount":473,"name":"Nathan Giesen","followersCount":49416},"senderLocation":point("46.05,93.34"),"sendTime":datetime("2005-10-14T10:10:00"),"referredTopics":{{"product-z","shortcut-menu"}},"messageText":" like product-z the shortcut-menu is awesome:)"},
        {"chirpId":"9","user":{"screenName":"NathanGiesen@211","lang":"en","friendsCount":39339,"statusesCount":473,"name":"Nathan Giesen","followersCount":49416},"senderLocation":point("36.86,74.62"),"sendTime":datetime("2012-07-21T10:10:00"),"referredTopics":{{"ccast","voicemail-service"}},"messageText":" love ccast its voicemail-service is awesome"},
        {"chirpId":"10","user":{"screenName":"ColineGeyer@63","lang":"en","friendsCount":121,"statusesCount":362,"name":"Coline Geyer","followersCount":17159},"senderLocation":point("29.15,76.53"),"sendTime":datetime("2008-01-26T10:10:00"),"referredTopics":{{"ccast","voice-clarity"}},"messageText":" hate ccast its voice-clarity is OMG:("},
        {"chirpId":"11","user":{"screenName":"NilaMilliron_tw","lang":"en","friendsCount":445,"statusesCount":164,"name":"Nila Milliron","followersCount":22649},"senderLocation":point("37.59,68.42"),"sendTime":datetime("2008-03-09T10:10:00"),"referredTopics":{{"x-phone","platform"}},"messageText":" can't stand x-phone its platform is terrible"},
        {"chirpId":"12","user":{"screenName":"OliJackson_512","lang":"en","friendsCount":445,"statusesCount":164,"name":"Oli Jackson","followersCount":22649},"senderLocation":point("24.82,94.63"),"sendTime":datetime("2010-02-13T10:10:00"),"referredTopics":{{"product-y","voice-command"}},"messageText":" like product-y the voice-command is amazing:)"}
        ]);

[Gleambook Users](../data/gbu.adm)

        use dataverse TinySocial;

        insert into dataset GleambookUsers
        ([
        {"id":1,"alias":"Margarita","name":"MargaritaStoddard","nickname":"Mags","userSince":datetime("2012-08-20T10:10:00"),"friendIds":{{2,3,6,10}},"employment":[{"organizationName":"Codetechno","startDate":date("2006-08-06")},{"organizationName":"geomedia","startDate":date("2010-06-17"),"endDate":date("2010-01-26")}],"gender":"F"},
        {"id":2,"alias":"Isbel","name":"IsbelDull","nickname":"Izzy","userSince":datetime("2011-01-22T10:10:00"),"friendIds":{{1,4}},"employment":[{"organizationName":"Hexviafind","startDate":date("2010-04-27")}]},
        {"id":3,"alias":"Emory","name":"EmoryUnk","userSince":datetime("2012-07-10T10:10:00"),"friendIds":{{1,5,8,9}},"employment":[{"organizationName":"geomedia","startDate":date("2010-06-17"),"endDate":date("2010-01-26")}]},
        {"id":4,"alias":"Nicholas","name":"NicholasStroh","userSince":datetime("2010-12-27T10:10:00"),"friendIds":{{2}},"employment":[{"organizationName":"Zamcorporation","startDate":date("2010-06-08")}]},
        {"id":5,"alias":"Von","name":"VonKemble","userSince":datetime("2010-01-05T10:10:00"),"friendIds":{{3,6,10}},"employment":[{"organizationName":"Kongreen","startDate":date("2010-11-27")}]},
        {"id":6,"alias":"Willis","name":"WillisWynne","userSince":datetime("2005-01-17T10:10:00"),"friendIds":{{1,3,7}},"employment":[{"organizationName":"jaydax","startDate":date("2009-05-15")}]},
        {"id":7,"alias":"Suzanna","name":"SuzannaTillson","userSince":datetime("2012-08-07T10:10:00"),"friendIds":{{6}},"employment":[{"organizationName":"Labzatron","startDate":date("2011-04-19")}]},
        {"id":8,"alias":"Nila","name":"NilaMilliron","userSince":datetime("2008-01-01T10:10:00"),"friendIds":{{3}},"employment":[{"organizationName":"Plexlane","startDate":date("2010-02-28")}]},
        {"id":9,"alias":"Woodrow","name":"WoodrowNehling","nickname":"Woody","userSince":datetime("2005-09-20T10:10:00"),"friendIds":{{3,10}},"employment":[{"organizationName":"Zuncan","startDate":date("2003-04-22"),"endDate":date("2009-12-13")}]},
        {"id":10,"alias":"Bram","name":"BramHatch","userSince":datetime("2010-10-16T10:10:00"),"friendIds":{{1,5,9}},"employment":[{"organizationName":"physcane","startDate":date("2007-06-05"),"endDate":date("2011-11-05")}]}
        ]);

[Gleambook Messages](../data/gbm.adm)

        use dataverse TinySocial;

        insert into dataset GleambookMessages
        ([
        {"messageId":1,"authorId":3,"inResponseTo":2,"senderLocation":point("47.16,77.75"),"message":" love product-b its shortcut-menu is awesome:)"},
        {"messageId":2,"authorId":1,"inResponseTo":4,"senderLocation":point("41.66,80.87"),"message":" dislike x-phone its touch-screen is horrible"},
        {"messageId":3,"authorId":2,"inResponseTo":4,"senderLocation":point("48.09,81.01"),"message":" like product-y the plan is amazing"},
        {"messageId":4,"authorId":1,"inResponseTo":2,"senderLocation":point("37.73,97.04"),"message":" can't stand acast the network is horrible:("},
        {"messageId":5,"authorId":6,"inResponseTo":2,"senderLocation":point("34.7,90.76"),"message":" love product-b the customization is mind-blowing"},
        {"messageId":6,"authorId":2,"inResponseTo":1,"senderLocation":point("31.5,75.56"),"message":" like product-z its platform is mind-blowing"},
        {"messageId":7,"authorId":5,"inResponseTo":15,"senderLocation":point("32.91,85.05"),"message":" dislike product-b the speed is horrible"},
        {"messageId":8,"authorId":1,"inResponseTo":11,"senderLocation":point("40.33,80.87"),"message":" like ccast the 3G is awesome:)"},
        {"messageId":9,"authorId":3,"inResponseTo":12,"senderLocation":point("34.45,96.48"),"message":" love ccast its wireless is good"},
        {"messageId":10,"authorId":1,"inResponseTo":12,"senderLocation":point("42.5,70.01"),"message":" can't stand product-w the touch-screen is terrible"},
        {"messageId":11,"authorId":1,"inResponseTo":1,"senderLocation":point("38.97,77.49"),"message":" can't stand acast its plan is terrible"},
        {"messageId":12,"authorId":10,"inResponseTo":6,"senderLocation":point("42.26,77.76"),"message":" can't stand product-z its voicemail-service is OMG:("},
        {"messageId":13,"authorId":10,"inResponseTo":4,"senderLocation":point("42.77,78.92"),"message":" dislike x-phone the voice-command is bad:("},
        {"messageId":14,"authorId":9,"inResponseTo":12,"senderLocation":point("41.33,85.28"),"message":" love acast its 3G is good:)"},
        {"messageId":15,"authorId":7,"inResponseTo":11,"senderLocation":point("44.47,67.11"),"message":" like x-phone the voicemail-service is awesome"}
        ]);

## AQL: Querying Your AsterixDB Data ##
Congratulations! You now have sample social data stored (and indexed) in AsterixDB.
(You are part of an elite and adventurous group of individuals. :-))
Now that you have successfully loaded the provided sample data into the datasets that we defined,
you can start running queries against them.

The query language for AsterixDB is AQL---the Asterix Query Language.
AQL is loosely based on XQuery, the language developed and standardized in the early to mid 2000's
by the World Wide Web Consortium (W3C) for querying semistructured data stored in their XML format.
We have tossed all of the "XML cruft" out of their language but retained many of its core ideas.
We did this because its design was developed over a period of years by a diverse committee of smart
and experienced language designers, including "SQL people", "functional programming people", and
"XML people", all of whom were focused on how to design a new query language that operates well over
semistructured data.
(We decided to stand on their shoulders instead of starting from scratch and revisiting many of the
same issues.)
Note that AQL is not SQL and not based on SQL: In other words, AsterixDB is fully "NoSQL compliant". :-)

In this section we introduce AQL via a set of example queries, along with their expected results,
based on the data above, to help you get started.
Many of the most important features of AQL are presented in this set of representative queries.
You can find more details in the document on the [Asterix Data Model (ADM)](datamodel.html),
in the [AQL Reference Manual](manual.html), and a complete list of built-in functions is available
in the [Asterix Functions](functions.html) document.

AQL is an expression language.
Even the expression 1+1 is a valid AQL query that evaluates to 2.
(Try it for yourself!
Okay, maybe that's _not_ the best use of a 512-node shared-nothing compute cluster.)
Most useful AQL queries will be based on the _FLWOR_ (pronounced "flower") expression structure
that AQL has borrowed from XQuery ((http://en.wikipedia.org/wiki/FLWOR)).
The FLWOR expression syntax supports both the incremental binding (_for_) of variables to ADM data
instances in a dataset (or in the result of any AQL expression, actually) and the full binding (_let_)
of variables to entire intermediate results in a fashion similar to temporary views in the SQL world.
FLWOR is an acronym that is short for _for_-_let_-_where_-_order by_-_return_,
naming five of the most frequently used clauses from the syntax of a full AQL query.
AQL also includes _group by_ and _limit_ clauses, as you will see shortly.
Roughly speaking, for SQL afficiandos, the _for_ clause in AQL is like the _from_ clause in SQL,
the _return_ clause in AQL is like the _select_ clause in SQL (but appears at the end instead of
the beginning of a query), the _let_ clause in AQL is like SQL's _with_ clause, and the _where_
and _order by_ clauses in both languages are similar.

Based on user demand, in order to let SQL afficiandos to write AQL queries in their favored ways,
AQL supports a few synonyms:  _from_ for _for_, _select_ for _return_,  _with_ for _let_, and
_keeping_ for _with_ in the group by clause.
These have been found to help die-hard SQL fans to feel a little more at home in AQL and to be less
likely to (mis)interpret _for_ as imperative looping, _return_ as returning from a function call,
and so on.

Enough talk!
Let's go ahead and try writing some queries and see about learning AQL by example.

### Query 0-A - Exact-Match Lookup ###
For our first query, let's find a Gleambook user based on his or her user id.
Suppose the user we want is the user whose id is 8:


        use dataverse TinySocial;

        for $user in dataset GleambookUsers
        where $user.id = 8
        return $user;

The query's _for_ clause  binds the variable `$user` incrementally to the data instances residing in
the dataset named GleambookUsers.
Its _where_ clause selects only those bindings having a user id of interest, filtering out the rest.
The _return_ clause returns the (entire) data instance for each binding that satisfies the predicate.
Since this dataset is indexed on user id (its primary key), this query will be done via a quick index lookup.

The expected result for our sample data is as follows:

        { "id": 8, "alias": "Nila", "name": "NilaMilliron", "userSince": datetime("2008-01-01T10:10:00.000Z"), "friendIds": {{ 3 }}, "employment": [ { "organizationName": "Plexlane", "startDate": date("2010-02-28") } ] }


Note the using the SQL keyword synonyms, another way of phrasing the same query would be:

        use dataverse TinySocial;

        from $user in dataset GleambookUsers
        where $user.id = 8
        select $user;

### Query 0-B - Range Scan ###
AQL, like SQL, supports a variety of different predicates.
For example, for our next query, let's find the Gleambook users whose ids are in the range between 2 and 4:

        use dataverse TinySocial;

        for $user in dataset GleambookUsers
        where $user.id >= 2 and $user.id <= 4
        return $user;

This query's expected result, also evaluable using the primary index on user id, is:

        { "id": 2, "alias": "Isbel", "name": "IsbelDull", "userSince": datetime("2011-01-22T10:10:00.000Z"), "friendIds": {{ 1, 4 }}, "employment": [ { "organizationName": "Hexviafind", "startDate": date("2010-04-27") } ], "nickname": "Izzy" }
        { "id": 4, "alias": "Nicholas", "name": "NicholasStroh", "userSince": datetime("2010-12-27T10:10:00.000Z"), "friendIds": {{ 2 }}, "employment": [ { "organizationName": "Zamcorporation", "startDate": date("2010-06-08") } ] }
        { "id": 3, "alias": "Emory", "name": "EmoryUnk", "userSince": datetime("2012-07-10T10:10:00.000Z"), "friendIds": {{ 1, 5, 8, 9 }}, "employment": [ { "organizationName": "geomedia", "startDate": date("2010-06-17"), "endDate": date("2010-01-26") } ] }

### Query 1 - Other Query Filters ###
AQL can do range queries on any data type that supports the appropriate set of comparators.
As an example, this next query retrieves the Gleambook users who joined between July 22, 2010 and July 29, 2012:

        use dataverse TinySocial;

        for $user in dataset GleambookUsers
        where $user.userSince >= datetime('2010-07-22T00:00:00')
          and $user.userSince <= datetime('2012-07-29T23:59:59')
        return $user;

The expected result for this query, also an indexable query, is as follows:

        { "id": 2, "alias": "Isbel", "name": "IsbelDull", "userSince": datetime("2011-01-22T10:10:00.000Z"), "friendIds": {{ 1, 4 }}, "employment": [ { "organizationName": "Hexviafind", "startDate": date("2010-04-27") } ], "nickname": "Izzy" }
        { "id": 4, "alias": "Nicholas", "name": "NicholasStroh", "userSince": datetime("2010-12-27T10:10:00.000Z"), "friendIds": {{ 2 }}, "employment": [ { "organizationName": "Zamcorporation", "startDate": date("2010-06-08") } ] }
        { "id": 10, "alias": "Bram", "name": "BramHatch", "userSince": datetime("2010-10-16T10:10:00.000Z"), "friendIds": {{ 1, 5, 9 }}, "employment": [ { "organizationName": "physcane", "startDate": date("2007-06-05"), "endDate": date("2011-11-05") } ] }
        { "id": 3, "alias": "Emory", "name": "EmoryUnk", "userSince": datetime("2012-07-10T10:10:00.000Z"), "friendIds": {{ 1, 5, 8, 9 }}, "employment": [ { "organizationName": "geomedia", "startDate": date("2010-06-17"), "endDate": date("2010-01-26") } ] }

### Query 2-A - Equijoin ###
In addition to simply binding variables to data instances and returning them "whole",
an AQL query can construct new ADM instances to return based on combinations of its variable bindings.
This gives AQL the power to do joins much like those done using multi-table _from_ clauses in SQL.
For example, suppose we wanted a list of all Gleambook users paired with their associated messages,
with the list enumerating the author name and the message text associated with each Gleambook message.
We could do this as follows in AQL:

        use dataverse TinySocial;

        for $user in dataset GleambookUsers
        for $message in dataset GleambookMessages
        where $message.authorId = $user.id
        return {
            "uname": $user.name,
            "message": $message.message
        };

The result of this query is a sequence of new ADM instances, one for each author/message pair.
Each instance in the result will be an ADM object containing two fields, "uname" and "message",
containing the user's name and the message text, respectively, for each author/message pair.
(Note that "uname" and "message" are both simple AQL expressions themselves---so in the most
general case, even the resulting field names can be computed as part of the query, making AQL
a very powerful tool for slicing and dicing semistructured data.)

The expected result of this example AQL join query for our sample data set is:

        { "uname": "WillisWynne", "message": " love product-b the customization is mind-blowing" }
        { "uname": "MargaritaStoddard", "message": " can't stand acast its plan is terrible" }
        { "uname": "MargaritaStoddard", "message": " dislike x-phone its touch-screen is horrible" }
        { "uname": "MargaritaStoddard", "message": " can't stand acast the network is horrible:(" }
        { "uname": "MargaritaStoddard", "message": " like ccast the 3G is awesome:)" }
        { "uname": "MargaritaStoddard", "message": " can't stand product-w the touch-screen is terrible" }
        { "uname": "IsbelDull", "message": " like product-z its platform is mind-blowing" }
        { "uname": "IsbelDull", "message": " like product-y the plan is amazing" }
        { "uname": "WoodrowNehling", "message": " love acast its 3G is good:)" }
        { "uname": "BramHatch", "message": " can't stand product-z its voicemail-service is OMG:(" }
        { "uname": "BramHatch", "message": " dislike x-phone the voice-command is bad:(" }
        { "uname": "EmoryUnk", "message": " love product-b its shortcut-menu is awesome:)" }
        { "uname": "EmoryUnk", "message": " love ccast its wireless is good" }
        { "uname": "VonKemble", "message": " dislike product-b the speed is horrible" }
        { "uname": "SuzannaTillson", "message": " like x-phone the voicemail-service is awesome" }

Again, as an aside, note that the same query expressed using AQL's SQL keyword synonyms would be:

        use dataverse TinySocial;

        from $user in dataset GleambookUsers
        from $message in dataset GleambookMessages
        where $message.authorId = $user.id
        select {
            "uname": $user.name,
            "message": $message.message
        };

### Query 2-B - Index join ###
By default, AsterixDB evaluates equijoin queries using hash-based join methods that work
well for doing ad hoc joins of very large data sets
([http://en.wikipedia.org/wiki/Hash_join](http://en.wikipedia.org/wiki/Hash_join)).
On a cluster, hash partitioning is employed as AsterixDB's divide-and-conquer strategy for
computing large parallel joins.
AsterixDB includes other join methods, but in the absence of data statistics and selectivity
estimates, it doesn't (yet) have the know-how to intelligently choose among its alternatives.
We therefore asked ourselves the classic question---WWOD?---What Would Oracle Do?---and in the
interim, AQL includes a clunky (but useful) hint-based mechanism for addressing the occasional
need to suggest to AsterixDB which join method it should use for a particular AQL query.

The following query is similar to Query 2-A but includes a suggestion to AsterixDB that it
should consider employing an index-based nested-loop join technique to process the query:

        use dataverse TinySocial;

        for $user in dataset GleambookUsers
        for $message in dataset GleambookMessages
        where $message.authorId /*+ indexnl */  = $user.id
        return {
            "uname": $user.name,
            "message": $message.message
        };


The expected result is (of course) the same as before, modulo the order of the instances.
Result ordering is (intentionally) undefined in AQL in the absence of an _order by_ clause.
The query result for our sample data in this case is:

        { "uname": "IsbelDull", "message": " like product-z its platform is mind-blowing" }
        { "uname": "MargaritaStoddard", "message": " can't stand acast its plan is terrible" }
        { "uname": "BramHatch", "message": " can't stand product-z its voicemail-service is OMG:(" }
        { "uname": "WoodrowNehling", "message": " love acast its 3G is good:)" }
        { "uname": "EmoryUnk", "message": " love product-b its shortcut-menu is awesome:)" }
        { "uname": "MargaritaStoddard", "message": " dislike x-phone its touch-screen is horrible" }
        { "uname": "MargaritaStoddard", "message": " can't stand acast the network is horrible:(" }
        { "uname": "BramHatch", "message": " dislike x-phone the voice-command is bad:(" }
        { "uname": "SuzannaTillson", "message": " like x-phone the voicemail-service is awesome" }
        { "uname": "MargaritaStoddard", "message": " like ccast the 3G is awesome:)" }
        { "uname": "EmoryUnk", "message": " love ccast its wireless is good" }
        { "uname": "MargaritaStoddard", "message": " can't stand product-w the touch-screen is terrible" }
        { "uname": "IsbelDull", "message": " like product-y the plan is amazing" }
        { "uname": "WillisWynne", "message": " love product-b the customization is mind-blowing" }
        { "uname": "VonKemble", "message": " dislike product-b the speed is horrible" }

(It is worth knowing, with respect to influencing AsterixDB's query evaluation, that nested _for_
clauses---a.k.a. joins--- are currently evaluated with the "outer" clause probing the data of the "inner"
clause.)

### Query 3 - Nested Outer Join ###
In order to support joins between tables with missing/dangling join tuples, the designers of SQL ended
up shoe-horning a subset of the relational algebra into SQL's _from_ clause syntax---and providing a
variety of join types there for users to choose from.
Left outer joins are particularly important in SQL, e.g., to print a summary of customers and orders,
grouped by customer, without omitting those customers who haven't placed any orders yet.

The AQL language supports nesting, both of queries and of query results, and the combination allows for
an arguably cleaner/more natural approach to such queries.
As an example, supposed we wanted, for each Gleambook user, to produce a object that has his/her name
plus a list of the messages written by that user.
In SQL, this would involve a left outer join between users and messages, grouping by user, and having
the user name repeated along side each message.
In AQL, this sort of use case can be handled (more naturally) as follows:

        use dataverse TinySocial;

        for $user in dataset GleambookUsers
        return {
            "uname": $user.name,
            "messages": for $message in dataset GleambookMessages
                        where $message.authorId = $user.id
                        return $message.message
        };

This AQL query binds the variable `$user` to the data instances in GleambookUsers;
for each user, it constructs a result object containing a "uname" field with the user's
name and a "messages" field with a nested collection of all messages for that user.
The nested collection for each user is specified by using a correlated subquery.
(Note: While it looks like nested loops could be involved in computing the result,
AsterixDB recogizes the equivalence of such a query to an outerjoin, and it will
use an efficient hash-based strategy when actually computing the query's result.)

Here is this example query's expected output:

        { "uname": "WillisWynne", "messages": [ " love product-b the customization is mind-blowing" ] }
        { "uname": "MargaritaStoddard", "messages": [ " can't stand acast its plan is terrible", " dislike x-phone its touch-screen is horrible", " can't stand acast the network is horrible:(", " like ccast the 3G is awesome:)", " can't stand product-w the touch-screen is terrible" ] }
        { "uname": "IsbelDull", "messages": [ " like product-z its platform is mind-blowing", " like product-y the plan is amazing" ] }
        { "uname": "NicholasStroh", "messages": [  ] }
        { "uname": "NilaMilliron", "messages": [  ] }
        { "uname": "WoodrowNehling", "messages": [ " love acast its 3G is good:)" ] }
        { "uname": "BramHatch", "messages": [ " can't stand product-z its voicemail-service is OMG:(", " dislike x-phone the voice-command is bad:(" ] }
        { "uname": "EmoryUnk", "messages": [ " love product-b its shortcut-menu is awesome:)", " love ccast its wireless is good" ] }
        { "uname": "VonKemble", "messages": [ " dislike product-b the speed is horrible" ] }
        { "uname": "SuzannaTillson", "messages": [ " like x-phone the voicemail-service is awesome" ] }

### Query 4 - Theta Join ###
Not all joins are expressible as equijoins and computable using equijoin-oriented algorithms.
The join predicates for some use cases involve predicates with functions; AsterixDB supports the
expression of such queries and will still evaluate them as best it can using nested loop based
techniques (and broadcast joins in the parallel case).

As an example of such a use case, suppose that we wanted, for each chirp T, to find all of the
other chirps that originated from within a circle of radius of 1 surrounding chirp T's location.
In AQL, this can be specified in a manner similar to the previous query using one of the built-in
functions on the spatial data type instead of id equality in the correlated query's _where_ clause:

        use dataverse TinySocial;

        for $cm in dataset ChirpMessages
        return {
            "message": $cm.messageText,
            "nearbyMessages": for $cm2 in dataset ChirpMessages
                              where spatial-distance($cm.senderLocation, $cm2.senderLocation) <= 1
                              return { "msgtxt":$cm2.messageText}
        };

Here is the expected result for this query:

        { "message": " can't stand x-phone its platform is terrible", "nearbyMessages": [ { "msgtxt": " can't stand x-phone its platform is terrible" } ] }
        { "message": " like ccast its shortcut-menu is awesome:)", "nearbyMessages": [ { "msgtxt": " like ccast its shortcut-menu is awesome:)" } ] }
        { "message": " like product-b the voice-command is mind-blowing:)", "nearbyMessages": [ { "msgtxt": " like product-b the voice-command is mind-blowing:)" } ] }
        { "message": " love ccast its voicemail-service is awesome", "nearbyMessages": [ { "msgtxt": " love ccast its voicemail-service is awesome" } ] }
        { "message": " love product-z its customization is good:)", "nearbyMessages": [ { "msgtxt": " love product-z its customization is good:)" } ] }
        { "message": " can't stand product-w its speed is terrible:(", "nearbyMessages": [ { "msgtxt": " can't stand product-w its speed is terrible:(" } ] }
        { "message": " like product-w the speed is good:)", "nearbyMessages": [ { "msgtxt": " like product-w the speed is good:)" }, { "msgtxt": " hate ccast its voice-clarity is OMG:(" } ] }
        { "message": " like x-phone the voice-clarity is good:)", "nearbyMessages": [ { "msgtxt": " like x-phone the voice-clarity is good:)" } ] }
        { "message": " like product-y the platform is good", "nearbyMessages": [ { "msgtxt": " like product-y the platform is good" } ] }
        { "message": " hate ccast its voice-clarity is OMG:(", "nearbyMessages": [ { "msgtxt": " like product-w the speed is good:)" }, { "msgtxt": " hate ccast its voice-clarity is OMG:(" } ] }
        { "message": " like product-y the voice-command is amazing:)", "nearbyMessages": [ { "msgtxt": " like product-y the voice-command is amazing:)" } ] }
        { "message": " like product-z the shortcut-menu is awesome:)", "nearbyMessages": [ { "msgtxt": " like product-z the shortcut-menu is awesome:)" } ] }


### Query 5 - Fuzzy Join ###
As another example of a non-equijoin use case, we could ask AsterixDB to find, for each Gleambook user,
all Chirp users with names "similar" to their name.
AsterixDB supports a variety of "fuzzy match" functions for use with textual and set-based data.
As one example, we could choose to use edit distance with a threshold of 3 as the definition of name
similarity, in which case we could write the following query using AQL's operator-based syntax (~=)
for testing whether or not two values are similar:

        use dataverse TinySocial;

        set simfunction "edit-distance";
        set simthreshold "3";

        for $gbu in dataset GleambookUsers
        return {
            "id": $gbu.id,
            "name": $gbu.name,
            "similarUsers": for $cm in dataset ChirpMessages
                            let $cu := $cm.user
                            where $cu.name ~= $gbu.name
                            return {
                                "chirpScreenname": $cu.screenName,
                                "chirpName": $cu.name
                            }
        };

The expected result for this query against our sample data is:

        { "id": 6, "name": "WillisWynne", "similarUsers": [  ] }
        { "id": 1, "name": "MargaritaStoddard", "similarUsers": [  ] }
        { "id": 2, "name": "IsbelDull", "similarUsers": [  ] }
        { "id": 4, "name": "NicholasStroh", "similarUsers": [  ] }
        { "id": 8, "name": "NilaMilliron", "similarUsers": [ { "chirpScreenname": "NilaMilliron_tw", "chirpName": "Nila Milliron" } ] }
        { "id": 9, "name": "WoodrowNehling", "similarUsers": [  ] }
        { "id": 10, "name": "BramHatch", "similarUsers": [  ] }
        { "id": 3, "name": "EmoryUnk", "similarUsers": [  ] }
        { "id": 5, "name": "VonKemble", "similarUsers": [  ] }
        { "id": 7, "name": "SuzannaTillson", "similarUsers": [  ] }

### Query 6 - Existential Quantification ###
The expressive power of AQL includes support for queries involving "some" (existentially quantified)
and "all" (universally quantified) query semantics.
As an example of an existential AQL query, here we show a query to list the Gleambook users who are currently employed.
Such employees will have an employment history containing a object with the endDate value missing, which leads us to the
following AQL query:

        use dataverse TinySocial;

        for $gbu in dataset GleambookUsers
        where (some $e in $gbu.employment satisfies is-missing($e.endDate))
        return $gbu;

The expected result in this case is:

        { "id": 6, "alias": "Willis", "name": "WillisWynne", "userSince": datetime("2005-01-17T10:10:00.000Z"), "friendIds": {{ 1, 3, 7 }}, "employment": [ { "organizationName": "jaydax", "startDate": date("2009-05-15") } ] }
        { "id": 1, "alias": "Margarita", "name": "MargaritaStoddard", "userSince": datetime("2012-08-20T10:10:00.000Z"), "friendIds": {{ 2, 3, 6, 10 }}, "employment": [ { "organizationName": "Codetechno", "startDate": date("2006-08-06") }, { "organizationName": "geomedia", "startDate": date("2010-06-17"), "endDate": date("2010-01-26") } ], "nickname": "Mags", "gender": "F" }
        { "id": 2, "alias": "Isbel", "name": "IsbelDull", "userSince": datetime("2011-01-22T10:10:00.000Z"), "friendIds": {{ 1, 4 }}, "employment": [ { "organizationName": "Hexviafind", "startDate": date("2010-04-27") } ], "nickname": "Izzy" }
        { "id": 4, "alias": "Nicholas", "name": "NicholasStroh", "userSince": datetime("2010-12-27T10:10:00.000Z"), "friendIds": {{ 2 }}, "employment": [ { "organizationName": "Zamcorporation", "startDate": date("2010-06-08") } ] }
        { "id": 8, "alias": "Nila", "name": "NilaMilliron", "userSince": datetime("2008-01-01T10:10:00.000Z"), "friendIds": {{ 3 }}, "employment": [ { "organizationName": "Plexlane", "startDate": date("2010-02-28") } ] }
        { "id": 5, "alias": "Von", "name": "VonKemble", "userSince": datetime("2010-01-05T10:10:00.000Z"), "friendIds": {{ 3, 6, 10 }}, "employment": [ { "organizationName": "Kongreen", "startDate": date("2010-11-27") } ] }
        { "id": 7, "alias": "Suzanna", "name": "SuzannaTillson", "userSince": datetime("2012-08-07T10:10:00.000Z"), "friendIds": {{ 6 }}, "employment": [ { "organizationName": "Labzatron", "startDate": date("2011-04-19") } ] }

### Query 7 - Universal Quantification ###
As an example of a universal AQL query, here we show a query to list the Gleambook users who are currently unemployed.
Such employees will have an employment history containing no objects that miss endDate values, leading us to the
following AQL query:

        use dataverse TinySocial;

        for $gbu in dataset GleambookUsers
        where (every $e in $gbu.employment satisfies not(is-missing($e.endDate)))
        return $gbu;

Here is the expected result for our sample data:

        { "id": 9, "alias": "Woodrow", "name": "WoodrowNehling", "userSince": datetime("2005-09-20T10:10:00.000Z"), "friendIds": {{ 3, 10 }}, "employment": [ { "organizationName": "Zuncan", "startDate": date("2003-04-22"), "endDate": date("2009-12-13") } ], "nickname": "Woody" }
        { "id": 10, "alias": "Bram", "name": "BramHatch", "userSince": datetime("2010-10-16T10:10:00.000Z"), "friendIds": {{ 1, 5, 9 }}, "employment": [ { "organizationName": "physcane", "startDate": date("2007-06-05"), "endDate": date("2011-11-05") } ] }
        { "id": 3, "alias": "Emory", "name": "EmoryUnk", "userSince": datetime("2012-07-10T10:10:00.000Z"), "friendIds": {{ 1, 5, 8, 9 }}, "employment": [ { "organizationName": "geomedia", "startDate": date("2010-06-17"), "endDate": date("2010-01-26") } ] }

### Query 8 - Simple Aggregation ###
Like SQL, the AQL language of AsterixDB provides support for computing aggregates over large amounts of data.
As a very simple example, the following AQL query computes the total number of Gleambook users:

        use dataverse TinySocial;

        count(for $gbu in dataset GleambookUsers return $gbu);

In AQL, aggregate functions can be applied to arbitrary subquery results; in this case, the count function
is applied to the result of a query that enumerates the Gleambook users.  The expected result here is:

        10

### Query 9-A - Grouping and Aggregation ###
Also like SQL, AQL supports grouped aggregation.
For every Chirp user, the following group-by/aggregate query counts the number of chirps sent by that user:

        use dataverse TinySocial;

        for $cm in dataset ChirpMessages
        group by $uid := $cm.user.screenName with $cm
        return {
            "user": $uid,
            "count": count($cm)
        };

The _for_ clause incrementally binds $cm to chirps, and the _group by_ clause groups the chirps by its
issuer's Chirp screenName.
Unlike SQL, where data is tabular---flat---the data model underlying AQL allows for nesting.
Thus, following the _group by_ clause, the _return_ clause in this query sees a sequence of $cm groups,
with each such group having an associated $uid variable value (i.e., the chirping user's screen name).
In the context of the return clause, due to "... with $cm ...", $uid is bound to the chirper's id and $cm
is bound to the _set_ of chirps issued by that chirper.
The return clause constructs a result object containing the chirper's user id and the count of the items
in the associated chirp set.
The query result will contain one such object per screen name.
This query also illustrates another feature of AQL; notice that each user's screen name is accessed via a
path syntax that traverses each chirp's nested object structure.

Here is the expected result for this query over the sample data:

        { "user": "OliJackson_512", "count": 1 }
        { "user": "ChangEwing_573", "count": 1 }
        { "user": "ColineGeyer@63", "count": 3 }
        { "user": "NathanGiesen@211", "count": 6 }
        { "user": "NilaMilliron_tw", "count": 1 }

### Query 9-B - (Hash-Based) Grouping and Aggregation ###
As for joins, AsterixDB has multiple evaluation strategies available for processing grouped aggregate queries.
For grouped aggregation, the system knows how to employ both sort-based and hash-based aggregation methods,
with sort-based methods being used by default and a hint being available to suggest that a different approach
be used in processing a particular AQL query.

The following query is similar to Query 9-A, but adds a hash-based aggregation hint:

        use dataverse TinySocial;

        for $cm in dataset ChirpMessages
        /*+ hash*/
        group by $uid := $cm.user.screenName with $cm
        return {
            "user": $uid,
            "count": count($cm)
        };

Here is the expected result:

        { "user": "OliJackson_512", "count": 1 }
        { "user": "ChangEwing_573", "count": 1 }
        { "user": "ColineGeyer@63", "count": 3 }
        { "user": "NathanGiesen@211", "count": 6 }
        { "user": "NilaMilliron_tw", "count": 1 }

### Query 10 - Grouping and Limits ###
In some use cases it is not necessary to compute the entire answer to a query.
In some cases, just having the first _N_ or top _N_ results is sufficient.
This is expressible in AQL using the _limit_ clause combined with the _order by_ clause.

The following AQL  query returns the top 3 Chirp users based on who has issued the most chirps:

        use dataverse TinySocial;

        for $cm in dataset ChirpMessages
        group by $uid := $cm.user.screenName with $cm
        let $c := count($cm)
        order by $c desc
        limit 3
        return {
            "user": $uid,
            "count": $c
        };

The expected result for this query is:

        { "user": "NathanGiesen@211", "count": 6 }
        { "user": "ColineGeyer@63", "count": 3 }
        { "user": "OliJackson_512", "count": 1 }

### Query 11 - Left Outer Fuzzy Join ###
As a last example of AQL and its query power, the following query, for each chirp,
finds all of the chirps that are similar based on the topics that they refer to:

        use dataverse TinySocial;

        set simfunction "jaccard";
        set simthreshold "0.3";

        for $cm in dataset ChirpMessages
        return {
            "chirp": $cm,
            "similarChirps": for $cm2 in dataset ChirpMessages
                             where  $cm2.referredTopics ~= $cm.referredTopics
                             and $cm2.chirpId != $cm.chirpId
                             return $cm2.referredTopics
        };

This query illustrates several things worth knowing in order to write fuzzy queries in AQL.
First, as mentioned earlier, AQL offers an operator-based syntax for seeing whether two values are "similar" to one another or not.
Second, recall that the referredTopics field of objects of datatype ChirpMessageType is a bag of strings.
This query sets the context for its similarity join by requesting that Jaccard-based similarity semantics
([http://en.wikipedia.org/wiki/Jaccard_index](http://en.wikipedia.org/wiki/Jaccard_index))
be used for the query's similarity operator and that a similarity index of 0.3 be used as its similarity threshold.

The expected result for this fuzzy join query is:

        { "chirp": { "chirpId": "11", "user": { "screenName": "NilaMilliron_tw", "lang": "en", "friendsCount": 445, "statusesCount": 164, "name": "Nila Milliron", "followersCount": 22649 }, "senderLocation": point("37.59,68.42"), "sendTime": datetime("2008-03-09T10:10:00.000Z"), "referredTopics": {{ "x-phone", "platform" }}, "messageText": " can't stand x-phone its platform is terrible" }, "similarChirps": [ {{ "x-phone", "voice-clarity" }}, {{ "product-y", "platform" }} ] }
        { "chirp": { "chirpId": "2", "user": { "screenName": "ColineGeyer@63", "lang": "en", "friendsCount": 121, "statusesCount": 362, "name": "Coline Geyer", "followersCount": 17159 }, "senderLocation": point("32.84,67.14"), "sendTime": datetime("2010-05-13T10:10:00.000Z"), "referredTopics": {{ "ccast", "shortcut-menu" }}, "messageText": " like ccast its shortcut-menu is awesome:)" }, "similarChirps": [ {{ "ccast", "voicemail-service" }}, {{ "ccast", "voice-clarity" }}, {{ "product-z", "shortcut-menu" }} ] }
        { "chirp": { "chirpId": "4", "user": { "screenName": "NathanGiesen@211", "lang": "en", "friendsCount": 39339, "statusesCount": 473, "name": "Nathan Giesen", "followersCount": 49416 }, "senderLocation": point("39.28,70.48"), "sendTime": datetime("2011-12-26T10:10:00.000Z"), "referredTopics": {{ "product-b", "voice-command" }}, "messageText": " like product-b the voice-command is mind-blowing:)" }, "similarChirps": [ {{ "product-y", "voice-command" }} ] }
        { "chirp": { "chirpId": "9", "user": { "screenName": "NathanGiesen@211", "lang": "en", "friendsCount": 39339, "statusesCount": 473, "name": "Nathan Giesen", "followersCount": 49416 }, "senderLocation": point("36.86,74.62"), "sendTime": datetime("2012-07-21T10:10:00.000Z"), "referredTopics": {{ "ccast", "voicemail-service" }}, "messageText": " love ccast its voicemail-service is awesome" }, "similarChirps": [ {{ "ccast", "shortcut-menu" }}, {{ "ccast", "voice-clarity" }} ] }
        { "chirp": { "chirpId": "1", "user": { "screenName": "NathanGiesen@211", "lang": "en", "friendsCount": 39339, "statusesCount": 473, "name": "Nathan Giesen", "followersCount": 49416 }, "senderLocation": point("47.44,80.65"), "sendTime": datetime("2008-04-26T10:10:00.000Z"), "referredTopics": {{ "product-z", "customization" }}, "messageText": " love product-z its customization is good:)" }, "similarChirps": [ {{ "product-z", "shortcut-menu" }} ] }
        { "chirp": { "chirpId": "5", "user": { "screenName": "NathanGiesen@211", "lang": "en", "friendsCount": 39339, "statusesCount": 473, "name": "Nathan Giesen", "followersCount": 49416 }, "senderLocation": point("40.09,92.69"), "sendTime": datetime("2006-08-04T10:10:00.000Z"), "referredTopics": {{ "product-w", "speed" }}, "messageText": " can't stand product-w its speed is terrible:(" }, "similarChirps": [ {{ "product-w", "speed" }} ] }
        { "chirp": { "chirpId": "3", "user": { "screenName": "NathanGiesen@211", "lang": "en", "friendsCount": 39339, "statusesCount": 473, "name": "Nathan Giesen", "followersCount": 49416 }, "senderLocation": point("29.72,75.8"), "sendTime": datetime("2006-11-04T10:10:00.000Z"), "referredTopics": {{ "product-w", "speed" }}, "messageText": " like product-w the speed is good:)" }, "similarChirps": [ {{ "product-w", "speed" }} ] }
        { "chirp": { "chirpId": "6", "user": { "screenName": "ColineGeyer@63", "lang": "en", "friendsCount": 121, "statusesCount": 362, "name": "Coline Geyer", "followersCount": 17159 }, "senderLocation": point("47.51,83.99"), "sendTime": datetime("2010-05-07T10:10:00.000Z"), "referredTopics": {{ "x-phone", "voice-clarity" }}, "messageText": " like x-phone the voice-clarity is good:)" }, "similarChirps": [ {{ "x-phone", "platform" }}, {{ "ccast", "voice-clarity" }} ] }
        { "chirp": { "chirpId": "7", "user": { "screenName": "ChangEwing_573", "lang": "en", "friendsCount": 182, "statusesCount": 394, "name": "Chang Ewing", "followersCount": 32136 }, "senderLocation": point("36.21,72.6"), "sendTime": datetime("2011-08-25T10:10:00.000Z"), "referredTopics": {{ "product-y", "platform" }}, "messageText": " like product-y the platform is good" }, "similarChirps": [ {{ "x-phone", "platform" }}, {{ "product-y", "voice-command" }} ] }
        { "chirp": { "chirpId": "10", "user": { "screenName": "ColineGeyer@63", "lang": "en", "friendsCount": 121, "statusesCount": 362, "name": "Coline Geyer", "followersCount": 17159 }, "senderLocation": point("29.15,76.53"), "sendTime": datetime("2008-01-26T10:10:00.000Z"), "referredTopics": {{ "ccast", "voice-clarity" }}, "messageText": " hate ccast its voice-clarity is OMG:(" }, "similarChirps": [ {{ "ccast", "shortcut-menu" }}, {{ "ccast", "voicemail-service" }}, {{ "x-phone", "voice-clarity" }} ] }
        { "chirp": { "chirpId": "12", "user": { "screenName": "OliJackson_512", "lang": "en", "friendsCount": 445, "statusesCount": 164, "name": "Oli Jackson", "followersCount": 22649 }, "senderLocation": point("24.82,94.63"), "sendTime": datetime("2010-02-13T10:10:00.000Z"), "referredTopics": {{ "product-y", "voice-command" }}, "messageText": " like product-y the voice-command is amazing:)" }, "similarChirps": [ {{ "product-b", "voice-command" }}, {{ "product-y", "platform" }} ] }
        { "chirp": { "chirpId": "8", "user": { "screenName": "NathanGiesen@211", "lang": "en", "friendsCount": 39339, "statusesCount": 473, "name": "Nathan Giesen", "followersCount": 49416 }, "senderLocation": point("46.05,93.34"), "sendTime": datetime("2005-10-14T10:10:00.000Z"), "referredTopics": {{ "product-z", "shortcut-menu" }}, "messageText": " like product-z the shortcut-menu is awesome:)" }, "similarChirps": [ {{ "ccast", "shortcut-menu" }}, {{ "product-z", "customization" }} ] }

### Deleting Existing Data  ###
In addition to inserting new data, AsterixDB supports deletion from datasets via the AQL _delete_ statement.
The statement supports "searched delete" semantics, and its
_where_ clause can involve any valid XQuery expression.

The following example deletes the chirp that we just added from user "NathanGiesen@211".  (Easy come, easy go. :-))

        use dataverse TinySocial;

        delete $cm from dataset ChirpMessages where $cm.chirpId = "13";

It should be noted that one form of data change not yet supported by AsterixDB is in-place data modification (_update_).
Currently, only insert and delete operations are supported; update is not.
To achieve the effect of an update, two statements are currently needed---one to delete the old object from the
dataset where it resides, and another to insert the new replacement object (with the same primary key but with
different field values for some of the associated data content).

### Upserting Data  ###
In addition to loading, querying, inserting, and deleting data, AsterixDB supports upserting
objects using the AQL _upsert_ statement.

The following example deletes the chirp with chirpId = 20 (if one exists) and inserts the
new chirp with chirpId = 20 by user "SwanSmitty" to the ChirpMessages dataset. The two
operations (delete if found and insert) are performed as an atomic operation that is either
performed completely or not at all.

        use dataverse TinySocial;
        upsert into dataset ChirpMessages
        (
           {"chirpId": "20",
            "user":
                {"screenName": "SwanSmitty",
                 "lang": "en",
                 "friendsCount": 91345,
                 "statusesCount": 4079,
                 "name": "Swanson Smith",
                 "followersCount": 50420
                },
            "senderLocation": point("47.44,80.65"),
            "sendTime": datetime("2008-04-26T10:10:35"),
            "referredTopics": {{"football"}},
            "messageText": "football is the best sport, period.!"
           }
        );

The data to be upserted may be specified using any valid AQL query expression.
For example, the following statement might be used to double the followers count of all existing users.

        use dataverse TinySocial;
        upsert into dataset ChirpUsers
        (
           for $user in dataset ChirpUsers
           return {
            "screenName": $user.screenName,
            "lang": $user.lang,
            "friendsCount": $user.friendsCount,
            "statusesCount": $user.statusesCount,
            "name": $user.name,
            "followersCount": $user.followersCount * 2
           }
        );

Note that such an upsert operation is executed in two steps:
The query is performed, after which the query's locks are released,
and then its result is upserted into the dataset.
This means that a object can be modified between computing the query result and performing the upsert.

### Transaction Support ###

AsterixDB supports object-level ACID transactions that begin and terminate implicitly for each object inserted, deleted, or searched while a given AQL statement is being executed. This is quite similar to the level of transaction support found in today's NoSQL stores. AsterixDB does not support multi-statement transactions, and in fact an AQL statement that involves multiple objects can itself involve multiple independent object-level transactions. An example consequence of this is that, when an AQL statement attempts to insert 1000 objects, it is possible that the first 800 objects could end up being committed while the remaining 200 objects fail to be inserted. This situation could happen, for example, if a duplicate key exception occurs as the 801st insertion is attempted. If this happens, AsterixDB will report the error (e.g., a duplicate key exception) as the result of the offending AQL insert statement, and the application logic above will need to take the appropriate action(s) needed to assess the resulting state and to clean up and/or continue as appropriate.


### Loading New Data in Bulk  ###

In addition to incremental additions to datasets via the AQL _insert_ statement, the _load_ statement can be used to
take a file from a given node and load it in a more efficient fashion. Note however that a dataset can currently only
be loaded if it is empty.

The following example loads a file in ADM format from "/home/user/gbm.adm" from the node named "nc1" into the GleambookUsers dataset.

    use dataverse TinySocial;

    load dataset GleambookUsers using localfs
        (("path"="nc1://home/user/gbu.adm"),("format"="adm"));


## Further Help ##
That's it!  You are now armed and dangerous with respect to semistructured data management using AsterixDB and AQL.

AsterixDB is a powerful new BDMS---Big Data Management System---that we hope may usher in a new era of much
more declarative Big Data management.
AsterixDB is powerful, so use it wisely, and remember: "With great power comes great responsibility..." :-)

Please e-mail the AsterixDB user group
(users (at) asterixdb.apache.org)
if you run into any problems or simply have further questions about the AsterixDB system, its features, or their proper use.
