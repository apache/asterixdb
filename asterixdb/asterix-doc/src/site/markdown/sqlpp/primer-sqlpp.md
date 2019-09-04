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

# AsterixDB 101: An ADM and SQL++ Primer #

## Welcome to AsterixDB! ##
This document introduces the main features of AsterixDB's data model (ADM) and its new SQL-like query language (SQL++) by example.
The example is a simple scenario involving (synthetic) sample data modeled after data from the social domain.
This document describes a set of sample datasets, together with a set of illustrative queries,
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
there should no changes needed in the SQL++ statements to run the examples locally and/or to run them
on a cluster when you are ready to take that step.)

As you read through this document, you should try each step for yourself on your own AsterixDB instance.
You will use the AsterixDB web interface to do this, and for SQL++ you will need to select SQL++ instead of AQL as your language of choice in the Query Language box that sits underneath the UI's query entry area.
Once you have reached the end of this tutorial, you will be fully armed and dangerous, with all the basic AsterixDB knowledge
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
Let's put these concepts to work.

Our little sample scenario involves information about users of two hypothetical social networks,
Gleambook and Chirp, and their messages.
We'll start by defining a dataverse called "TinySocial" to hold our datatypes and datasets.
The AsterixDB data model (ADM) is essentially a superset of JSON---it's what you get by extending
JSON with more data types and additional data modeling constructs borrowed from object databases.
The following shows how we can create the TinySocial dataverse plus a set of ADM types for modeling
Chirp users, their Chirps, Gleambook users, their users' employment information, and their messages.
(Note: Keep in mind that this is just a tiny and somewhat silly example intended for illustrating
some of the key features of AsterixDB. :-)) As a point of information, SQL++ is case-insensitive
for both keywords and built-in type names, so the exact style of the examples below is just one of
a number of possibilities.

        DROP DATAVERSE TinySocial IF EXISTS;
        CREATE DATAVERSE TinySocial;
        USE TinySocial;

        CREATE TYPE ChirpUserType AS {
            screenName: string,
            lang: string,
            friendsCount: int,
            statusesCount: int,
            name: string,
            followersCount: int
        };

        CREATE TYPE ChirpMessageType AS closed {
            chirpId: string,
            user: ChirpUserType,
            senderLocation: point?,
            sendTime: datetime,
            referredTopics: {{ string }},
            messageText: string
        };

        CREATE TYPE EmploymentType AS {
            organizationName: string,
            startDate: date,
            endDate: date?
        };

        CREATE TYPE GleambookUserType AS {
            id: int,
            alias: string,
            name: string,
            userSince: datetime,
            friendIds: {{ int }},
            employment: [EmploymentType]
        };

        CREATE TYPE GleambookMessageType AS {
            messageId: int,
            authorId: int,
            inResponseTo: int?,
            senderLocation: point?,
            message: string
        };

The first three lines above tell AsterixDB to drop the old TinySocial dataverse, if one already
exists, and then to create a brand new one and make it the focus of the statements that follow.
The first _CREATE TYPE_ statement creates a datatype for holding information about Chirp users.
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
The next two _CREATE TYPE_ statements create a object type for holding information about one component of
the employment history of a Gleambook user and then a object type for holding the user information itself.
The Gleambook user type highlights a few additional ADM data model features.
Its friendIds field is a bag of integers, presumably the Gleambook user ids for this user's friends,
and its employment field is an ordered list of employment objects.
The final _CREATE TYPE_ statement defines a type for handling the content of a Gleambook message in our
hypothetical social data storage scenario.

Before going on, we need to once again emphasize the idea that AsterixDB is aimed at storing
and querying not just Big Data, but Big _Semistructured_ Data.
This means that most of the fields listed in the _CREATE TYPE_ statements above could have been
omitted without changing anything other than the resulting size of stored data instances on disk.
AsterixDB stores its information about the fields defined a priori as separate metadata, whereas
the information about other fields that are "just there" in instances of open datatypes is stored
with each instance---making for more bits on disk and longer times for operations affected by
data size (e.g., dataset scans).
The only fields that _must_ be specified a priori are the primary key fields of each dataset.

### Creating Datasets and Indexes ###

Now that we have defined our datatypes, we can move on and create datasets to store the actual data.
(If we wanted to, we could even have several named datasets based on any one of these datatypes.)
We can do this as follows, utilizing the SQL++ DDL capabilities of AsterixDB.

        USE TinySocial;

        CREATE DATASET GleambookUsers(GleambookUserType)
            PRIMARY KEY id;

        CREATE DATASET GleambookMessages(GleambookMessageType)
            PRIMARY KEY messageId;

        CREATE DATASET ChirpUsers(ChirpUserType)
            PRIMARY KEY screenName;

        CREATE DATASET ChirpMessages(ChirpMessageType)
            PRIMARY KEY chirpId
            hints(cardinality=100);

        CREATE INDEX gbUserSinceIdx on GleambookUsers(userSince);
        CREATE INDEX gbAuthorIdx on GleambookMessages(authorId) TYPE btree;
        CREATE INDEX gbSenderLocIndex on GleambookMessages(senderLocation) TYPE rtree;
        CREATE INDEX gbMessageIdx on GleambookMessages(message) TYPE keyword;

        SELECT VALUE ds FROM Metadata.`Dataset` ds;
        SELECT VALUE ix FROM Metadata.`Index` ix;

The SQL++ DDL statements above create four datasets for holding our social data in the TinySocial
dataverse: GleambookUsers, GleambookMessages, ChirpUsers, and ChirpMessages.
The first _CREATE DATASET_ statement creates the GleambookUsers data set.
It specifies that this dataset will store data instances conforming to GleambookUserType and that
it has a primary key which is the id field of each instance.
The primary key information is used by AsterixDB to uniquely identify instances for the purpose
of later lookup and for use in secondary indexes.
Each AsterixDB dataset is stored (and indexed) in the form of a B+ tree on primary key;
secondary indexes point to their indexed data by primary key.
In AsterixDB clusters, the primary key is also used to hash-partition (*a.k.a.* shard) the
dataset across the nodes of the cluster.
The next three _CREATE DATASET_ statements are similar.
The last one illustrates an optional clause for providing useful hints to AsterixDB.
In this case, the hint tells AsterixDB that the dataset definer is anticipating that the
ChirpMessages dataset will contain roughly 100 objects; knowing this can help AsterixDB
to more efficiently manage and query this dataset.
(AsterixDB does not yet gather and maintain data statistics; it will currently, abitrarily,
assume a cardinality of one million objects per dataset in the absence of such an optional
definition-time hint.)

The _CREATE DATASET_ statements above are followed by four more DDL statements, each of which
creates a secondary index on a field of one of the datasets.
The first one indexes the GleambookUsers dataset on its user-since field.
This index will be a B+ tree index; its type is unspecified and _btree_ is the default type.
The other three illustrate how you can explicitly specify the desired type of index.
In addition to btree, _rtree_ and inverted _keyword_ indexes are supported by AsterixDB.
Indexes can also have composite keys, and more advanced text indexing is available as well
(ngram(k), where k is the desired gram length).

### Querying the Metadata Dataverse ###

The last two statements above show how you can use queries in SQL++ to examine the AsterixDB
system catalogs and tell what artifacts you have created.
Just as relational DBMSs use their own tables to store their catalogs, AsterixDB uses
its own datasets to persist descriptions of its datasets, datatypes, indexes, and so on.
Running the first of the two queries above will list all of your newly created datasets,
and it will also show you a full list of all the metadata datasets.
(You can then explore from there on your own if you are curious)
These last two queries also illustrate a few other factoids worth knowing:
First, AsterixDB allows queries to span dataverses via the use
of fully-qualified dataset names (i.e., _dataversename.datasetname_)
to reference datasets that live in a dataverse other than the one
referenced in the most recently executed _USE_ directive.
Second, they show how to escape SQL++ keywords (or other special names) in object names by using backquotes.
Last but not least, they show that SQL++ supports a _SELECT VALUE_ variation of SQL's traditional _SELECT_
statement that returns a single value (or element) from a query instead of constructing a new
object as the query's result like _SELECT_ does; here, the returned value is an entire object from
the dataset being queried (e.g., _SELECT VALUE ds_ in the first statement returns the entire
object from the metadata dataset containing the descriptions of all datasets.

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

        USE TinySocial;

        INSERT INTO ChirpUsers
        ([
        {"screenName":"NathanGiesen@211","lang":"en","friendsCount":18,"statusesCount":473,"name":"Nathan Giesen","followersCount":49416},
        {"screenName":"ColineGeyer@63","lang":"en","friendsCount":121,"statusesCount":362,"name":"Coline Geyer","followersCount":17159},
        {"screenName":"NilaMilliron_tw","lang":"en","friendsCount":445,"statusesCount":164,"name":"Nila Milliron","followersCount":22649},
        {"screenName":"ChangEwing_573","lang":"en","friendsCount":182,"statusesCount":394,"name":"Chang Ewing","followersCount":32136}
        ]);

[Chirp Messages](../data/chm.adm)

        USE TinySocial;

        INSERT INTO ChirpMessages
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

        USE TinySocial;

        INSERT INTO GleambookUsers
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

        USE TinySocial;

        INSERT INTO GleambookMessages
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


## SQL++: Querying Your AsterixDB Data ##
Congratulations! You now have sample social data stored (and indexed) in AsterixDB.
(You are part of an elite and adventurous group of individuals. :-))
Now that you have successfully loaded the provided sample data into the datasets that we defined,
you can start running queries against them.

AsterixDB currently supports two query languages.
The first---AsterixDB's original query language---is AQL (the Asterix Query Language).
The AQL language was inspired by XQuery, the W3C standard language for querying XML data.
(There is a version of this tutorial for AQL if you would like to learn more about it.)
The query language described in the remainder of this tutorial is SQL++,
a SQL-inspired language designed (as AQL was) for working with semistructured data.
SQL++ has much in common with SQL, but there are differences due to the data model
that SQL++ is designed to serve.
SQL was designed in the 1970's to interact with the flat, schema-ified world of relational databases.
SQL++ is designed for the nested, schema-less (or schema-optional, in AsterixDB) world of NoSQL systems.
While SQL++ has the same expressive power as AQL,
it offers a more familar paradigm for experienced SQL users to use to query and manipulate data in AsterixDB.

In this section we introduce SQL++ via a set of example queries, along with their expected results,
based on the data above, to help you get started.
Many of the most important features of SQL++ are presented in this set of representative queries.
You can find more details in the document on the [Asterix Data Model (ADM)](datamodel.html),
in the [SQL++ Reference Manual](manual-sqlpp.html), and a complete list of built-in functions is available
in the [SQL++ Functions](functions-sqlpp.html) document.

SQL++ is an expression language.
Even the simple expression 1+1 is a valid SQL++ query that evaluates to 2.
(Try it for yourself!
Okay, maybe that's _not_ the best use of a 512-node shared-nothing compute cluster.)
But enough talk!
Let's go ahead and try writing some queries and see about learning SQL++ by example.
(Again, don't forget to choose SQL++ as the query language in the web interface!)

### Query 0-A - Exact-Match Lookup ###
For our first query, let's find a Gleambook user based on his or her user id.
Suppose the user we want is the user whose id is 8:

        USE TinySocial;

        SELECT VALUE user
        FROM GleambookUsers user
        WHERE user.id = 8;

As in SQL, the query's _FROM_ clause  binds the variable `user` incrementally to the data instances residing in
the dataset named GleambookUsers.
Its _WHERE_ clause  selects only those bindings having a user id of interest, filtering out the rest.
The _SELECT_ _VALUE_ clause returns the (entire) data value (a Gleambook user object in this case)
for each binding that satisfies the predicate.
Since this dataset is indexed on user id (its primary key), this query will be done via a quick index lookup.

The expected result for our sample data is as follows:

        { "id": 8, "alias": "Nila", "name": "NilaMilliron", "userSince": datetime("2008-01-01T10:10:00.000Z"), "friendIds": {{ 3 }}, "employment": [ { "organizationName": "Plexlane", "startDate": date("2010-02-28") } ] }


### Query 0-B - Range Scan ###
SQL++, like SQL, supports a variety of different predicates.
For example, for our next query, let's find the Gleambook users whose ids are in the range between 2 and 4:

        USE TinySocial;

        SELECT VALUE user
        FROM GleambookUsers user
        WHERE user.id >= 2 AND user.id <= 4;

This query's expected result, also evaluable using the primary index on user id, is:

        { "id": 2, "alias": "Isbel", "name": "IsbelDull", "userSince": datetime("2011-01-22T10:10:00.000Z"), "friendIds": {{ 1, 4 }}, "employment": [ { "organizationName": "Hexviafind", "startDate": date("2010-04-27") } ], "nickname": "Izzy" }
        { "id": 3, "alias": "Emory", "name": "EmoryUnk", "userSince": datetime("2012-07-10T10:10:00.000Z"), "friendIds": {{ 1, 5, 8, 9 }}, "employment": [ { "organizationName": "geomedia", "startDate": date("2010-06-17"), "endDate": date("2010-01-26") } ] }
        { "id": 4, "alias": "Nicholas", "name": "NicholasStroh", "userSince": datetime("2010-12-27T10:10:00.000Z"), "friendIds": {{ 2 }}, "employment": [ { "organizationName": "Zamcorporation", "startDate": date("2010-06-08") } ] }

### Query 1 - Other Query Filters ###
SQL++ can do range queries on any data type that supports the appropriate set of comparators.
As an example, this next query retrieves the Gleambook users who joined between July 22, 2010 and July 29, 2012:

        USE TinySocial;

        SELECT VALUE user
        FROM GleambookUsers user
        WHERE user.userSince >= datetime('2010-07-22T00:00:00')
          AND user.userSince <= datetime('2012-07-29T23:59:59');

The expected result for this query, also an indexable query, is as follows:

        { "id": 10, "alias": "Bram", "name": "BramHatch", "userSince": datetime("2010-10-16T10:10:00.000Z"), "friendIds": {{ 1, 5, 9 }}, "employment": [ { "organizationName": "physcane", "startDate": date("2007-06-05"), "endDate": date("2011-11-05") } ] }
        { "id": 2, "alias": "Isbel", "name": "IsbelDull", "userSince": datetime("2011-01-22T10:10:00.000Z"), "friendIds": {{ 1, 4 }}, "employment": [ { "organizationName": "Hexviafind", "startDate": date("2010-04-27") } ], "nickname": "Izzy" }
        { "id": 3, "alias": "Emory", "name": "EmoryUnk", "userSince": datetime("2012-07-10T10:10:00.000Z"), "friendIds": {{ 1, 5, 8, 9 }}, "employment": [ { "organizationName": "geomedia", "startDate": date("2010-06-17"), "endDate": date("2010-01-26") } ] }
        { "id": 4, "alias": "Nicholas", "name": "NicholasStroh", "userSince": datetime("2010-12-27T10:10:00.000Z"), "friendIds": {{ 2 }}, "employment": [ { "organizationName": "Zamcorporation", "startDate": date("2010-06-08") } ] }

### Query 2-A - Equijoin ###
In addition to simply binding variables to data instances and returning them "whole",
an SQL++ query can construct new ADM instances to return based on combinations of its variable bindings.
This gives SQL++ the power to do projections and joins much like those done using multi-table _FROM_ clauses in SQL.
For example, suppose we wanted a list of all Gleambook users paired with their associated messages,
with the list enumerating the author name and the message text associated with each Gleambook message.
We could do this as follows in SQL++:

        USE TinySocial;

        SELECT user.name AS uname, msg.message AS message
        FROM GleambookUsers user, GleambookMessages msg
        WHERE msg.authorId = user.id;

The result of this query is a sequence of new ADM instances, one for each author/message pair.
Each instance in the result will be an ADM object containing two fields, "uname" and "message",
containing the user's name and the message text, respectively, for each author/message pair.
Notice how the use of a traditional SQL-style _SELECT_ clause, as opposed to the new SQL++ _SELECT VALUE_
clause, automatically results in the construction of a new object value for each result.

The expected result of this example SQL++ join query for our sample data set is:

        { "uname": "WillisWynne", "message": " love product-b the customization is mind-blowing" }
        { "uname": "WoodrowNehling", "message": " love acast its 3G is good:)" }
        { "uname": "BramHatch", "message": " can't stand product-z its voicemail-service is OMG:(" }
        { "uname": "BramHatch", "message": " dislike x-phone the voice-command is bad:(" }
        { "uname": "MargaritaStoddard", "message": " like ccast the 3G is awesome:)" }
        { "uname": "MargaritaStoddard", "message": " can't stand product-w the touch-screen is terrible" }
        { "uname": "MargaritaStoddard", "message": " can't stand acast its plan is terrible" }
        { "uname": "MargaritaStoddard", "message": " dislike x-phone its touch-screen is horrible" }
        { "uname": "MargaritaStoddard", "message": " can't stand acast the network is horrible:(" }
        { "uname": "IsbelDull", "message": " like product-z its platform is mind-blowing" }
        { "uname": "IsbelDull", "message": " like product-y the plan is amazing" }
        { "uname": "EmoryUnk", "message": " love ccast its wireless is good" }
        { "uname": "EmoryUnk", "message": " love product-b its shortcut-menu is awesome:)" }
        { "uname": "VonKemble", "message": " dislike product-b the speed is horrible" }
        { "uname": "SuzannaTillson", "message": " like x-phone the voicemail-service is awesome" }

If we were feeling lazy, we might use _SELECT *_ in SQL++ to return all of the matching user/message data:

        USE TinySocial;

        SELECT *
        FROM GleambookUsers user, GleambookMessages msg
        WHERE msg.authorId = user.id;

In SQL++, this _SELECT *_ query will produce a new nested object for each user/message pair.
Each result object contains one field (named after the "user" variable) to hold the user object
and another field (named after the "msg" variable) to hold the matching message object.
Note that the nested nature of this SQL++ _SELECT *_ result is different than traditional SQL,
as SQL was not designed to handle the richer, nested data model that underlies the design of SQL++.

The expected result of this version of the SQL++ join query for our sample data set is:

        { "user": { "id": 6, "alias": "Willis", "name": "WillisWynne", "userSince": datetime("2005-01-17T10:10:00.000Z"), "friendIds": {{ 1, 3, 7 }}, "employment": [ { "organizationName": "jaydax", "startDate": date("2009-05-15") } ] }, "msg": { "messageId": 5, "authorId": 6, "inResponseTo": 2, "senderLocation": point("34.7,90.76"), "message": " love product-b the customization is mind-blowing" } }
        { "user": { "id": 9, "alias": "Woodrow", "name": "WoodrowNehling", "userSince": datetime("2005-09-20T10:10:00.000Z"), "friendIds": {{ 3, 10 }}, "employment": [ { "organizationName": "Zuncan", "startDate": date("2003-04-22"), "endDate": date("2009-12-13") } ], "nickname": "Woody" }, "msg": { "messageId": 14, "authorId": 9, "inResponseTo": 12, "senderLocation": point("41.33,85.28"), "message": " love acast its 3G is good:)" } }
        { "user": { "id": 10, "alias": "Bram", "name": "BramHatch", "userSince": datetime("2010-10-16T10:10:00.000Z"), "friendIds": {{ 1, 5, 9 }}, "employment": [ { "organizationName": "physcane", "startDate": date("2007-06-05"), "endDate": date("2011-11-05") } ] }, "msg": { "messageId": 12, "authorId": 10, "inResponseTo": 6, "senderLocation": point("42.26,77.76"), "message": " can't stand product-z its voicemail-service is OMG:(" } }
        { "user": { "id": 10, "alias": "Bram", "name": "BramHatch", "userSince": datetime("2010-10-16T10:10:00.000Z"), "friendIds": {{ 1, 5, 9 }}, "employment": [ { "organizationName": "physcane", "startDate": date("2007-06-05"), "endDate": date("2011-11-05") } ] }, "msg": { "messageId": 13, "authorId": 10, "inResponseTo": 4, "senderLocation": point("42.77,78.92"), "message": " dislike x-phone the voice-command is bad:(" } }
        { "user": { "id": 1, "alias": "Margarita", "name": "MargaritaStoddard", "userSince": datetime("2012-08-20T10:10:00.000Z"), "friendIds": {{ 2, 3, 6, 10 }}, "employment": [ { "organizationName": "Codetechno", "startDate": date("2006-08-06") }, { "organizationName": "geomedia", "startDate": date("2010-06-17"), "endDate": date("2010-01-26") } ], "nickname": "Mags", "gender": "F" }, "msg": { "messageId": 8, "authorId": 1, "inResponseTo": 11, "senderLocation": point("40.33,80.87"), "message": " like ccast the 3G is awesome:)" } }
        { "user": { "id": 1, "alias": "Margarita", "name": "MargaritaStoddard", "userSince": datetime("2012-08-20T10:10:00.000Z"), "friendIds": {{ 2, 3, 6, 10 }}, "employment": [ { "organizationName": "Codetechno", "startDate": date("2006-08-06") }, { "organizationName": "geomedia", "startDate": date("2010-06-17"), "endDate": date("2010-01-26") } ], "nickname": "Mags", "gender": "F" }, "msg": { "messageId": 10, "authorId": 1, "inResponseTo": 12, "senderLocation": point("42.5,70.01"), "message": " can't stand product-w the touch-screen is terrible" } }
        { "user": { "id": 1, "alias": "Margarita", "name": "MargaritaStoddard", "userSince": datetime("2012-08-20T10:10:00.000Z"), "friendIds": {{ 2, 3, 6, 10 }}, "employment": [ { "organizationName": "Codetechno", "startDate": date("2006-08-06") }, { "organizationName": "geomedia", "startDate": date("2010-06-17"), "endDate": date("2010-01-26") } ], "nickname": "Mags", "gender": "F" }, "msg": { "messageId": 11, "authorId": 1, "inResponseTo": 1, "senderLocation": point("38.97,77.49"), "message": " can't stand acast its plan is terrible" } }
        { "user": { "id": 1, "alias": "Margarita", "name": "MargaritaStoddard", "userSince": datetime("2012-08-20T10:10:00.000Z"), "friendIds": {{ 2, 3, 6, 10 }}, "employment": [ { "organizationName": "Codetechno", "startDate": date("2006-08-06") }, { "organizationName": "geomedia", "startDate": date("2010-06-17"), "endDate": date("2010-01-26") } ], "nickname": "Mags", "gender": "F" }, "msg": { "messageId": 2, "authorId": 1, "inResponseTo": 4, "senderLocation": point("41.66,80.87"), "message": " dislike x-phone its touch-screen is horrible" } }
        { "user": { "id": 1, "alias": "Margarita", "name": "MargaritaStoddard", "userSince": datetime("2012-08-20T10:10:00.000Z"), "friendIds": {{ 2, 3, 6, 10 }}, "employment": [ { "organizationName": "Codetechno", "startDate": date("2006-08-06") }, { "organizationName": "geomedia", "startDate": date("2010-06-17"), "endDate": date("2010-01-26") } ], "nickname": "Mags", "gender": "F" }, "msg": { "messageId": 4, "authorId": 1, "inResponseTo": 2, "senderLocation": point("37.73,97.04"), "message": " can't stand acast the network is horrible:(" } }
        { "user": { "id": 2, "alias": "Isbel", "name": "IsbelDull", "userSince": datetime("2011-01-22T10:10:00.000Z"), "friendIds": {{ 1, 4 }}, "employment": [ { "organizationName": "Hexviafind", "startDate": date("2010-04-27") } ], "nickname": "Izzy" }, "msg": { "messageId": 6, "authorId": 2, "inResponseTo": 1, "senderLocation": point("31.5,75.56"), "message": " like product-z its platform is mind-blowing" } }
        { "user": { "id": 2, "alias": "Isbel", "name": "IsbelDull", "userSince": datetime("2011-01-22T10:10:00.000Z"), "friendIds": {{ 1, 4 }}, "employment": [ { "organizationName": "Hexviafind", "startDate": date("2010-04-27") } ], "nickname": "Izzy" }, "msg": { "messageId": 3, "authorId": 2, "inResponseTo": 4, "senderLocation": point("48.09,81.01"), "message": " like product-y the plan is amazing" } }
        { "user": { "id": 3, "alias": "Emory", "name": "EmoryUnk", "userSince": datetime("2012-07-10T10:10:00.000Z"), "friendIds": {{ 1, 5, 8, 9 }}, "employment": [ { "organizationName": "geomedia", "startDate": date("2010-06-17"), "endDate": date("2010-01-26") } ] }, "msg": { "messageId": 9, "authorId": 3, "inResponseTo": 12, "senderLocation": point("34.45,96.48"), "message": " love ccast its wireless is good" } }
        { "user": { "id": 3, "alias": "Emory", "name": "EmoryUnk", "userSince": datetime("2012-07-10T10:10:00.000Z"), "friendIds": {{ 1, 5, 8, 9 }}, "employment": [ { "organizationName": "geomedia", "startDate": date("2010-06-17"), "endDate": date("2010-01-26") } ] }, "msg": { "messageId": 1, "authorId": 3, "inResponseTo": 2, "senderLocation": point("47.16,77.75"), "message": " love product-b its shortcut-menu is awesome:)" } }
        { "user": { "id": 5, "alias": "Von", "name": "VonKemble", "userSince": datetime("2010-01-05T10:10:00.000Z"), "friendIds": {{ 3, 6, 10 }}, "employment": [ { "organizationName": "Kongreen", "startDate": date("2010-11-27") } ] }, "msg": { "messageId": 7, "authorId": 5, "inResponseTo": 15, "senderLocation": point("32.91,85.05"), "message": " dislike product-b the speed is horrible" } }
        { "user": { "id": 7, "alias": "Suzanna", "name": "SuzannaTillson", "userSince": datetime("2012-08-07T10:10:00.000Z"), "friendIds": {{ 6 }}, "employment": [ { "organizationName": "Labzatron", "startDate": date("2011-04-19") } ] }, "msg": { "messageId": 15, "authorId": 7, "inResponseTo": 11, "senderLocation": point("44.47,67.11"), "message": " like x-phone the voicemail-service is awesome" } }

Finally (for now :-)), another less lazy and more explicit SQL++ way of achieving the result shown above is:

        USE TinySocial;

        SELECT VALUE {"user": user, "message": msg}
        FROM GleambookUsers user, GleambookMessages msg
        WHERE msg.authorId = user.id;

This version of the query uses an explicit object constructor to build each result object.
(Note that "uname" and "message" are both simple SQL++ expressions themselves---so in the most general case,
even the resulting field names can be computed as part of the query,
making SQL++ a very powerful tool for slicing and dicing semistructured data.)

### Query 2-B - Index join ###
By default, AsterixDB evaluates equijoin queries using hash-based join methods that work
well for doing ad hoc joins of very large data sets
([http://en.wikipedia.org/wiki/Hash_join](http://en.wikipedia.org/wiki/Hash_join)).
On a cluster, hash partitioning is employed as AsterixDB's divide-and-conquer strategy for
computing large parallel joins.
AsterixDB includes other join methods, but in the absence of data statistics and selectivity
estimates, it doesn't (yet) have the know-how to intelligently choose among its alternatives.
We therefore asked ourselves the classic question---WWOD?---What Would Oracle Do?---and in the
interim, SQL++ includes a clunky (but useful) hint-based mechanism for addressing the occasional
need to suggest to AsterixDB which join method it should use for a particular SQL++ query.

The following query is similar to the first version of Query 2-A but includes a suggestion to AsterixDB
that it should consider employing an index-based nested-loop join technique to process the query:

        USE TinySocial;

        SELECT user.name AS uname, msg.message AS message
        FROM GleambookUsers user, GleambookMessages msg
        WHERE msg.authorId /*+ indexnl */ = user.id;

In addition to illustrating the use of a hint, the query also shows how to achieve the same
result object format using _SELECT_ and _AS_ instead of using an explicit object constructor.
The expected result is (of course) the same as before, modulo the order of the instances.
Result ordering is (intentionally) undefined in SQL++ in the absence of an _ORDER BY_ clause.
The query result for our sample data in this case is:

        { "uname": "IsbelDull", "message": " like product-z its platform is mind-blowing" }
        { "uname": "MargaritaStoddard", "message": " like ccast the 3G is awesome:)" }
        { "uname": "EmoryUnk", "message": " love ccast its wireless is good" }
        { "uname": "MargaritaStoddard", "message": " can't stand product-w the touch-screen is terrible" }
        { "uname": "MargaritaStoddard", "message": " can't stand acast its plan is terrible" }
        { "uname": "BramHatch", "message": " can't stand product-z its voicemail-service is OMG:(" }
        { "uname": "WoodrowNehling", "message": " love acast its 3G is good:)" }
        { "uname": "EmoryUnk", "message": " love product-b its shortcut-menu is awesome:)" }
        { "uname": "MargaritaStoddard", "message": " dislike x-phone its touch-screen is horrible" }
        { "uname": "IsbelDull", "message": " like product-y the plan is amazing" }
        { "uname": "MargaritaStoddard", "message": " can't stand acast the network is horrible:(" }
        { "uname": "WillisWynne", "message": " love product-b the customization is mind-blowing" }
        { "uname": "VonKemble", "message": " dislike product-b the speed is horrible" }
        { "uname": "BramHatch", "message": " dislike x-phone the voice-command is bad:(" }
        { "uname": "SuzannaTillson", "message": " like x-phone the voicemail-service is awesome" }

(It is worth knowing, with respect to influencing AsterixDB's query evaluation,
that _FROM_ clauses---*a.k.a.* joins--- are currently evaluated in order,
with the "left" clause probing the data of the "right" clause.
SQL++ also supports SQL-style _JOIN_ clauses, and the same is true for those.)

### Query 3 - Nested Outer Join ###
In order to support joins between tables with missing/dangling join tuples, the designers of SQL ended
up shoe-horning a subset of the relational algebra into SQL's _FROM_ clause syntax---and providing a
variety of join types there for users to choose from (which SQL++ supports for SQL compatibility).
Left outer joins are particularly important in SQL, e.g., to print a summary of customers and orders,
grouped by customer, without omitting those customers who haven't placed any orders yet.

The SQL++ language supports nesting, both of queries and of query results, and the combination allows for
an arguably cleaner/more natural approach to such queries.
As an example, supposed we wanted, for each Gleambook user, to produce a object that has his/her name
plus a list of the messages written by that user.
In SQL, this would involve a left outer join between users and messages, grouping by user, and having
the user name repeated along side each message.
In SQL++, this sort of use case can be handled (more naturally) as follows:

        USE TinySocial;

        SELECT user.name AS uname,
               (SELECT VALUE msg.message
                FROM GleambookMessages msg
                WHERE msg.authorId = user.id) AS messages
        FROM GleambookUsers user;

This SQL++ query binds the variable `user` to the data instances in GleambookUsers;
for each user, it constructs a result object containing a "uname" field with the user's
name and a "messages" field with a nested collection of all messages for that user.
The nested collection for each user is specified by using a correlated subquery.
(Note: While it looks like nested loops could be involved in computing the result,
AsterixDB recogizes the equivalence of such a query to an outerjoin, and it will
use an efficient hash-based strategy when actually computing the query's result.)

Here is this example query's expected output:

        { "uname": "WillisWynne", "messages": [ " love product-b the customization is mind-blowing" ] }
        { "uname": "NilaMilliron", "messages": [  ] }
        { "uname": "WoodrowNehling", "messages": [ " love acast its 3G is good:)" ] }
        { "uname": "BramHatch", "messages": [ " dislike x-phone the voice-command is bad:(", " can't stand product-z its voicemail-service is OMG:(" ] }
        { "uname": "MargaritaStoddard", "messages": [ " dislike x-phone its touch-screen is horrible", " can't stand acast the network is horrible:(", " like ccast the 3G is awesome:)", " can't stand product-w the touch-screen is terrible", " can't stand acast its plan is terrible" ] }
        { "uname": "IsbelDull", "messages": [ " like product-y the plan is amazing", " like product-z its platform is mind-blowing" ] }
        { "uname": "EmoryUnk", "messages": [ " love product-b its shortcut-menu is awesome:)", " love ccast its wireless is good" ] }
        { "uname": "NicholasStroh", "messages": [  ] }
        { "uname": "VonKemble", "messages": [ " dislike product-b the speed is horrible" ] }
        { "uname": "SuzannaTillson", "messages": [ " like x-phone the voicemail-service is awesome" ] }

### Query 4 - Theta Join ###
Not all joins are expressible as equijoins and computable using equijoin-oriented algorithms.
The join predicates for some use cases involve predicates with functions; AsterixDB supports the
expression of such queries and will still evaluate them as best it can using nested loop based
techniques (and broadcast joins in the parallel case).

As an example of such a use case, suppose that we wanted, for each chirp message C, to find all of the
other chirp messages that originated from within a circle of radius of 1 surrounding chirp C's location.
In SQL++, this can be specified in a manner similar to the previous query using one of the built-in
functions on the spatial data type instead of id equality in the correlated query's _WHERE_ clause:

        USE TinySocial;

        SELECT cm1.messageText AS message,
               (SELECT VALUE cm2.messageText
                FROM ChirpMessages cm2
                WHERE `spatial-distance`(cm1.senderLocation, cm2.senderLocation) <= 1
                  AND cm2.chirpId < cm1.chirpId) AS nearbyMessages
        FROM ChirpMessages cm1;

Here is the expected result for this query:

        { "message": " can't stand x-phone its platform is terrible", "nearbyMessages": [  ] }
        { "message": " like ccast its shortcut-menu is awesome:)", "nearbyMessages": [  ] }
        { "message": " like product-w the speed is good:)", "nearbyMessages": [ " hate ccast its voice-clarity is OMG:(" ] }
        { "message": " like product-b the voice-command is mind-blowing:)", "nearbyMessages": [  ] }
        { "message": " like x-phone the voice-clarity is good:)", "nearbyMessages": [  ] }
        { "message": " like product-y the platform is good", "nearbyMessages": [  ] }
        { "message": " love ccast its voicemail-service is awesome", "nearbyMessages": [  ] }
        { "message": " love product-z its customization is good:)", "nearbyMessages": [  ] }
        { "message": " hate ccast its voice-clarity is OMG:(", "nearbyMessages": [  ] }
        { "message": " like product-y the voice-command is amazing:)", "nearbyMessages": [  ] }
        { "message": " can't stand product-w its speed is terrible:(", "nearbyMessages": [  ] }
        { "message": " like product-z the shortcut-menu is awesome:)", "nearbyMessages": [  ] }

### Query 5 - Fuzzy Join ###
As another example of a non-equijoin use case, we could ask AsterixDB to find, for each Gleambook user,
all Chirp users with names "similar" to their name.
AsterixDB supports a variety of "fuzzy match" functions for use with textual and set-based data.
As one example, we could choose to use edit distance with a threshold of 3 as the definition of name
similarity, in which case we could write the following query using SQL++'s operator-based syntax (~=)
for testing whether or not two values are similar:

        USE TinySocial;
        SET simfunction "edit-distance";
        SET simthreshold "3";

        SELECT gbu.id AS id, gbu.name AS name,
               (SELECT cm.user.screenName AS chirpScreenname,
                       cm.user.name AS chirpName
                FROM ChirpMessages cm
                WHERE cm.user.name ~= gbu.name) AS similarUsers
        FROM GleambookUsers gbu;

The expected result for this query against our sample data is:

        { "id": 6, "name": "WillisWynne", "similarUsers": [  ] }
        { "id": 8, "name": "NilaMilliron", "similarUsers": [ { "chirpScreenname": "NilaMilliron_tw", "chirpName": "Nila Milliron" } ] }
        { "id": 9, "name": "WoodrowNehling", "similarUsers": [  ] }
        { "id": 10, "name": "BramHatch", "similarUsers": [  ] }
        { "id": 1, "name": "MargaritaStoddard", "similarUsers": [  ] }
        { "id": 2, "name": "IsbelDull", "similarUsers": [  ] }
        { "id": 3, "name": "EmoryUnk", "similarUsers": [  ] }
        { "id": 4, "name": "NicholasStroh", "similarUsers": [  ] }
        { "id": 5, "name": "VonKemble", "similarUsers": [  ] }
        { "id": 7, "name": "SuzannaTillson", "similarUsers": [  ] }

### Query 6 - Existential Quantification ###
The expressive power of SQL++ includes support for queries involving "some" (existentially quantified)
and "all" (universally quantified) query semantics.
As an example of an existential SQL++ query, here we show a query to list the Gleambook users who are currently employed.
Such employees will have an employment history containing a object in which the end-date field is _MISSING_
(or it could be there but have the value _NULL_, as JSON unfortunately provides two ways to represent unknown values).
This leads us to the following SQL++ query:

        USE TinySocial;

        SELECT VALUE gbu
        FROM GleambookUsers gbu
        WHERE (SOME e IN gbu.employment SATISFIES e.endDate IS UNKNOWN);

The expected result in this case is:

        { "id": 6, "alias": "Willis", "name": "WillisWynne", "userSince": datetime("2005-01-17T10:10:00.000Z"), "friendIds": {{ 1, 3, 7 }}, "employment": [ { "organizationName": "jaydax", "startDate": date("2009-05-15") } ] }
        { "id": 8, "alias": "Nila", "name": "NilaMilliron", "userSince": datetime("2008-01-01T10:10:00.000Z"), "friendIds": {{ 3 }}, "employment": [ { "organizationName": "Plexlane", "startDate": date("2010-02-28") } ] }
        { "id": 1, "alias": "Margarita", "name": "MargaritaStoddard", "userSince": datetime("2012-08-20T10:10:00.000Z"), "friendIds": {{ 2, 3, 6, 10 }}, "employment": [ { "organizationName": "Codetechno", "startDate": date("2006-08-06") }, { "organizationName": "geomedia", "startDate": date("2010-06-17"), "endDate": date("2010-01-26") } ], "nickname": "Mags", "gender": "F" }
        { "id": 2, "alias": "Isbel", "name": "IsbelDull", "userSince": datetime("2011-01-22T10:10:00.000Z"), "friendIds": {{ 1, 4 }}, "employment": [ { "organizationName": "Hexviafind", "startDate": date("2010-04-27") } ], "nickname": "Izzy" }
        { "id": 4, "alias": "Nicholas", "name": "NicholasStroh", "userSince": datetime("2010-12-27T10:10:00.000Z"), "friendIds": {{ 2 }}, "employment": [ { "organizationName": "Zamcorporation", "startDate": date("2010-06-08") } ] }
        { "id": 5, "alias": "Von", "name": "VonKemble", "userSince": datetime("2010-01-05T10:10:00.000Z"), "friendIds": {{ 3, 6, 10 }}, "employment": [ { "organizationName": "Kongreen", "startDate": date("2010-11-27") } ] }
        { "id": 7, "alias": "Suzanna", "name": "SuzannaTillson", "userSince": datetime("2012-08-07T10:10:00.000Z"), "friendIds": {{ 6 }}, "employment": [ { "organizationName": "Labzatron", "startDate": date("2011-04-19") } ] }

### Query 7 - Universal Quantification ###
As an example of a universal SQL++ query, here we show a query to list the Gleambook users who are currently unemployed.
Such employees will have an employment history containing no objects with unknown end-date field values, leading us to the
following SQL++ query:

        USE TinySocial;

        SELECT VALUE gbu
        FROM GleambookUsers gbu
        WHERE (EVERY e IN gbu.employment SATISFIES e.endDate IS NOT UNKNOWN);

Here is the expected result for our sample data:

        { "id": 9, "alias": "Woodrow", "name": "WoodrowNehling", "userSince": datetime("2005-09-20T10:10:00.000Z"), "friendIds": {{ 3, 10 }}, "employment": [ { "organizationName": "Zuncan", "startDate": date("2003-04-22"), "endDate": date("2009-12-13") } ], "nickname": "Woody" }
        { "id": 10, "alias": "Bram", "name": "BramHatch", "userSince": datetime("2010-10-16T10:10:00.000Z"), "friendIds": {{ 1, 5, 9 }}, "employment": [ { "organizationName": "physcane", "startDate": date("2007-06-05"), "endDate": date("2011-11-05") } ] }
        { "id": 3, "alias": "Emory", "name": "EmoryUnk", "userSince": datetime("2012-07-10T10:10:00.000Z"), "friendIds": {{ 1, 5, 8, 9 }}, "employment": [ { "organizationName": "geomedia", "startDate": date("2010-06-17"), "endDate": date("2010-01-26") } ] }

### Query 8 - Simple Aggregation ###
Like SQL, the SQL++ language of AsterixDB provides support for computing aggregates over large amounts of data.
As a very simple example, the following SQL++ query computes the total number of Gleambook users in a SQL-like way:

        USE TinySocial;

        SELECT COUNT(gbu) AS numUsers FROM GleambookUsers gbu;

This query's result will be:

        { "numUsers": 10 }

If an "unwrapped" value is preferred, the following variant could be used instead:

        SELECT VALUE COUNT(gbu) FROM GleambookUsers gbu;

This time the result will simply be:

        10

In SQL++, aggregate functions can be applied to arbitrary collections, including subquery results.
To illustrate, here is a less SQL-like---and also more explicit---way to express the query above:

        SELECT VALUE ARRAY_COUNT((SELECT gbu FROM GleambookUsers gbu));

For each traditional SQL aggregate function _F_, SQL++ has a corresponding function _ARRAY_F_ that
can be used to perform the desired aggregate calculation.
Each such function is a regular function that takes a collection-valued argument to aggregate over.
Thus, the query above counts the results produced by the GleambookUsers subquery, and the previous,
more SQL-like versions are just syntactic sugar for SQL++ queries that use _ARRAY_COUNT_.
(Note: Subqueries in SQL++ must always be parenthesized.)

### Query 9-A - Grouping and Aggregation ###
Also like SQL, SQL++ supports grouped aggregation.
For every Chirp user, the following group-by/aggregate query counts the number of chirps sent by that user:

        USE TinySocial;

        SELECT uid AS user, COUNT(cm) AS count
        FROM ChirpMessages cm
        GROUP BY cm.user.screenName AS uid;

The _FROM_ clause incrementally binds the variable _cm_ to chirps, and the _GROUP BY_ clause groups
the chirps by their issuer's Chirp screen-name.
Unlike SQL, where data is tabular---flat---the data model underlying SQL++ allows for nesting.
Thus, due to the _GROUP BY_ clause, the _SELECT_ clause in this query sees a sequence of _cm_ groups,
with each such group having an associated _uid_ variable value (i.e., the chirping user's screen name).
In the context of the _SELECT_ clause, _uid_ is bound to the chirper's id and _cm_
is now re-bound (due to grouping) to the _set_ of chirps issued by that chirper.
The _SELECT_ clause yields a result object containing the chirper's user id and the count of the items
in the associated chirp set.
The query result will contain one such object per screen name.
This query also illustrates another feature of SQL++; notice how each user's screen name is accessed via a
path syntax that traverses each chirp's nested object structure.

Here is the expected result for this query over the sample data:

        { "user": "ChangEwing_573", "count": 1 }
        { "user": "OliJackson_512", "count": 1 }
        { "user": "ColineGeyer@63", "count": 3 }
        { "user": "NathanGiesen@211", "count": 6 }
        { "user": "NilaMilliron_tw", "count": 1 }

### Query 9-B - (Hash-Based) Grouping and Aggregation ###
As for joins, AsterixDB has multiple evaluation strategies available for processing grouped aggregate queries.
For grouped aggregation, the system knows how to employ both sort-based and hash-based aggregation methods,
with sort-based methods being used by default and a hint being available to suggest that a different approach
be used in processing a particular SQL++ query.

The following query is similar to Query 9-A, but adds a hash-based aggregation hint:

        USE TinySocial;

        SELECT uid AS user, COUNT(cm) AS count
        FROM ChirpMessages cm
         /*+ hash */
        GROUP BY cm.user.screenName AS uid;

Here is the expected result (the same result, but in a slightly different order):

        { "user": "OliJackson_512", "count": 1 }
        { "user": "ChangEwing_573", "count": 1 }
        { "user": "ColineGeyer@63", "count": 3 }
        { "user": "NathanGiesen@211", "count": 6 }
        { "user": "NilaMilliron_tw", "count": 1 }

### Query 10 - Grouping and Limits ###
In some use cases it is not necessary to compute the entire answer to a query.
In some cases, just having the first _N_ or top _N_ results is sufficient.
This is expressible in SQL++ using the _LIMIT_ clause combined with the _ORDER BY_ clause.

The following SQL++ query returns the top 3 Chirp users based on who has issued the most chirps:

        USE TinySocial;

        SELECT uid AS user, c AS count
        FROM ChirpMessages cm
        GROUP BY cm.user.screenName AS uid WITH c AS count(cm)
        ORDER BY c DESC
        LIMIT 3;

The expected result for this query is:

        { "user": "NathanGiesen@211", "count": 6 }
        { "user": "ColineGeyer@63", "count": 3 }
        { "user": "ChangEwing_573", "count": 1 }

### Query 11 - Left Outer Fuzzy Join ###
As a last example of SQL++ and its query power, the following query, for each chirp,
finds all of the chirps that are similar based on the topics that they refer to:

        USE TinySocial;
        SET simfunction "jaccard";
        SET simthreshold "0.3";

        SELECT cm1 AS chirp,
               (SELECT VALUE cm2.chirpId
                FROM ChirpMessages cm2
                WHERE cm2.referredTopics ~= cm1.referredTopics
                  AND cm2.chirpId > cm1.chirpId) AS similarChirps
        FROM ChirpMessages cm1;

This query illustrates several things worth knowing in order to write fuzzy queries in SQL++.
First, as mentioned earlier, SQL++ offers an operator-based syntax (as well as a functional approach, not shown)
for seeing whether two values are "similar" to one another or not.
Second, recall that the referredTopics field of objects of datatype ChirpMessageType is a bag of strings.
This query sets the context for its similarity join by requesting that Jaccard-based similarity semantics
([http://en.wikipedia.org/wiki/Jaccard_index](http://en.wikipedia.org/wiki/Jaccard_index))
be used for the query's similarity operator and that a similarity index of 0.3 be used as its similarity threshold.

The expected result for this fuzzy join query is:

        { "chirp": { "chirpId": "11", "user": { "screenName": "NilaMilliron_tw", "lang": "en", "friendsCount": 445, "statusesCount": 164, "name": "Nila Milliron", "followersCount": 22649 }, "senderLocation": point("37.59,68.42"), "sendTime": datetime("2008-03-09T10:10:00.000Z"), "referredTopics": {{ "x-phone", "platform" }}, "messageText": " can't stand x-phone its platform is terrible" }, "similarChirps": [ "6", "7" ] }
        { "chirp": { "chirpId": "2", "user": { "screenName": "ColineGeyer@63", "lang": "en", "friendsCount": 121, "statusesCount": 362, "name": "Coline Geyer", "followersCount": 17159 }, "senderLocation": point("32.84,67.14"), "sendTime": datetime("2010-05-13T10:10:00.000Z"), "referredTopics": {{ "ccast", "shortcut-menu" }}, "messageText": " like ccast its shortcut-menu is awesome:)" }, "similarChirps": [ "9", "8" ] }
        { "chirp": { "chirpId": "3", "user": { "screenName": "NathanGiesen@211", "lang": "en", "friendsCount": 39339, "statusesCount": 473, "name": "Nathan Giesen", "followersCount": 49416 }, "senderLocation": point("29.72,75.8"), "sendTime": datetime("2006-11-04T10:10:00.000Z"), "referredTopics": {{ "product-w", "speed" }}, "messageText": " like product-w the speed is good:)" }, "similarChirps": [ "5" ] }
        { "chirp": { "chirpId": "4", "user": { "screenName": "NathanGiesen@211", "lang": "en", "friendsCount": 39339, "statusesCount": 473, "name": "Nathan Giesen", "followersCount": 49416 }, "senderLocation": point("39.28,70.48"), "sendTime": datetime("2011-12-26T10:10:00.000Z"), "referredTopics": {{ "product-b", "voice-command" }}, "messageText": " like product-b the voice-command is mind-blowing:)" }, "similarChirps": [  ] }
        { "chirp": { "chirpId": "6", "user": { "screenName": "ColineGeyer@63", "lang": "en", "friendsCount": 121, "statusesCount": 362, "name": "Coline Geyer", "followersCount": 17159 }, "senderLocation": point("47.51,83.99"), "sendTime": datetime("2010-05-07T10:10:00.000Z"), "referredTopics": {{ "x-phone", "voice-clarity" }}, "messageText": " like x-phone the voice-clarity is good:)" }, "similarChirps": [  ] }
        { "chirp": { "chirpId": "7", "user": { "screenName": "ChangEwing_573", "lang": "en", "friendsCount": 182, "statusesCount": 394, "name": "Chang Ewing", "followersCount": 32136 }, "senderLocation": point("36.21,72.6"), "sendTime": datetime("2011-08-25T10:10:00.000Z"), "referredTopics": {{ "product-y", "platform" }}, "messageText": " like product-y the platform is good" }, "similarChirps": [  ] }
        { "chirp": { "chirpId": "9", "user": { "screenName": "NathanGiesen@211", "lang": "en", "friendsCount": 39339, "statusesCount": 473, "name": "Nathan Giesen", "followersCount": 49416 }, "senderLocation": point("36.86,74.62"), "sendTime": datetime("2012-07-21T10:10:00.000Z"), "referredTopics": {{ "ccast", "voicemail-service" }}, "messageText": " love ccast its voicemail-service is awesome" }, "similarChirps": [  ] }
        { "chirp": { "chirpId": "1", "user": { "screenName": "NathanGiesen@211", "lang": "en", "friendsCount": 39339, "statusesCount": 473, "name": "Nathan Giesen", "followersCount": 49416 }, "senderLocation": point("47.44,80.65"), "sendTime": datetime("2008-04-26T10:10:00.000Z"), "referredTopics": {{ "product-z", "customization" }}, "messageText": " love product-z its customization is good:)" }, "similarChirps": [ "8" ] }
        { "chirp": { "chirpId": "10", "user": { "screenName": "ColineGeyer@63", "lang": "en", "friendsCount": 121, "statusesCount": 362, "name": "Coline Geyer", "followersCount": 17159 }, "senderLocation": point("29.15,76.53"), "sendTime": datetime("2008-01-26T10:10:00.000Z"), "referredTopics": {{ "ccast", "voice-clarity" }}, "messageText": " hate ccast its voice-clarity is OMG:(" }, "similarChirps": [ "2", "6", "9" ] }
        { "chirp": { "chirpId": "12", "user": { "screenName": "OliJackson_512", "lang": "en", "friendsCount": 445, "statusesCount": 164, "name": "Oli Jackson", "followersCount": 22649 }, "senderLocation": point("24.82,94.63"), "sendTime": datetime("2010-02-13T10:10:00.000Z"), "referredTopics": {{ "product-y", "voice-command" }}, "messageText": " like product-y the voice-command is amazing:)" }, "similarChirps": [ "4", "7" ] }
        { "chirp": { "chirpId": "5", "user": { "screenName": "NathanGiesen@211", "lang": "en", "friendsCount": 39339, "statusesCount": 473, "name": "Nathan Giesen", "followersCount": 49416 }, "senderLocation": point("40.09,92.69"), "sendTime": datetime("2006-08-04T10:10:00.000Z"), "referredTopics": {{ "product-w", "speed" }}, "messageText": " can't stand product-w its speed is terrible:(" }, "similarChirps": [  ] }
        { "chirp": { "chirpId": "8", "user": { "screenName": "NathanGiesen@211", "lang": "en", "friendsCount": 39339, "statusesCount": 473, "name": "Nathan Giesen", "followersCount": 49416 }, "senderLocation": point("46.05,93.34"), "sendTime": datetime("2005-10-14T10:10:00.000Z"), "referredTopics": {{ "product-z", "shortcut-menu" }}, "messageText": " like product-z the shortcut-menu is awesome:)" }, "similarChirps": [  ] }

## Inserting New Data  ###
In addition to loading and querying data, AsterixDB supports incremental additions to datasets via the SQL++ _INSERT_ statement.

The following example adds a new chirp by user "NathanGiesen@211" to the ChirpMessages dataset.
(An astute reader may notice that this chirp was issued a half an hour after his last chirp, so his counts
have all gone up in the interim, although he appears not to have moved in the last half hour.)

        USE TinySocial;

        INSERT INTO ChirpMessages
        (
           {"chirpId": "13",
            "user":
                {"screenName": "NathanGiesen@211",
                 "lang": "en",
                 "friendsCount": 39345,
                 "statusesCount": 479,
                 "name": "Nathan Giesen",
                 "followersCount": 49420
                },
            "senderLocation": point("47.44,80.65"),
            "sendTime": datetime("2008-04-26T10:10:35"),
            "referredTopics": {{"chirping"}},
            "messageText": "chirpy chirp, my fellow chirpers!"
           }
        );

In general, the data to be inserted may be specified using any valid SQL++ query expression.
The insertion of a single object instance, as in this example, is just a special case where
the query expression happens to be a object constructor involving only constants.

### Deleting Existing Data  ###
In addition to inserting new data, AsterixDB supports deletion from datasets via the SQL++ _DELETE_ statement.
The statement supports "searched delete" semantics, and its
_WHERE_ clause can involve any valid XQuery expression.

The following example deletes the chirp that we just added from user "NathanGiesen@211".  (Easy come, easy go. :-))

        USE TinySocial;
        DELETE FROM ChirpMessages cm WHERE cm.chirpId = "13";

It should be noted that one form of data change not yet supported by AsterixDB is in-place data modification (_update_).
Currently, only insert and delete operations are supported in SQL++; updates are not.
To achieve the effect of an update, two SQL++ statements are currently needed---one to delete the old object from the
dataset where it resides, and another to insert the new replacement object (with the same primary key but with
different field values for some of the associated data content).
AQL additionally supports an upsert operation to either insert a object, if no object with its primary key is currently
present in the dataset, or to replace the existing object if one already exists with the primary key value being upserted.
SQL++ will soon have _UPSERT_ as well.

### Transaction Support

AsterixDB supports object-level ACID transactions that begin and terminate implicitly for each object inserted, deleted, or searched while a given SQL++ statement is being executed. This is quite similar to the level of transaction support found in today's NoSQL stores. AsterixDB does not support multi-statement transactions, and in fact an SQL++ statement that involves multiple objects can itself involve multiple independent object-level transactions. An example consequence of this is that, when an SQL++ statement attempts to insert 1000 objects, it is possible that the first 800 objects could end up being committed while the remaining 200 objects fail to be inserted. This situation could happen, for example, if a duplicate key exception occurs as the 801st insertion is attempted. If this happens, AsterixDB will report the error (e.g., a duplicate key exception) as the result of the offending SQL++ _INSERT_ statement, and the application logic above will need to take the appropriate action(s) needed to assess the resulting state and to clean up and/or continue as appropriate.


### Loading New Data in Bulk  ###

In addition to incremental additions to datasets via the SQL++ _insert_ statement, the _load_ statement can be used to
take a file from a given node and load it in a more efficient fashion. Note however that a dataset can currently only
be loaded if it is empty.

The following example loads a file in ADM format from "/home/user/gbm.adm" from the node named "nc1" into the GleambookUsers dataset.

    USE TinySocial;

    LOAD DATASET GleambookUsers USING localfs
        (("path"="nc1://home/user/gbu.adm"),("format"="adm"));

## Further Help ##
That's it! You are now armed and dangerous with respect to semistructured data management using AsterixDB via SQL++.
More information about SQL++ is available in the SQL++ Query Language (SQL++) reference document as well as in its companion SQL++ Functions document.

AsterixDB is a powerful new BDMS---Big Data Management System---that we hope may usher in a new era of much
more declarative Big Data management.
AsterixDB is powerful, so use it wisely, and remember: "With great power comes great responsibility..." :-)

Please e-mail the AsterixDB user group
(users (at) asterixdb.apache.org)
if you run into any problems or simply have further questions about the AsterixDB system, its features, or their proper use.
