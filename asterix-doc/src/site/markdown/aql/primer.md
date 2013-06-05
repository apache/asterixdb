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

----
## ADM: Modeling Semistructed Data in AsterixDB ##
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

Our little sample scenario involves hypothetical information about users of two popular social networks,
Facebook and Twitter, and their messages.
We'll start by defining a dataverse called "TinySocial" to hold our datatypes and datasets.
The AsterixDB data model (ADM) is essentially a superset of JSON---it's what you get by extending
JSON with more data types and additional data modeling constructs borrowed from object databases.
The following is how we can create the TinySocial dataverse plus a set of ADM types for modeling
Twitter users, their Tweets, Facebook users, their users' employment information, and their messages.
(Note: Keep in mind that this is just a tiny and somewhat silly example intended for illustrating
some of the key features of AsterixDB. :-))


        drop dataverse TinySocial if exists;
        create dataverse TinySocial;
        use dataverse TinySocial;
        
        create type TwitterUserType as open {
        	screen-name: string,
        	lang: string,
        	friends_count: int32,
        	statuses_count: int32,
        	name: string,
        	followers_count: int32
        }
        
        create type TweetMessageType as closed {
        	tweetid: string,
        	user: TwitterUserType,
        	sender-location: point?,
        	send-time: datetime,
        	referred-topics: {{ string }},
        	message-text: string
        }
        
        create type EmploymentType as open {
        	organization-name: string,
        	start-date: date,
        	end-date: date?
        }
        
        create type FacebookUserType as closed {
        	id: int32,
        	alias: string,
        	name: string,
        	user-since: datetime,
        	friend-ids: {{ int32 }},
        	employment: [EmploymentType]
        }
        
        create type FacebookMessageType as closed {
        	message-id: int32,
        	author-id: int32,
        	in-response-to: int32?,
        	sender-location: point?,
        	message: string
        }
        


The first three lines above tell AsterixDB to drop the old TinySocial dataverse, if one already
exists, and then to create a brand new one and make it the focus of the statements that follow.
The first type creation statement creates a datatype for holding information about Twitter users.
It is a record type with a mix of integer and string data, very much like a (flat) relational tuple.
The indicated fields are all mandatory, but because the type is open, additional fields are welcome.
The second statement creates a datatype for Twitter messages; this shows how to specify a closed type.
Interestingly (based on one of Twitter's APIs), each Twitter message actually embeds an instance of the
sending user's information (current as of when the message was sent), so this is an example of a nested
record in ADM.
Twitter messages can optionally contain the sender's location, which is modeled via the sender-location
field of spatial type _point_; the question mark following the field type indicates its optionality.
An optional field is like a nullable field in SQL---it may be present or missing, but when it's present,
its data type will conform to the datatype's specification.
The send-time field illustrates the use of a temporal primitive type, _datetime_.
Lastly, the referred-topics field illustrates another way that ADM is richer than the relational model;
this field holds a bag (a.k.a. an unordered list) of strings.
Since the overall datatype definition for Twitter messages says "closed", the fields that it lists are
the only fields that instances of this type will be allowed to contain.
The next two create type statements create a record type for holding information about one component of
the employment history of a Facebook user and then a record type for holding the user information itself.
The Facebook user type highlights a few additional ADM data model features.
Its friend-ids field is a bag of integers, presumably the Facebook user ids for this user's friends,
and its employment field is an ordered list of employment records.
The final create type statement defines a type for handling the content of a Facebook message in our
hypothetical social data storage scenario.

Before going on, we need to once again emphasize the idea that AsterixDB is aimed at storing
and querying not just Big Data, but Big _Semistructured_ Data.
This means that most of the fields listed in the create type statements above could have been
omitted without changing anything other than the resulting size of stored data instances on disk.
AsterixDB stores its information about the fields defined a priori as separate metadata, whereas
the information about other fields that are "just there" in instances of open datatypes is stored
with each instance---making for more bits on disk and longer times for operations affected by
data size (e.g., dataset scans).
The only fields that _must_ be specified a priori are the primary key and any fields that you
would like to build indexes on.
(AsterixDB does not yet support auto-generated keys or indexes on the unspecified "open" fields
of its data instances).

### Creating Datasets and Indexes ###

Now that we have defined our datatypes, we can move on and create datasets to store the actual data.
(If we wanted to, we could even have several named datasets based on any one of these datatypes.)
We can do this as follows, utilizing the DDL capabilities of AsterixDB.


        
        use dataverse TinySocial;
        
        create dataset FacebookUsers(FacebookUserType)
        primary key id;
        
        create dataset FacebookMessages(FacebookMessageType)
        primary key message-id;
        
        create dataset TwitterUsers(TwitterUserType)
        primary key screen-name;
        
        create dataset TweetMessages(TweetMessageType)
        primary key tweetid
        hints(cardinality=100);
        
        create index fbUserSinceIdx on FacebookUsers(user-since);
        create index fbAuthorIdx on FacebookMessages(author-id) type btree;
        create index fbSenderLocIndex on FacebookMessages(sender-location) type rtree;
        create index fbMessageIdx on FacebookMessages(message) type keyword;
        
        for $ds in dataset Metadata.Dataset return $ds;
        for $ix in dataset Metadata.Index return $ix;
        


The ADM DDL statements above create four datasets for holding our social data in the TinySocial
dataverse: FacebookUsers, FacebookMessages, TwitterUsers, and TweetMessages.
The first statement creates the FacebookUsers data set.
It specifies that this dataset will store data instances conforming to FacebookUserType and that
it has a primary key which is the id field of each instance.
The primary key information is used by AsterixDB to uniquely identify instances for the purpose
of later lookup and for use in secondary indexes.
Each AsterixDB dataset is stored (and indexed) in the form of a B+ tree on primary key;
secondary indexes point to their indexed data by primary key.
In AsterixDB clusters, the primary key is also used to hash-partition (a.k.a. shard) the
dataset across the nodes of the cluster.
The next three create dataset statements are similar.
The last one illustrates an optional clause for providing useful hints to AsterixDB.
In this case, the hint tells AsterixDB that the dataset definer is anticipating that the
TweetMessages dataset will contain roughly 100 objects; knowing this can help AsterixDB
to more efficiently manage and query this dataset.
(AsterixDB does not yet gather and maintain data statistics; it will currently, abitrarily,
assume a cardinality of one million objects per dataset in the absence of such an optional
definition-time hint.)

The create dataset statements above are followed by four more DDL statements, each of which
creates a secondary index on a field of one of the datasets.
The first one indexes the FacebookUsers dataset on its user-since field.
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

----
## Loading Data Into AsterixDB ##
Okay, so far so good---AsterixDB is now ready for data, so let's give it some data to store
Our next task will be to load some sample data into the four datasets that we just defined.
Here we will load a tiny set of records, defined in ADM format (a superset of JSON), into each dataset.
In the boxes below you can see the actual data instances contained in each of the provided sample files.
In order to load this data yourself, you should first store the four corresponding `.adm` files
(whose URLs are indicated on top of each box below) into a filesystem directory accessible to your
running AsterixDB instance.
Take a few minutes to look carefully at each of the sample data sets.
This will give you a better sense of the nature of the data that we are about to load and query.
We should note that ADM format is a textual serialization of what AsterixDB will actually store;
when persisted in AsterixDB, the data format will be binary and the data in the predefined fields
of the data instances will be stored separately from their associated field name and type metadata.

[Twitter Users](http://asterixdb.googlecode.com/files/twu.adm)

        {"screen-name":"NathanGiesen@211","lang":"en","friends_count":18,"statuses_count":473,"name":"Nathan Giesen","followers_count":49416}
        {"screen-name":"ColineGeyer@63","lang":"en","friends_count":121,"statuses_count":362,"name":"Coline Geyer","followers_count":17159}
        {"screen-name":"NilaMilliron_tw","lang":"en","friends_count":445,"statuses_count":164,"name":"Nila Milliron","followers_count":22649}
        {"screen-name":"ChangEwing_573","lang":"en","friends_count":182,"statuses_count":394,"name":"Chang Ewing","followers_count":32136}

[Tweet Messages](http://asterixdb.googlecode.com/files/twm.adm)

        {"tweetid":"1","user":{"screen-name":"NathanGiesen@211","lang":"en","friends_count":39339,"statuses_count":473,"name":"Nathan Giesen","followers_count":49416},"sender-location":point("47.44,80.65"),"send-time":datetime("2008-04-26T10:10:00"),"referred-topics":{{"t-mobile","customization"}},"message-text":" love t-mobile its customization is good:)"}
        {"tweetid":"2","user":{"screen-name":"ColineGeyer@63","lang":"en","friends_count":121,"statuses_count":362,"name":"Coline Geyer","followers_count":17159},"sender-location":point("32.84,67.14"),"send-time":datetime("2010-05-13T10:10:00"),"referred-topics":{{"verizon","shortcut-menu"}},"message-text":" like verizon its shortcut-menu is awesome:)"}
        {"tweetid":"3","user":{"screen-name":"NathanGiesen@211","lang":"en","friends_count":39339,"statuses_count":473,"name":"Nathan Giesen","followers_count":49416},"sender-location":point("29.72,75.8"),"send-time":datetime("2006-11-04T10:10:00"),"referred-topics":{{"motorola","speed"}},"message-text":" like motorola the speed is good:)"}
        {"tweetid":"4","user":{"screen-name":"NathanGiesen@211","lang":"en","friends_count":39339,"statuses_count":473,"name":"Nathan Giesen","followers_count":49416},"sender-location":point("39.28,70.48"),"send-time":datetime("2011-12-26T10:10:00"),"referred-topics":{{"sprint","voice-command"}},"message-text":" like sprint the voice-command is mind-blowing:)"}
        {"tweetid":"5","user":{"screen-name":"NathanGiesen@211","lang":"en","friends_count":39339,"statuses_count":473,"name":"Nathan Giesen","followers_count":49416},"sender-location":point("40.09,92.69"),"send-time":datetime("2006-08-04T10:10:00"),"referred-topics":{{"motorola","speed"}},"message-text":" can't stand motorola its speed is terrible:("}
        {"tweetid":"6","user":{"screen-name":"ColineGeyer@63","lang":"en","friends_count":121,"statuses_count":362,"name":"Coline Geyer","followers_count":17159},"sender-location":point("47.51,83.99"),"send-time":datetime("2010-05-07T10:10:00"),"referred-topics":{{"iphone","voice-clarity"}},"message-text":" like iphone the voice-clarity is good:)"}
        {"tweetid":"7","user":{"screen-name":"ChangEwing_573","lang":"en","friends_count":182,"statuses_count":394,"name":"Chang Ewing","followers_count":32136},"sender-location":point("36.21,72.6"),"send-time":datetime("2011-08-25T10:10:00"),"referred-topics":{{"samsung","platform"}},"message-text":" like samsung the platform is good"}
        {"tweetid":"8","user":{"screen-name":"NathanGiesen@211","lang":"en","friends_count":39339,"statuses_count":473,"name":"Nathan Giesen","followers_count":49416},"sender-location":point("46.05,93.34"),"send-time":datetime("2005-10-14T10:10:00"),"referred-topics":{{"t-mobile","shortcut-menu"}},"message-text":" like t-mobile the shortcut-menu is awesome:)"}
        {"tweetid":"9","user":{"screen-name":"NathanGiesen@211","lang":"en","friends_count":39339,"statuses_count":473,"name":"Nathan Giesen","followers_count":49416},"sender-location":point("36.86,74.62"),"send-time":datetime("2012-07-21T10:10:00"),"referred-topics":{{"verizon","voicemail-service"}},"message-text":" love verizon its voicemail-service is awesome"}
        {"tweetid":"10","user":{"screen-name":"ColineGeyer@63","lang":"en","friends_count":121,"statuses_count":362,"name":"Coline Geyer","followers_count":17159},"sender-location":point("29.15,76.53"),"send-time":datetime("2008-01-26T10:10:00"),"referred-topics":{{"verizon","voice-clarity"}},"message-text":" hate verizon its voice-clarity is OMG:("}
        {"tweetid":"11","user":{"screen-name":"NilaMilliron_tw","lang":"en","friends_count":445,"statuses_count":164,"name":"Nila Milliron","followers_count":22649},"sender-location":point("37.59,68.42"),"send-time":datetime("2008-03-09T10:10:00"),"referred-topics":{{"iphone","platform"}},"message-text":" can't stand iphone its platform is terrible"}
        {"tweetid":"12","user":{"screen-name":"OliJackson_512","lang":"en","friends_count":445,"statuses_count":164,"name":"Oli Jackson","followers_count":22649},"sender-location":point("24.82,94.63"),"send-time":datetime("2010-02-13T10:10:00"),"referred-topics":{{"samsung","voice-command"}},"message-text":" like samsung the voice-command is amazing:)"}

[Facebook Users](http://asterixdb.googlecode.com/files/fbu.adm)

        {"id":1,"alias":"Margarita","name":"MargaritaStoddard","user-since":datetime("2012-08-20T10:10:00"),"friend-ids":{{2,3,6,10}},"employment":[{"organization-name":"Codetechno","start-date":date("2006-08-06")}]}
        {"id":2,"alias":"Isbel","name":"IsbelDull","user-since":datetime("2011-01-22T10:10:00"),"friend-ids":{{1,4}},"employment":[{"organization-name":"Hexviafind","start-date":date("2010-04-27")}]}
        {"id":3,"alias":"Emory","name":"EmoryUnk","user-since":datetime("2012-07-10T10:10:00"),"friend-ids":{{1,5,8,9}},"employment":[{"organization-name":"geomedia","start-date":date("2010-06-17"),"end-date":date("2010-01-26")}]}
        {"id":4,"alias":"Nicholas","name":"NicholasStroh","user-since":datetime("2010-12-27T10:10:00"),"friend-ids":{{2}},"employment":[{"organization-name":"Zamcorporation","start-date":date("2010-06-08")}]}
        {"id":5,"alias":"Von","name":"VonKemble","user-since":datetime("2010-01-05T10:10:00"),"friend-ids":{{3,6,10}},"employment":[{"organization-name":"Kongreen","start-date":date("2010-11-27")}]}
        {"id":6,"alias":"Willis","name":"WillisWynne","user-since":datetime("2005-01-17T10:10:00"),"friend-ids":{{1,3,7}},"employment":[{"organization-name":"jaydax","start-date":date("2009-05-15")}]}
        {"id":7,"alias":"Suzanna","name":"SuzannaTillson","user-since":datetime("2012-08-07T10:10:00"),"friend-ids":{{6}},"employment":[{"organization-name":"Labzatron","start-date":date("2011-04-19")}]}
        {"id":8,"alias":"Nila","name":"NilaMilliron","user-since":datetime("2008-01-01T10:10:00"),"friend-ids":{{3}},"employment":[{"organization-name":"Plexlane","start-date":date("2010-02-28")}]}
        {"id":9,"alias":"Woodrow","name":"WoodrowNehling","user-since":datetime("2005-09-20T10:10:00"),"friend-ids":{{3,10}},"employment":[{"organization-name":"Zuncan","start-date":date("2003-04-22"),"end-date":date("2009-12-13")}]}
        {"id":10,"alias":"Bram","name":"BramHatch","user-since":datetime("2010-10-16T10:10:00"),"friend-ids":{{1,5,9}},"employment":[{"organization-name":"physcane","start-date":date("2007-06-05"),"end-date":date("2011-11-05")}]}

[Facebook Messages](http://asterixdb.googlecode.com/files/fbm.adm)

        {"message-id":1,"author-id":3,"in-response-to":2,"sender-location":point("47.16,77.75"),"message":" love sprint its shortcut-menu is awesome:)"}
        {"message-id":2,"author-id":1,"in-response-to":4,"sender-location":point("41.66,80.87"),"message":" dislike iphone its touch-screen is horrible"}
        {"message-id":3,"author-id":2,"in-response-to":4,"sender-location":point("48.09,81.01"),"message":" like samsung the plan is amazing"}
        {"message-id":4,"author-id":1,"in-response-to":2,"sender-location":point("37.73,97.04"),"message":" can't stand at&t the network is horrible:("}
        {"message-id":5,"author-id":6,"in-response-to":2,"sender-location":point("34.7,90.76"),"message":" love sprint the customization is mind-blowing"}
        {"message-id":6,"author-id":2,"in-response-to":1,"sender-location":point("31.5,75.56"),"message":" like t-mobile its platform is mind-blowing"}
        {"message-id":7,"author-id":5,"in-response-to":15,"sender-location":point("32.91,85.05"),"message":" dislike sprint the speed is horrible"}
        {"message-id":8,"author-id":1,"in-response-to":11,"sender-location":point("40.33,80.87"),"message":" like verizon the 3G is awesome:)"}
        {"message-id":9,"author-id":3,"in-response-to":12,"sender-location":point("34.45,96.48"),"message":" love verizon its wireless is good"}
        {"message-id":10,"author-id":1,"in-response-to":12,"sender-location":point("42.5,70.01"),"message":" can't stand motorola the touch-screen is terrible"}
        {"message-id":11,"author-id":1,"in-response-to":1,"sender-location":point("38.97,77.49"),"message":" can't stand at&t its plan is terrible"}
        {"message-id":12,"author-id":10,"in-response-to":6,"sender-location":point("42.26,77.76"),"message":" can't stand t-mobile its voicemail-service is OMG:("}
        {"message-id":13,"author-id":10,"in-response-to":4,"sender-location":point("42.77,78.92"),"message":" dislike iphone the voice-command is bad:("}
        {"message-id":14,"author-id":9,"in-response-to":12,"sender-location":point("41.33,85.28"),"message":" love at&t its 3G is good:)"}
        {"message-id":15,"author-id":7,"in-response-to":11,"sender-location":point("44.47,67.11"),"message":" like iphone the voicemail-service is awesome"}


It's loading time! We can use AQL _load_ statements to populate our datasets with the sample records shown above.
The following shows how loading can be done for data stored in `.adm` files in your local filesystem.
*Note:* You _MUST_ replace the `<Host Name>` and `<Absolute File Path>` placeholders in each load
statement below with valid values based on the host IP address (or host name) for the machine and
directory that you have downloaded the provided `.adm` files to.
As you do so, be very, very careful to retain the two slashes in the load statements, i.e.,
do not delete the two slashes that appear in front of the absolute path to your `.adm` files.
(This will lead to a three-slash character sequence at the start of each load statement's file
input path specification.)


        use dataverse TinySocial;
        
        load dataset FacebookUsers using localfs
        (("path"="<Host Name>://<Absolute File Path>/fbu.adm"),("format"="adm"));
        
        load dataset FacebookMessages using localfs
        (("path"="<Host Name>://<Absolute File Path>/fbm.adm"),("format"="adm"));
        
        load dataset TwitterUsers using localfs
        (("path"="<Host Name>://<Absolute File Path>/twu.adm"),("format"="adm"));
        
        load dataset TweetMessages using localfs
        (("path"="<Host Name>://<Absolute File Path>/twm.adm"),("format"="adm"));


----
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
You can find a BNF description of the current AQL grammar at [wiki:AsterixDBGrammar], and someday
in the not-too-distant future we will also provide a complete reference manual for the language.
In the meantime, this will get you started down the path of using AsterixDB.
A more complete list of the supported AsterixDB primitive types and built-in functions can be
found at [Asterix Data Model (ADM)](datamodel.html) and [Asterix Functions](functions.html).

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

Enough talk!
Let's go ahead and try writing some queries and see about learning AQL by example.

### Query 0-A - Exact-Match Lookup ###
For our first query, let's find a Facebook user based on his or her user id.
Suppose the user we want is the user whose id is 8:


        use dataverse TinySocial;
        
        for $user in dataset FacebookUsers
        where $user.id = 8
        return $user;

The query's _for_ clause  binds the variable `$user` incrementally to the data instances residing in
the dataset named FacebookUsers.
Its _where_ clause selects only those bindings having a user id of interest, filtering out the rest.
The _return_ clause returns the (entire) data instance for each binding that satisfies the predicate.
Since this dataset is indexed on user id (its primary key), this query will be done via a quick index lookup.

The expected result for our sample data is as follows:

        { "id": 8, "alias": "Nila", "name": "NilaMilliron", "user-since": datetime("2008-01-01T10:10:00.000Z"), "friend-ids": {{ 3 }}, "employment": [ { "organization-name": "Plexlane", "start-date": date("2010-02-28"), "end-date": null } ] }


### Query 0-B - Range Scan ###
AQL, like SQL, supports a variety of different predicates.
For example, for our next query, let's find the Facebook users whose ids are in the range between 2 and 4:

        use dataverse TinySocial;
        
        for $user in dataset FacebookUsers
        where $user.id >= 2 and $user.id <= 4
        return $user;

This query's expected result, also evaluable using the primary index on user id, is:

        { "id": 2, "alias": "Isbel", "name": "IsbelDull", "user-since": datetime("2011-01-22T10:10:00.000Z"), "friend-ids": {{ 1, 4 }}, "employment": [ { "organization-name": "Hexviafind", "start-date": date("2010-04-27"), "end-date": null } ] }
        { "id": 3, "alias": "Emory", "name": "EmoryUnk", "user-since": datetime("2012-07-10T10:10:00.000Z"), "friend-ids": {{ 1, 5, 8, 9 }}, "employment": [ { "organization-name": "geomedia", "start-date": date("2010-06-17"), "end-date": date("2010-01-26") } ] }
        { "id": 4, "alias": "Nicholas", "name": "NicholasStroh", "user-since": datetime("2010-12-27T10:10:00.000Z"), "friend-ids": {{ 2 }}, "employment": [ { "organization-name": "Zamcorporation", "start-date": date("2010-06-08"), "end-date": null } ] }


### Query 1 - Other Query Filters ###
AQL can do range queries on any data type that supports the appropriate set of comparators.
As an example, this next query retrieves the Facebook users who joined between July 22, 2010 and July 29, 2012:

        use dataverse TinySocial;
        
        for $user in dataset FacebookUsers
        where $user.user-since >= datetime('2010-07-22T00:00:00')
          and $user.user-since <= datetime('2012-07-29T23:59:59')
        return $user;

The expected result for this query, also an indexable query, is as follows:

        { "id": 2, "alias": "Isbel", "name": "IsbelDull", "user-since": datetime("2011-01-22T10:10:00.000Z"), "friend-ids": {{ 1, 4 }}, "employment": [ { "organization-name": "Hexviafind", "start-date": date("2010-04-27"), "end-date": null } ] }
        { "id": 3, "alias": "Emory", "name": "EmoryUnk", "user-since": datetime("2012-07-10T10:10:00.000Z"), "friend-ids": {{ 1, 5, 8, 9 }}, "employment": [ { "organization-name": "geomedia", "start-date": date("2010-06-17"), "end-date": date("2010-01-26") } ] }
        { "id": 4, "alias": "Nicholas", "name": "NicholasStroh", "user-since": datetime("2010-12-27T10:10:00.000Z"), "friend-ids": {{ 2 }}, "employment": [ { "organization-name": "Zamcorporation", "start-date": date("2010-06-08"), "end-date": null } ] }
        { "id": 10, "alias": "Bram", "name": "BramHatch", "user-since": datetime("2010-10-16T10:10:00.000Z"), "friend-ids": {{ 1, 5, 9 }}, "employment": [ { "organization-name": "physcane", "start-date": date("2007-06-05"), "end-date": date("2011-11-05") } ] }


### Query 2-A - Equijoin ###
In addition to simply binding variables to data instances and returning them "whole",
an AQL query can construct new ADM instances to return based on combinations of its variable bindings.
This gives AQL the power to do joins much like those done using multi-table _from_ clauses in SQL.
For example, suppose we wanted a list of all Facebook users paired with their associated messages,
with the list enumerating the author name and the message text associated with each Facebook message.
We could do this as follows in AQL:

        use dataverse TinySocial;
        
        for $user in dataset FacebookUsers
        for $message in dataset FacebookMessages
        where $message.author-id = $user.id
        return {
        "uname": $user.name,
        "message": $message.message
        };

The result of this query is a sequence of new ADM instances, one for each author/message pair.
Each instance in the result will be an ADM record containing two fields, "uname" and "message",
containing the user's name and the message text, respectively, for each author/message pair.
(Note that "uname" and "message" are both simple AQL expressions themselves---so in the most
general case, even the resulting field names can be computed as part of the query, making AQL
a very powerful tool for slicing and dicing semistructured data.)

The expected result of this example AQL join query for our sample data set is:

        { "uname": "MargaritaStoddard", "message": " dislike iphone its touch-screen is horrible" }
        { "uname": "MargaritaStoddard", "message": " can't stand at&t the network is horrible:(" }
        { "uname": "MargaritaStoddard", "message": " like verizon the 3G is awesome:)" }
        { "uname": "MargaritaStoddard", "message": " can't stand motorola the touch-screen is terrible" }
        { "uname": "MargaritaStoddard", "message": " can't stand at&t its plan is terrible" }
        { "uname": "IsbelDull", "message": " like samsung the plan is amazing" }
        { "uname": "IsbelDull", "message": " like t-mobile its platform is mind-blowing" }
        { "uname": "EmoryUnk", "message": " love sprint its shortcut-menu is awesome:)" }
        { "uname": "EmoryUnk", "message": " love verizon its wireless is good" }
        { "uname": "VonKemble", "message": " dislike sprint the speed is horrible" }
        { "uname": "WillisWynne", "message": " love sprint the customization is mind-blowing" }
        { "uname": "SuzannaTillson", "message": " like iphone the voicemail-service is awesome" }
        { "uname": "WoodrowNehling", "message": " love at&t its 3G is good:)" }
        { "uname": "BramHatch", "message": " can't stand t-mobile its voicemail-service is OMG:(" }
        { "uname": "BramHatch", "message": " dislike iphone the voice-command is bad:(" }


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
        
        for $user in dataset FacebookUsers
        for $message in dataset FacebookMessages
        where $message.author-id /*+ indexnl */  = $user.id
        return {
        "uname": $user.name,
        "message": $message.message
        };
        

The expected result is (of course) the same as before, modulo the order of the instances.
Result ordering is (intentionally) undefined in AQL in the absence of an _order by_ clause.
The query result for our sample data in this case is:

        { "uname": "EmoryUnk", "message": " love sprint its shortcut-menu is awesome:)" }
        { "uname": "MargaritaStoddard", "message": " dislike iphone its touch-screen is horrible" }
        { "uname": "IsbelDull", "message": " like samsung the plan is amazing" }
        { "uname": "MargaritaStoddard", "message": " can't stand at&t the network is horrible:(" }
        { "uname": "WillisWynne", "message": " love sprint the customization is mind-blowing" }
        { "uname": "IsbelDull", "message": " like t-mobile its platform is mind-blowing" }
        { "uname": "VonKemble", "message": " dislike sprint the speed is horrible" }
        { "uname": "MargaritaStoddard", "message": " like verizon the 3G is awesome:)" }
        { "uname": "EmoryUnk", "message": " love verizon its wireless is good" }
        { "uname": "MargaritaStoddard", "message": " can't stand motorola the touch-screen is terrible" }
        { "uname": "MargaritaStoddard", "message": " can't stand at&t its plan is terrible" }
        { "uname": "BramHatch", "message": " can't stand t-mobile its voicemail-service is OMG:(" }
        { "uname": "BramHatch", "message": " dislike iphone the voice-command is bad:(" }
        { "uname": "WoodrowNehling", "message": " love at&t its 3G is good:)" }
        { "uname": "SuzannaTillson", "message": " like iphone the voicemail-service is awesome" }


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
As an example, supposed we wanted, for each Facebook user, to produce a record that has his/her name
plus a list of the messages written by that user.
In SQL, this would involve a left outer join between users and messages, grouping by user, and having
the user name repeated along side each message.
In AQL, this sort of use case can be handled (more naturally) as follows:

        use dataverse TinySocial;
        
        for $user in dataset FacebookUsers
        return {
        "uname": $user.name,
        "messages": for $message in dataset FacebookMessages
        		where $message.author-id = $user.id
        		return $message.message
        };

This AQL query binds the variable `$user` to the data instances in FacebookUsers;
for each user, it constructs a result record containing a "uname" field with the user's
name and a "messages" field with a nested collection of all messages for that user.
The nested collection for each user is specified by using a correlated subquery.
(Note: While it looks like nested loops could be involved in computing the result,
AsterixDB recogizes the equivalence of such a query to an outerjoin, and it will
use an efficient hash-based strategy when actually computing the query's result.)

Here is this example query's expected output:

        { "uname": "MargaritaStoddard", "messages": [ " dislike iphone its touch-screen is horrible", " can't stand at&t the network is horrible:(", " like verizon the 3G is awesome:)", " can't stand motorola the touch-screen is terrible", " can't stand at&t its plan is terrible" ] }
        { "uname": "IsbelDull", "messages": [ " like samsung the plan is amazing", " like t-mobile its platform is mind-blowing" ] }
        { "uname": "EmoryUnk", "messages": [ " love sprint its shortcut-menu is awesome:)", " love verizon its wireless is good" ] }
        { "uname": "NicholasStroh", "messages": [  ] }
        { "uname": "VonKemble", "messages": [ " dislike sprint the speed is horrible" ] }
        { "uname": "WillisWynne", "messages": [ " love sprint the customization is mind-blowing" ] }
        { "uname": "SuzannaTillson", "messages": [ " like iphone the voicemail-service is awesome" ] }
        { "uname": "NilaMilliron", "messages": [  ] }
        { "uname": "WoodrowNehling", "messages": [ " love at&t its 3G is good:)" ] }
        { "uname": "BramHatch", "messages": [ " dislike iphone the voice-command is bad:(", " can't stand t-mobile its voicemail-service is OMG:(" ] }


### Query 4 - Theta Join ###
Not all joins are expressible as equijoins and computable using equijoin-oriented algorithms.
The join predicates for some use cases involve predicates with functions; AsterixDB supports the
expression of such queries and will still evaluate them as best it can using nested loop based
techniques (and broadcast joins in the parallel case).

As an example of such a use case, suppose that we wanted, for each tweet T, to find all of the
other tweets that originated from within a circle of radius of 1 surrounding tweet T's location.
In AQL, this can be specified in a manner similar to the previous query using one of the built-in
functions on the spatial data type instead of id equality in the correlated query's _where_ clause:

        use dataverse TinySocial;
        
        for $t in dataset TweetMessages
        return {
        "message": $t.message-text,
        "nearby-messages": for $t2 in dataset TweetMessages
        			where spatial-distance($t.sender-location, $t2.sender-location) <= 1
        			return { "msgtxt":$t2.message-text}
        };

Here is the expected result for this query:

        { "message": " love t-mobile its customization is good:)", "nearby-messages": [ { "msgtxt": " love t-mobile its customization is good:)" } ] }
        { "message": " hate verizon its voice-clarity is OMG:(", "nearby-messages": [ { "msgtxt": " like motorola the speed is good:)" }, { "msgtxt": " hate verizon its voice-clarity is OMG:(" } ] }
        { "message": " can't stand iphone its platform is terrible", "nearby-messages": [ { "msgtxt": " can't stand iphone its platform is terrible" } ] }
        { "message": " like samsung the voice-command is amazing:)", "nearby-messages": [ { "msgtxt": " like samsung the voice-command is amazing:)" } ] }
        { "message": " like verizon its shortcut-menu is awesome:)", "nearby-messages": [ { "msgtxt": " like verizon its shortcut-menu is awesome:)" } ] }
        { "message": " like motorola the speed is good:)", "nearby-messages": [ { "msgtxt": " hate verizon its voice-clarity is OMG:(" }, { "msgtxt": " like motorola the speed is good:)" } ] }
        { "message": " like sprint the voice-command is mind-blowing:)", "nearby-messages": [ { "msgtxt": " like sprint the voice-command is mind-blowing:)" } ] }
        { "message": " can't stand motorola its speed is terrible:(", "nearby-messages": [ { "msgtxt": " can't stand motorola its speed is terrible:(" } ] }
        { "message": " like iphone the voice-clarity is good:)", "nearby-messages": [ { "msgtxt": " like iphone the voice-clarity is good:)" } ] }
        { "message": " like samsung the platform is good", "nearby-messages": [ { "msgtxt": " like samsung the platform is good" } ] }
        { "message": " like t-mobile the shortcut-menu is awesome:)", "nearby-messages": [ { "msgtxt": " like t-mobile the shortcut-menu is awesome:)" } ] }
        { "message": " love verizon its voicemail-service is awesome", "nearby-messages": [ { "msgtxt": " love verizon its voicemail-service is awesome" } ] }


### Query 5 - Fuzzy Join ###
As another example of a non-equijoin use case, we could ask AsterixDB to find, for each Facebook user,
all Twitter users with names "similar" to their name.
AsterixDB supports a variety of "fuzzy match" functions for use with textual and set-based data.
As one example, we could choose to use edit distance with a threshold of 3 as the definition of name
similarity, in which case we could write the following query using AQL's operator-based syntax (~=)
for testing whether or not two values are similar:

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

The expected result for this query against our sample data is:

        { "id": 1, "name": "MargaritaStoddard", "similar-users": [  ] }
        { "id": 2, "name": "IsbelDull", "similar-users": [  ] }
        { "id": 3, "name": "EmoryUnk", "similar-users": [  ] }
        { "id": 4, "name": "NicholasStroh", "similar-users": [  ] }
        { "id": 5, "name": "VonKemble", "similar-users": [  ] }
        { "id": 6, "name": "WillisWynne", "similar-users": [  ] }
        { "id": 7, "name": "SuzannaTillson", "similar-users": [  ] }
        { "id": 8, "name": "NilaMilliron", "similar-users": [ { "twitter-screenname": "NilaMilliron_tw", "twitter-name": "Nila Milliron" } ] }
        { "id": 9, "name": "WoodrowNehling", "similar-users": [  ] }
        { "id": 10, "name": "BramHatch", "similar-users": [  ] }


### Query 6 - Existential Quantification ###
The expressive power of AQL includes support for queries involving "some" (existentially quantified)
and "all" (universally quantified) query semantics.
As an example of an existential AQL query, here we show a query to list the Facebook users who are currently employed.
Such employees will have an employment history containing a record with a null end-date value, which leads us to the
following AQL query:

        use dataverse TinySocial;
        
        for $fbu in dataset FacebookUsers
        where (some $e in $fbu.employment satisfies is-null($e.end-date))
        return $fbu;

The expected result in this case is:

        { "id": 1, "alias": "Margarita", "name": "MargaritaStoddard", "user-since": datetime("2012-08-20T10:10:00.000Z"), "friend-ids": {{ 2, 3, 6, 10 }}, "employment": [ { "organization-name": "Codetechno", "start-date": date("2006-08-06"), "end-date": null } ] }
        { "id": 2, "alias": "Isbel", "name": "IsbelDull", "user-since": datetime("2011-01-22T10:10:00.000Z"), "friend-ids": {{ 1, 4 }}, "employment": [ { "organization-name": "Hexviafind", "start-date": date("2010-04-27"), "end-date": null } ] }
        { "id": 4, "alias": "Nicholas", "name": "NicholasStroh", "user-since": datetime("2010-12-27T10:10:00.000Z"), "friend-ids": {{ 2 }}, "employment": [ { "organization-name": "Zamcorporation", "start-date": date("2010-06-08"), "end-date": null } ] }
        { "id": 5, "alias": "Von", "name": "VonKemble", "user-since": datetime("2010-01-05T10:10:00.000Z"), "friend-ids": {{ 3, 6, 10 }}, "employment": [ { "organization-name": "Kongreen", "start-date": date("2010-11-27"), "end-date": null } ] }
        { "id": 6, "alias": "Willis", "name": "WillisWynne", "user-since": datetime("2005-01-17T10:10:00.000Z"), "friend-ids": {{ 1, 3, 7 }}, "employment": [ { "organization-name": "jaydax", "start-date": date("2009-05-15"), "end-date": null } ] }
        { "id": 7, "alias": "Suzanna", "name": "SuzannaTillson", "user-since": datetime("2012-08-07T10:10:00.000Z"), "friend-ids": {{ 6 }}, "employment": [ { "organization-name": "Labzatron", "start-date": date("2011-04-19"), "end-date": null } ] }
        { "id": 8, "alias": "Nila", "name": "NilaMilliron", "user-since": datetime("2008-01-01T10:10:00.000Z"), "friend-ids": {{ 3 }}, "employment": [ { "organization-name": "Plexlane", "start-date": date("2010-02-28"), "end-date": null } ] }


### Query 7 - Universal Quantification ###
As an example of a universal AQL query, here we show a query to list the Facebook users who are currently unemployed.
Such employees will have an employment history containing no records with null end-date values, leading us to the
following AQL query:

        use dataverse TinySocial;
        
        for $fbu in dataset FacebookUsers
        where (every $e in $fbu.employment satisfies not(is-null($e.end-date)))
        return $fbu;

Here is the expected result for our sample data:

        { "id": 3, "alias": "Emory", "name": "EmoryUnk", "user-since": datetime("2012-07-10T10:10:00.000Z"), "friend-ids": {{ 1, 5, 8, 9 }}, "employment": [ { "organization-name": "geomedia", "start-date": date("2010-06-17"), "end-date": date("2010-01-26") } ] }
        { "id": 9, "alias": "Woodrow", "name": "WoodrowNehling", "user-since": datetime("2005-09-20T10:10:00.000Z"), "friend-ids": {{ 3, 10 }}, "employment": [ { "organization-name": "Zuncan", "start-date": date("2003-04-22"), "end-date": date("2009-12-13") } ] }
        { "id": 10, "alias": "Bram", "name": "BramHatch", "user-since": datetime("2010-10-16T10:10:00.000Z"), "friend-ids": {{ 1, 5, 9 }}, "employment": [ { "organization-name": "physcane", "start-date": date("2007-06-05"), "end-date": date("2011-11-05") } ] }


### Query 8 - Simple Aggregation ###
Like SQL, the AQL language of AsterixDB provides support for computing aggregates over large amounts of data.
As a very simple example, the following AQL query computes the total number of Facebook users:

        use dataverse TinySocial;
        
        count(for $fbu in dataset FacebookUsers return $fbu);

In AQL, aggregate functions can be applied to arbitrary subquery results; in this case, the count function
is applied to the result of a query that enumerates the Facebook users.  The expected result here is:

        10



### Query 9-A - Grouping and Aggregation ###
Also like SQL, AQL supports grouped aggregation.
For every Twitter user, the following group-by/aggregate query counts the number of tweets sent by that user:

        use dataverse TinySocial;
        
        for $t in dataset TweetMessages
        group by $uid := $t.user.screen-name with $t
        return {
        "user": $uid,
        "count": count($t)
        };

The _for_ clause incrementally binds $t to tweets, and the _group by_ clause groups the tweets by its
issuer's Twitter screen-name.
Unlike SQL, where data is tabular---flat---the data model underlying AQL allows for nesting.
Thus, following the _group by_ clause, the _return_ clause in this query sees a sequence of $t groups,
with each such group having an associated $uid variable value (i.e., the tweeting user's screen name).
In the context of the return clause, due to "... with $t ...", $uid is bound to the tweeter's id and $t
is bound to the _set_ of tweets issued by that tweeter.
The return clause constructs a result record containing the tweeter's user id and the count of the items
in the associated tweet set.
The query result will contain one such record per screen name.
This query also illustrates another feature of AQL; notice that each user's screen name is accessed via a
path syntax that traverses each tweet's nested record structure.

Here is the expected result for this query over the sample data:

        { "user": "ChangEwing_573", "count": 1 }
        { "user": "ColineGeyer@63", "count": 3 }
        { "user": "NathanGiesen@211", "count": 6 }
        { "user": "NilaMilliron_tw", "count": 1 }
        { "user": "OliJackson_512", "count": 1 }



### Query 9-B - (Hash-Based) Grouping and Aggregation ###
As for joins, AsterixDB has multiple evaluation strategies available for processing grouped aggregate queries.
For grouped aggregation, the system knows how to employ both sort-based and hash-based aggregation methods,
with sort-based methods being used by default and a hint being available to suggest that a different approach
be used in processing a particular AQL query.

The following query is similar to Query 9-A, but adds a hash-based aggregation hint:

        use dataverse TinySocial;
        
        for $t in dataset TweetMessages
        /*+ hash*/
        group by $uid := $t.user.screen-name with $t
        return {
        "user": $uid,
        "count": count($t)
        };

Here is the expected result:

        { "user": "OliJackson_512", "count": 1 }
        { "user": "ColineGeyer@63", "count": 3 }
        { "user": "NathanGiesen@211", "count": 6 }
        { "user": "NilaMilliron_tw", "count": 1 }
        { "user": "ChangEwing_573", "count": 1 }



### Query 10 - Grouping and Limits ###
In some use cases it is not necessary to compute the entire answer to a query.
In some cases, just having the first _N_ or top _N_ results is sufficient.
This is expressible in AQL using the _limit_ clause combined with the _order by_ clause.

The following AQL  query returns the top 3 Twitter users based on who has issued the most tweets:

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

The expected result for this query is:

        { "user": "NathanGiesen@211", "count": 6 }
        { "user": "ColineGeyer@63", "count": 3 }
        { "user": "NilaMilliron_tw", "count": 1 }


### Query 11 - Left Outer Fuzzy Join ###
As a last example of AQL and its query power, the following query, for each tweet,
finds all of the tweets that are similar based on the topics that they refer to:

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

This query illustrates several things worth knowing in order to write fuzzy queries in AQL.
First, as mentioned earlier, AQL offers an operator-based syntax for seeing whether two values are "similar" to one another or not.
Second, recall that the referred-topics field of records of datatype TweetMessageType is a bag of strings.
This query sets the context for its similarity join by requesting that Jaccard-based similarity semantics
([http://en.wikipedia.org/wiki/Jaccard_index](http://en.wikipedia.org/wiki/Jaccard_index))
be used for the query's similarity operator and that a similarity index of 0.3 be used as its similarity threshold.

The expected result for this fuzzy join query is:

        { "tweet": { "tweetid": "1", "user": { "screen-name": "NathanGiesen@211", "lang": "en", "friends_count": 39339, "statuses_count": 473, "name": "Nathan Giesen", "followers_count": 49416 }, "sender-location": point("47.44,80.65"), "send-time": datetime("2008-04-26T10:10:00.000Z"), "referred-topics": {{ "t-mobile", "customization" }}, "message-text": " love t-mobile its customization is good:)" }, "similar-tweets": [ {{ "t-mobile", "shortcut-menu" }} ] }
        { "tweet": { "tweetid": "10", "user": { "screen-name": "ColineGeyer@63", "lang": "en", "friends_count": 121, "statuses_count": 362, "name": "Coline Geyer", "followers_count": 17159 }, "sender-location": point("29.15,76.53"), "send-time": datetime("2008-01-26T10:10:00.000Z"), "referred-topics": {{ "verizon", "voice-clarity" }}, "message-text": " hate verizon its voice-clarity is OMG:(" }, "similar-tweets": [ {{ "iphone", "voice-clarity" }}, {{ "verizon", "voicemail-service" }}, {{ "verizon", "shortcut-menu" }} ] }
        { "tweet": { "tweetid": "11", "user": { "screen-name": "NilaMilliron_tw", "lang": "en", "friends_count": 445, "statuses_count": 164, "name": "Nila Milliron", "followers_count": 22649 }, "sender-location": point("37.59,68.42"), "send-time": datetime("2008-03-09T10:10:00.000Z"), "referred-topics": {{ "iphone", "platform" }}, "message-text": " can't stand iphone its platform is terrible" }, "similar-tweets": [ {{ "iphone", "voice-clarity" }}, {{ "samsung", "platform" }} ] }
        { "tweet": { "tweetid": "12", "user": { "screen-name": "OliJackson_512", "lang": "en", "friends_count": 445, "statuses_count": 164, "name": "Oli Jackson", "followers_count": 22649 }, "sender-location": point("24.82,94.63"), "send-time": datetime("2010-02-13T10:10:00.000Z"), "referred-topics": {{ "samsung", "voice-command" }}, "message-text": " like samsung the voice-command is amazing:)" }, "similar-tweets": [ {{ "samsung", "platform" }}, {{ "sprint", "voice-command" }} ] }
        { "tweet": { "tweetid": "2", "user": { "screen-name": "ColineGeyer@63", "lang": "en", "friends_count": 121, "statuses_count": 362, "name": "Coline Geyer", "followers_count": 17159 }, "sender-location": point("32.84,67.14"), "send-time": datetime("2010-05-13T10:10:00.000Z"), "referred-topics": {{ "verizon", "shortcut-menu" }}, "message-text": " like verizon its shortcut-menu is awesome:)" }, "similar-tweets": [ {{ "verizon", "voicemail-service" }}, {{ "verizon", "voice-clarity" }}, {{ "t-mobile", "shortcut-menu" }} ] }
        { "tweet": { "tweetid": "3", "user": { "screen-name": "NathanGiesen@211", "lang": "en", "friends_count": 39339, "statuses_count": 473, "name": "Nathan Giesen", "followers_count": 49416 }, "sender-location": point("29.72,75.8"), "send-time": datetime("2006-11-04T10:10:00.000Z"), "referred-topics": {{ "motorola", "speed" }}, "message-text": " like motorola the speed is good:)" }, "similar-tweets": [ {{ "motorola", "speed" }} ] }
        { "tweet": { "tweetid": "4", "user": { "screen-name": "NathanGiesen@211", "lang": "en", "friends_count": 39339, "statuses_count": 473, "name": "Nathan Giesen", "followers_count": 49416 }, "sender-location": point("39.28,70.48"), "send-time": datetime("2011-12-26T10:10:00.000Z"), "referred-topics": {{ "sprint", "voice-command" }}, "message-text": " like sprint the voice-command is mind-blowing:)" }, "similar-tweets": [ {{ "samsung", "voice-command" }} ] }
        { "tweet": { "tweetid": "5", "user": { "screen-name": "NathanGiesen@211", "lang": "en", "friends_count": 39339, "statuses_count": 473, "name": "Nathan Giesen", "followers_count": 49416 }, "sender-location": point("40.09,92.69"), "send-time": datetime("2006-08-04T10:10:00.000Z"), "referred-topics": {{ "motorola", "speed" }}, "message-text": " can't stand motorola its speed is terrible:(" }, "similar-tweets": [ {{ "motorola", "speed" }} ] }
        { "tweet": { "tweetid": "6", "user": { "screen-name": "ColineGeyer@63", "lang": "en", "friends_count": 121, "statuses_count": 362, "name": "Coline Geyer", "followers_count": 17159 }, "sender-location": point("47.51,83.99"), "send-time": datetime("2010-05-07T10:10:00.000Z"), "referred-topics": {{ "iphone", "voice-clarity" }}, "message-text": " like iphone the voice-clarity is good:)" }, "similar-tweets": [ {{ "verizon", "voice-clarity" }}, {{ "iphone", "platform" }} ] }
        { "tweet": { "tweetid": "7", "user": { "screen-name": "ChangEwing_573", "lang": "en", "friends_count": 182, "statuses_count": 394, "name": "Chang Ewing", "followers_count": 32136 }, "sender-location": point("36.21,72.6"), "send-time": datetime("2011-08-25T10:10:00.000Z"), "referred-topics": {{ "samsung", "platform" }}, "message-text": " like samsung the platform is good" }, "similar-tweets": [ {{ "iphone", "platform" }}, {{ "samsung", "voice-command" }} ] }
        { "tweet": { "tweetid": "8", "user": { "screen-name": "NathanGiesen@211", "lang": "en", "friends_count": 39339, "statuses_count": 473, "name": "Nathan Giesen", "followers_count": 49416 }, "sender-location": point("46.05,93.34"), "send-time": datetime("2005-10-14T10:10:00.000Z"), "referred-topics": {{ "t-mobile", "shortcut-menu" }}, "message-text": " like t-mobile the shortcut-menu is awesome:)" }, "similar-tweets": [ {{ "t-mobile", "customization" }}, {{ "verizon", "shortcut-menu" }} ] }
        { "tweet": { "tweetid": "9", "user": { "screen-name": "NathanGiesen@211", "lang": "en", "friends_count": 39339, "statuses_count": 473, "name": "Nathan Giesen", "followers_count": 49416 }, "sender-location": point("36.86,74.62"), "send-time": datetime("2012-07-21T10:10:00.000Z"), "referred-topics": {{ "verizon", "voicemail-service" }}, "message-text": " love verizon its voicemail-service is awesome" }, "similar-tweets": [ {{ "verizon", "voice-clarity" }}, {{ "verizon", "shortcut-menu" }} ] }


### Inserting New Data  ###
In addition to loading and querying data, AsterixDB supports incremental additions to datasets via the AQL _insert_ statement.

The following example adds a new tweet by user "NathanGiesen@211" to the TweetMessages dataset.
(An astute reader may notice that this tweet was issued a half an hour after his last tweet, so his counts
have all gone up in the interim, although he appears not to have moved in the last half hour.)

        use dataverse TinySocial;
        
        insert into dataset TweetMessages
        (
           {"tweetid":"13",
            "user":
                {"screen-name":"NathanGiesen@211",
                 "lang":"en",
                 "friends_count":39345,
                 "statuses_count":479,
                 "name":"Nathan Giesen",
                 "followers_count":49420
                },
            "sender-location":point("47.44,80.65"),
            "send-time":datetime("2008-04-26T10:10:35"),
            "referred-topics":{{"tweeting"}},
            "message-text":"tweety tweet, my fellow tweeters!"
           }
        );

In general, the data to be inserted may be specified using any valid AQL query expression.
The insertion of a single object instance, as in this example, is just a special case where
the query expression happens to be a record constructor involving only constants.

### Deleting Existing Data  ###
In addition to inserting new data, AsterixDB supports deletion from datasets via the AQL _delete_ statement.
The statement supports "searched delete" semantics, and its
_where_ clause can involve any valid XQuery expression.

The following example deletes the tweet that we just added from user "NathanGiesen@211".  (Easy come, easy go. :-))

        use dataverse TinySocial;
        
        delete $tm from dataset TweetMessages where $tm.tweetid = "13";

It should be noted that one form of data change not yet supported by AsterixDB is in-place data modification (_update_).
Currently, only insert and delete operations are supported; update is not.
To achieve the effect of an update, two statements are currently needed---one to delete the old record from the
dataset where it resides, and another to insert the new replacement record (with the same primary key but with
different field values for some of the associated data content).

## Further Help ##
That's it  You are now armed and dangerous with respect to semistructured data management using AsterixDB.

AsterixDB is a powerful new BDMS---Big Data Management System---that we hope may usher in a new era of much
more declarative Big Data management.
AsterixDB is powerful, so use it wisely, and remember: "With great power comes great responsibility..." :-)

Please e-mail the AsterixDB user group
(asterixdb-users (at) googlegroups.com)
if you run into any problems or simply have further questions about the AsterixDB system, its features, or their proper use.
