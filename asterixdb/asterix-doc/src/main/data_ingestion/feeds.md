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

## <a name="Introduction">Introduction</a>  ##

In this document, we describe the support for data ingestion in
AsterixDB. Data feeds are a new mechanism for having continuous
data arrive into a BDMS from external sources and incrementally
populate a persisted dataset and associated indexes. We add a new BDMS
architectural component, called a data feed, that makes a Big Data system the caretaker for functionality that
used to live outside, and we show how it improves users' lives and system performance.

## <a name="FeedAdapters">Feed Adapters</a>  ##

The functionality of establishing a connection with a data source
and receiving, parsing and translating its data into ADM objects
(for storage inside AsterixDB) is contained in a feed adapter. A
feed adapter is an implementation of an interface and its details are
specific to a given data source. An adapter may optionally be given
parameters to configure its runtime behavior. Depending upon the
data transfer protocol/APIs offered by the data source, a feed adapter
may operate in a push or a pull mode. Push mode involves just
one initial request by the adapter to the data source for setting up
the connection. Once a connection is authorized, the data source
"pushes" data to the adapter without any subsequent requests by
the adapter. In contrast, when operating in a pull mode, the adapter
makes a separate request each time to receive data.
AsterixDB currently provides built-in adapters for several popular
data sources such as Twitter and RSS feeds. AsterixDB additionally
provides a generic socket-based adapter that can be used
to ingest data that is directed at a prescribed socket.


In this tutorial, we shall describe building two example data ingestion pipelines
that cover the popular scenarios of ingesting data from (a) Twitter (b) RSS  (c) Socket Feed source.

####Ingesting Twitter Stream
We shall use the built-in push-based Twitter adapter.
As a pre-requisite, we must define a Tweet using the AsterixDB Data Model (ADM)
and the query language SQL++. Given below are the type definitions in SQL++
that create a Tweet datatype which is representative of a real tweet as obtained from Twitter.

        drop dataverse feeds if exists;

        create dataverse feeds;
        use feeds;

        create type TwitterUser as closed {
            screen_name: string,
            lang: string,
            friends_count: int32,
            statuses_count: int32
        };

        create type Tweet as open {
            id: int64,
            user: TwitterUser
        };

        create dataset Tweets (Tweet) primary key id;

We also create a dataset that we shall use to persist the tweets in AsterixDB.
Next we make use of the `create feed` SQL++ statement to define our example data feed.

#####Using the "push_twitter" feed adapter#####
The "push_twitter" adapter requires setting up an application account with Twitter. To retrieve
tweets, Twitter requires registering an application. Registration involves providing
a name and a brief description for the application. Each application has associated OAuth
authentication credentials that include OAuth keys and tokens. Accessing the
Twitter API requires providing the following.

1. Consumer Key (API Key)
2. Consumer Secret (API Secret)
3. Access Token
4. Access Token Secret

The "push_twitter" adapter takes as configuration the above mentioned
parameters. End users are required to obtain the above authentication credentials prior to
using the "push_twitter" adapter. For further information on obtaining OAuth keys and tokens and
registering an application with Twitter, please visit http://apps.twitter.com.

Note that AsterixDB uses the Twitter4J API for getting data from Twitter. Due to a license conflict,
Apache AsterixDB cannot ship the Twitter4J library. To use the Twitter adapter in AsterixDB,
please download the necessary dependencies (`twitter4j-core-4.0.x.jar` and `twitter4j-stream-4.0.x.jar`) and drop
them into the `repo/` directory before AsterixDB starts.

Given below is an example SQL++ statement that creates a feed called "TwitterFeed" by using the
"push_twitter" adapter.

        use feeds;

        create feed TwitterFeed with {
          "adapter-name": "push_twitter",
          "type-name": "Tweet",
          "format": "twitter-status",
          "consumer.key": "************",
          "consumer.secret": "************",
          "access.token": "**********",
          "access.token.secret": "*************"
        };

It is required that the above authentication parameters are provided valid.
Note that the `create feed` statement does not initiate the flow of data from Twitter into
the AsterixDB instance. Instead, the `create feed` statement only results in registering
the feed with the instance. The flow of data along a feed is initiated when it is connected
to a target dataset using the connect feed statement and activated using the start feed statement.

The Twitter adapter also supports several Twitter streaming APIs as follow:

1. Track filter `"keywords": "AsterixDB, Apache"`
2. Locations filter `"locations": "-29.7, 79.2, 36.7, 72.0; -124.848974,-66.885444, 24.396308, 49.384358"`
3. Language filter `"language": "en"`
4. Filter level `"filter-level": "low"`

An example of Twitter adapter tracking tweets with keyword "news" can be described using following ddl:

        use feeds;

        create feed TwitterFeed with {
          "adapter-name": "push_twitter",
          "type-name": "Tweet",
          "format": "twitter-status",
          "consumer.key": "************",
          "consumer.secret": "************",
          "access.token": "**********",
          "access.token.secret": "*************",
          "keywords": "news"
        };

For more details about these APIs, please visit https://dev.twitter.com/streaming/overview/request-parameters

####Lifecycle of a Feed####

A feed is a logical artifact that is brought to life (i.e., its data flow
is initiated) only when it is activated using the `start feed` statement.
Before we active a feed, we need to designate the dataset where the data to be persisted
using `connect feed` statement.
Subsequent to a `connect feed` statement, the feed is said to be in the connected state.
After that, `start feed` statement will activate the feed, and start the dataflow from feed to its connected dataset.
Multiple feeds can simultaneously be connected to a dataset such that the
contents of the dataset represent the union of the connected feeds.
Also one feed can be simultaneously connected to multiple target datasets.

        use feeds;

        connect feed TwitterFeed to dataset Tweets;

        start feed TwitterFeed;

The `connect feed` statement above directs AsterixDB to persist
the data from `TwitterFeed` feed into the `Tweets` dataset. The `start feed` statement will activate the feed and
start the dataflow.
If it is required (by the high-level application) to also retain the raw
tweets obtained from Twitter, the end user may additionally choose
to connect TwitterFeed to a different dataset.

Let the feed run for a minute, then run the following query to see the
latest tweets that are stored into the data set.

        use feeds;

        select * from Tweets limit 10;

The dataflow of data from a feed can be terminated explicitly by `stop feed` statement.

        use feeds;

        stop feed TwitterFeed;

The `disconnnect statement` can be used to disconnect the feed from certain dataset.

        use feeds;

        disconnect feed TwitterFeed from dataset Tweets;

###Ingesting with Other Adapters
AsterixDB has several builtin feed adapters for data ingestion. User can also
implement their own adapters and plug them into AsterixDB.
Here we introduce `socket_adapter` and `localfs`
feed adapter that cover most of the common application scenarios.

#####Using the "socket_adapter" feed adapter#####
`socket_adapter` feed opens a web socket on the given node which allows user to push data into
AsterixDB directly. Here is an example:

        drop dataverse feeds if exists;
        create dataverse feeds;
        use feeds;

        create type TestDataType as open {
           screenName: string
        };

        create dataset TestDataset(TestDataType) primary key screenName;

        create feed TestSocketFeed with {
          "adapter-name": "socket_adapter",
          "sockets": "127.0.0.1:10001",
          "address-type": "IP",
          "type-name": "TestDataType",
          "format": "adm"
        };

        connect feed TestSocketFeed to dataset TestDataset;

        use feeds;
        start feed TestSocketFeed;

The above statements create a socket feed which is listening to "10001" port of the host machine. This feed accepts data
records in "adm" format. As an example, you can download the sample dataset [Chirp Users](../data/chu.adm) and push them line
by line into the socket feed using any socket client you like. Following is a socket client example in Python:

        from socket import socket

        ip = '127.0.0.1'
        port1 = 10001
        filePath = 'chu.adm'

        sock1 = socket()
        sock1.connect((ip, port1))

        with open(filePath) as inputData:
            for line in inputData:
                sock1.sendall(line)
            sock1.close()


####Using the "localfs" feed adapter####
`localfs` adapter enables data ingestion from local file system. It allows user to feed data records on local disk
into a dataset. A DDL example for creating a `localfs` feed is given as follow:

        use feeds;

        create type TestDataType as open {
           screenName: string
        };

        create dataset TestDataset(TestDataType) primary key screenName;

        create feed TestFileFeed with {
          "adapter-name": "localfs",
          "type-name": "TestDataType",
          "path": "HOSTNAME://LOCAL_FILE_PATH",
          "format": "adm"
        };

        connect feed TestFileFeed to dataset TestDataset;

        start feed TestFileFeed;

Similar to previous examples, we need to define the datatype and dataset this feed uses.
The "path" parameter refers to the local data file that we want to ingest data from.
`HOSTNAME` can either be the IP address or node name of the machine which holds the file.
`LOCAL_FILE_PATH` indicates the absolute path to the file on that machine. Similarly to `socket_adapter`,
this feed takes `adm` formatted data records.

### Datatype for feed and target dataset

The "type-name" parameter in create feed statement defines the `datatype` of the datasource. In most use cases,
feed will have the same `datatype` as the target dataset. However, if we want to perform certain preprocess before the
data records gets into the target dataset (append autogenerated key, apply user defined functions, etc.), we will
need to define the datatypes for feed and dataset separately.

#### Ingestion with autogenerated key

AsterixDB supports using autogenerated uuid as the primary key for dataset. When we use this feature, we will need to
define a datatype with the primary key field, and specify that field to be autogenerated when creating the dataset.
Use that same datatype in feed definition will cause a type discrepancy since there is no such field in the datasource.
Thus, we will need to define two separate datatypes for feed and dataset:

        use feeds;

        create type DBLPFeedType as closed {
          dblpid: string,
          title: string,
          authors: string,
          misc: string
        }

        create type DBLPDataSetType as open {
          id: uuid,
          dblpid: string,
          title: string,
          authors: string,
          misc: string
        }
        create dataset DBLPDataset(DBLPDataSetType) primary key id autogenerated;

        create feed DBLPFeed with {
          "adapter-name": "socket_adapter",
          "sockets": "127.0.0.1:10001",
          "address-type": "IP",
          "type-name": "DBLPFeedType",
          "format": "adm"
        };

        connect feed DBLPFeed to dataset DBLPDataset;

        start feed DBLPFeed;

## <a name="FeedPolicies">Policies for Feed Ingestion</a>  ##

Multiple feeds may be concurrently operational on an AsterixDB
cluster, each competing for resources (CPU cycles, network bandwidth,
disk IO) to maintain pace with their respective data sources.
As a data management system, AsterixDB is able to manage a set of concurrent
feeds and make dynamic decisions related to the allocation of
resources, resolving resource bottlenecks and the handling of failures.
Each feed has its own set of constraints, influenced largely
by the nature of its data source and the applications that intend
to consume and process the ingested data. Consider an application
that intends to discover the trending topics on Twitter by analyzing
tweets that are being processed. Losing a few tweets may be
acceptable. In contrast, when ingesting from a data source that
provides a click-stream of ad clicks, losing data would translate to
a loss of revenue for an application that tracks revenue by charging
advertisers per click.

AsterixDB allows a data feed to have an associated ingestion
policy that is expressed as a collection of parameters and associated
values. An ingestion policy dictates the runtime behavior of
the feed in response to resource bottlenecks and failures. AsterixDB provides
a set of policies that help customize the
system's runtime behavior when handling excess objects.

####Policies

- *Spill*: Objects that cannot be processed by an operator for lack of resources
(referred to as excess objects hereafter) should be persisted to the local disk for deferred processing.

- *Discard*: Excess objects should be discarded.

Note that the end user may choose to form a custom policy.  For example,
it is possible in AsterixDB to create a custom policy that spills excess
objects to disk and subsequently resorts to throttling if the
spillage crosses a configured threshold. In all cases, the desired
ingestion policy is specified as part of the `connect feed` statement
or else the "Basic" policy will be chosen as the default.

        use feeds;

        connect feed TwitterFeed to dataset Tweets using policy Basic;