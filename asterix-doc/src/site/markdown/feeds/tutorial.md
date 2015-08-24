# Support for Data Ingestion in AsterixDB #

## <a id="#toc">Table of Contents</a> ##

* [Introduction](#Introduction)
* [Feed Adaptors](#FeedAdaptors)
* [Feed Policies](#FeedPolicies)

## <a name="Introduction">Introduction</a>  ##

In this document, we describe the support for data ingestion in
AsterixDB.  Data feeds are a new mechanism for having continuous data arrive into a BDMS from external sources and incrementally populate a persisted dataset and associated indexes. We add a new BDMS architectural component, called a data feed, that makes a Big Data system the caretaker for functionality that
used to live outside, and we show how it improves users' lives and system performance.

## <a name="FeedAdaptors">Feed Adaptors</a>  ##

The functionality of establishing a connection with a data source
and receiving, parsing and translating its data into ADM records
(for storage inside AsterixDB) is contained in a feed adaptor. A
feed adaptor is an implementation of an interface and its details are
specific to a given data source. An adaptor may optionally be given
parameters to configure its runtime behavior. Depending upon the
data transfer protocol/APIs offered by the data source, a feed adaptor
may operate in a push or a pull mode. Push mode involves just
one initial request by the adaptor to the data source for setting up
the connection. Once a connection is authorized, the data source
"pushes" data to the adaptor without any subsequent requests by
the adaptor. In contrast, when operating in a pull mode, the adaptor
makes a separate request each time to receive data.
AsterixDB currently provides built-in adaptors for several popular
data sources such as Twitter, CNN, and RSS feeds. AsterixDB additionally
provides a generic socket-based adaptor that can be used
to ingest data that is directed at a prescribed socket. 


In this tutorial, we shall describe building two example data ingestion pipelines that cover the popular scenario of ingesting data from (a) Twitter and (b) RSS Feed source.

####Ingesting Twitter Stream 
We shall use the built-in push-based Twitter adaptor.
As a pre-requisite, we must define a Tweet using the AsterixDB Data Model (ADM) and the AsterixDB Query Language (AQL). Given below are the type definition in AQL that create a Tweet datatype which is representative of a real tweet as obtained from Twitter.  

        create dataverse feeds;
        use dataverse feeds;

        create type TwitterUser if not exists as open{
            screen_name: string,
            language: string,
            friends_count: int32,
            status_count: int32,
            name: string,
            followers_count: string
        };
        create type Tweet if not exists as open{
            id: string,
            user: TwitterUser,
            latitude:double,
            longitude:double,
            created_at:string,
            message_text:string
        };

	    create dataset Tweets (Tweet)
        primary key id;

We also create a dataset that we shall use to persist the tweets in AsterixDB. 
Next we make use of the `create feed` AQL statement to define our example data feed. 

#####Using the "push_twitter" feed adapter#####
The "push_twitter" adaptor requires setting up an application account with Twitter. To retrieve
tweets, Twitter requires registering an application with Twitter. Registration involves providing a name and a brief description for the application. Each application has an associated OAuth authentication credential that includes OAuth keys and tokens. Accessing the 
Twitter API requires providing the following.
1. Consumer Key (API Key)
2. Consumer Secret (API Secret)
3. Access Token
4. Access Token Secret

The "push_twitter" adaptor takes as configuration the above mentioned
parameters. End users are required to obtain the above authentication credentials prior to using the "push_twitter" adaptor. For further information on obtaining OAuth keys and tokens and registering an application with Twitter, please visit http://apps.twitter.com 

Given below is an example AQL statement that creates a feed called "TwitterFeed" by using the 
"push_twitter" adaptor. 

        use dataverse feeds;

        create feed TwitterFeed if not exists using "push_twitter"
        (("type-name"="Tweet"),
         ("consumer.key"="************"),  
         ("consumer.secret"="**************"),
         ("access.token"="**********"),  
         ("access.token.secret"="*************"));

It is required that the above authentication parameters are provided valid values. 
Note that the `create feed` statement does not initiate the flow of data from Twitter into our AsterixDB instance. Instead, the `create feed` statement only results in registering the feed with AsterixDB. The flow of data along a feed is initiated when it is connected
to a target dataset using the connect feed statement (which we shall revisit later).


####Lifecycle of a Feed####

A feed is a logical artifact that is brought to life (i.e., its data flow
is initiated) only when it is connected to a dataset using the `connect
feed` AQL statement. Subsequent to a `connect feed`
statement, the feed is said to be in the connected state. Multiple
feeds can simultaneously be connected to a dataset such that the
contents of the dataset represent the union of the connected feeds.
In a supported but unlikely scenario, one feed may also be simultaneously
connected to different target datasets. Note that connecting
a secondary feed does not require the parent feed (or any ancestor
feed) to be in the connected state; the order in which feeds are connected
to their respective datasets is not important. Furthermore,
additional (secondary) feeds can be added to an existing hierarchy
and connected to a dataset at any time without impeding/interrupting
the flow of data along a connected ancestor feed.

        use dataverse feeds;

        connect feed TwitterFeed to dataset Tweets;

The `connect feed` statement above directs AsterixDB to persist
the `TwitterFeed` feed in the `Tweets` dataset.
If it is required (by the high-level application) to also retain the raw
tweets obtained from Twitter, the end user may additionally choose
to connect TwitterFeed to a different dataset. 

Let the feed run for a minute, then run the following query to see the
latest tweets that are stored into the data set.

        use dataverse feeds;

        for $i in dataset Tweets limit 10 return $i;


The flow of data from a feed into a dataset can be terminated
explicitly by use of the `disconnect feed` statement.
Disconnecting a feed from a particular dataset does not interrupt
the flow of data from the feed to any other dataset(s), nor does it
impact other connected feeds in the lineage.

        use dataverse feeds;

        disconnect feed TwitterFeed from dataset Tweets;

####Ingesting an RSS Feed

RSS (Rich Site Summary), originally RDF Site Summary and often called Really Simple Syndication, uses a family of standard web feed formats to publish frequently updated information: blog entries, news headlines, audio, video. An RSS document (called "feed", "web feed", or "channel") includes full or summarized text, and metadata, like publishing date and author's name. RSS feeds enable publishers to syndicate data automatically. 


#####Using the "rss_feed" feed adapter#####
AsterixDB provides a built-in feed adaptor that allows retrieving data given a collection of RSS end point URLs. As observed in the case of ingesting tweets, it is required to model an RSS data item using AQL.  

        use dataverse feeds;

        create type Rss if not exists as open {
        	id: string,
        	title: string,
        	description: string,
        	link: string
        };

        create dataset RssDataset (Rss)
		primary key id; 

Next, we define an RSS feed using our built-in adaptor "rss_feed". 

        use dataverse feeds;

        create feed my_feed using 
	    rss_feed (
	       ("type-name"="Rss"),
	       ("url"="http://rss.cnn.com/rss/edition.rss")
		);

In the above definition, the configuration parameter "url" can be a comma-separated list that reflects a collection of RSS URLs, where each URL corresponds to an RSS endpoint or a RSS feed. 
The "rss_adaptor" retrieves data from each of the specified RSS URLs (comma separated values) in parallel. 

The following statements connect the feed into the `RssDataset`:

        use dataverse feeds;

        connect feed my_feed to dataset RssDataset;

The following statements show the latest data from the data set, and
disconnect the feed from the data set.

        use dataverse feeds;

        for $i in dataset RssDataset limit 10 return $i;

        disconnect feed my_feed from dataset RssDataset;

AsterixDB also allows multiple feeds to be connected to form a cascade
network to process data.

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
a list of policy parameters that help customize the
system's runtime behavior when handling excess records. AsterixDB
provides a set of built-in policies, each constructed by setting
appropriate value(s) for the policy parameter(s) from the table below.

####Policy Parameters 

- *excess.records.spill*: Set to true if records that cannot be processed by an operator for lack of resources (referred to as excess records hereafter) should be persisted to the local disk for deferred processing. (Default: false)

- *excess.records.discard*: Set to true if excess records should be discarded. (Default: false) 

- *excess.records.throttle*: Set to true if rate of arrival of records is required to be reduced in an adaptive manner to prevent having any excess records (Default: false) 

- *excess.records.elastic*: Set to true if the system should attempt to resolve resource bottlenecks by re-structuring and/or rescheduling the feed ingestion pipeline. (Default: false) 

- *recover.soft.failure*:  Set to true if the feed must attempt to survive any runtime exception. A false value permits an early termination of a feed in such an event. (Default: true) 

- *recover.soft.failure*:  Set to true if the feed must attempt to survive a hardware failures (loss of AsterixDB node(s)). A false value permits the early termination of a feed in the event of a hardware failure (Default: false) 

Note that the end user may choose to form a custom policy.  For example,
it is possible in AsterixDB to create a custom policy that spills excess
records to disk and subsequently resorts to throttling if the
spillage crosses a configured threshold. In all cases, the desired
ingestion policy is specified as part of the `connect feed` statement
or else the "Basic" policy will be chosen as the default.
It is worth noting that a feed can be connected to a dataset at any
time, which is independent from other related feeds in the hierarchy.

        use dataverse feeds;

        connect feed TwitterFeed to dataset Tweets
        using policy Basic ;


