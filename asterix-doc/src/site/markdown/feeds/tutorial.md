# Support for Data Ingestion in AsterixDB #

## <a id="#toc">Table of Contents</a> ##

* [Introduction](#Introduction)
  * [Data Feed Basics](#DataFeedBasics)
    * [Collecting Data: Feed Adaptors](#FeedAdaptors)
    * [Preprocessing Collected Data](#PreprocessingCollectedData)
  * [Creating an External UDF](#CreatingAnExternalUDF)
  * [Installing an AsterixDB Library](#InstallingAnAsterixDBLibrary)


## <a name="Introduction">Introduction</a>  ##

In this document, we describe the support for data ingestion in AsterixDB, an open-source Big Data Management System (BDMS) that provides a platform for storage and analysis of large volumes of semi-structured data. Data feeds are a new mechanism for having continuous data arrive into a BDMS from external sources and incrementally populate a persisted dataset and associated indexes. We add a new BDMS architectural component, called a data feed, that makes a Big Data system the caretaker for functionality that
used to live outside, and we show how it improves users’ lives and system performance.

## <a name="DataFeedBasics">Data Feed Basics</a>  ##

####Collecting Data: Feed Adaptors####
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
“pushes” data to the adaptor without any subsequent requests by
the adaptor. In contrast, when operating in a pull mode, the adaptor
makes a separate request each time to receive data.
AsterixDB currently provides built-in adaptors for several popular
data sources—Twitter, CNN, and RSS feeds. AsterixDB additionally
provides a generic socket-based adaptor that can be used
to ingest data that is directed at a prescribed socket.


In this tutorial, we shall describe building two example data ingestion pipelines that cover the popular scenario of ingesting data from (a) Twitter and (b) RSS Feed source.

####Ingesting Twitter Stream
We shall use the built-in push-based Twitter adaptor.
As a pre-requisite, we must define a Tweet using the AsterixDB Data Model (ADM) and the AsterixDB Query Language (AQL). Given below are the type definition in AQL that create a Tweet datatype which is representative of a real tweet as obtained from Twitter.

        create dataverse feeds;
        use dataverse feeds;

        create type TwitterUser  if not exists as open{
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
Next we make use of the create feed AQL statement to define our example data feed.

#####Using the "push_twitter" feed adapter#####
The push_twitter adaptor requires setting up an application account with Twitter. To retrieve
tweets, Twitter requires registering an application with Twitter. Registration involves providing a name and a brief description for the application. Each application has an associated OAuth authentication credential that includes OAuth keys and tokens. Accessing the
Twitter API requires providing the following.
1. Consumer Key (API Key)
2. Consumer Secret (API Secret)
3. Access Token
4. Access Token Secret


The "push_twitter" adaptor takes as configuration the above mentioned parameters. End-user(s) are required to obtain the above authentication credentials prior to using the "push_twitter" adaptor. For further information on obtaining OAuth keys and tokens and registering an application with Twitter, please visit http://apps.twitter.com

Given below is an example AQL statement that creates a feed - TwitterFeed by using the
"push_twitter" adaptor.



        create feed TwitterFeed if not exists using "push_twitter"
        (("type-name"="Tweet"),
         ("consumer.key"="************"),
         ("consumer.secret"="**************"),
         ("access.token"="**********"),
         ("access.token.secret"="*************"));

It is required that the above authentication parameters are provided valid values.
Note that the create feed statement does not initiate the flow of data from Twitter into our AsterixDB instance. Instead, the create feed statement only results in registering the feed with AsterixDB. The flow of data along a feed is initiated when it is connected
to a target dataset using the connect feed statement (which we shall revisit later).


####Ingesting an RSS Feed
RSS (Rich Site Summary); originally RDF Site Summary; often called Really Simple Syndication, uses a family of standard web feed formats to publish frequently updated information: blog entries, news headlines, audio, video. An RSS document (called "feed", "web feed", or "channel") includes full or summarized text, and metadata, like publishing date and author's name. RSS feeds enable publishers to syndicate data automatically.


#####Using the "rss_feed" feed adapter#####
AsterixDB provides a built-in feed adaptor that allows retrieving data given a collection of RSS end point URLs. As observed in the case of ingesting tweets, it is required to model an RSS data item using AQL.

        create dataverse feeds if not exists;
        use dataverse feeds;

        create type Rss  if not exists as open{
            id: string,
            title: string,
            description: string,
            link: string
        };

        create dataset RssDataset (Rss)
        primary key id;


Next, we define an RSS feed using our built-in adaptor "rss_feed".

        create feed my_feed using
        rss_feed (
           ("type-name"="Rss"),
           ("url"="http://rss.cnn.com/rss/edition.rss")
        );

In the above definition, the configuration parameter "url" can be a comma separated list that reflects a collection of RSS URLs, where each URL corresponds to an RSS endpoint or a RSS feed.
The "rss_adaptor" retrieves data from each of the specified RSS URLs (comma separated values) in parallel.


So far, we have discussed the mechanism for retrieving data from the external world into the AsterixDB system. However, the arriving data may require certain pre-processing prior to being persisted in AsterixDB storage. Next, we discuss how the arriving data can be pre-processed.



## <a id="PreprocessingCollectedData">Preprocessing Collected Data</a> ###
A feed definition may optionally include the specification of a
user-defined function that is to be applied to each feed record prior
to persistence. Examples of pre-processing might include adding
attributes, filtering out records, sampling, sentiment analysis, feature
extraction, etc. The pre-processing is expressed as a userdefined
function (UDF) that can be defined in AQL or in a programming
language like Java. An AQL UDF is a good fit when
pre-processing a record requires the result of a query (join or aggregate)
over data contained in AsterixDB datasets. More sophisticated
processing such as sentiment analysis of text is better handled
by providing a Java UDF. A Java UDF has an initialization phase
that allows the UDF to access any resources it may need to initialize
itself prior to being used in a data flow. It is assumed by the
AsterixDB compiler to be stateless and thus usable as an embarassingly
parallel black box. In constrast, the AsterixDB compiler can
reason about an AQL UDF and involve the use of indexes during
its invocation.

We consider an example transformation of a raw tweet into its lightweight version - ProcessedTweet - which is defined next.

        create type ProcessedTweet if not exists as open {
            id: string,
            user_name:string,
            location:point,
            created_at:string,
            message_text:string,
            country: string,
            topics: [string]
        };


The processing required in transforming a collected tweet to its lighter version (of type ProcessedTweet) involves extracting the topics or hash-tags (if any) in a tweet
and collecting them in the referred-topics attribute for the tweet.
Additionally, the latitude and longitude values (doubles) are combined into the spatial point type. Note that spatial data types are considered as first class citizens that come with the support for creating indexes. Next we show a revised version of our example TwitterFeed that involves the use of a UDF. We assume that the UDF that contains the transformation logic into a ProcessedTweet is avaialable as a Java UDF inside an AsterixDB library named 'testlib'. We defer the writing of a Java UDF and its installation as part of an AsterixDB library to a later section of this document.

        create feed ProcessedTwitterFeed if not exists
        using "push_twitter"
        (("type-name"="Tweet"));
        apply function testlib#processRawTweet;

Note that a feed adaptor and a UDF act as pluggable components. These
contribute towards providing a generic ‘plug-and-play‘ model where
custom implementations can be provided to cater to specific requirements.

####Building a Cascade Network of Feeds####
Multiple high-level applications may wish to consume the data
ingested from a data feed. Each such application might perceive the
feed in a different way and require the arriving data to be processed
and/or persisted differently. Building a separate flow of data from
the external source for each application is wasteful of resources as
the pre-processing or transformations required by each application
might overlap and could be done together in an incremental fashion
to avoid redundancy. A single flow of data from the external source
could provide data for multiple applications. To achieve this, we
introduce the notion of primary and secondary feeds in AsterixDB.

A feed in AsterixDB is considered to be a primary feed if it gets
its data from an external data source. The records contained in a
feed (subsequent to any pre-processing) are directed to a designated
AsterixDB dataset. Alternatively or additionally, these records can
be used to derive other feeds known as secondary feeds. A secondary
feed is similar to its parent feed in every other aspect; it can
have an associated UDF to allow for any subsequent processing,
can be persisted into a dataset, and/or can be made to derive other
secondary feeds to form a cascade network. A primary feed and a
dependent secondary feed form a hierarchy. As an example, we next show an
example AQL statement that redefines the previous feed—
ProcessedTwitterFeed in terms of their
respective parent feed (TwitterFeed).

        create secondary feed ProcessedTwitterFeed from feed TwitterFeed
        apply function testlib#addFeatures;


####Lifecycle of a Feed####
A feed is a logical artifact that is brought to life (i.e. its data flow
is initiated) only when it is connected to a dataset using the connect
feed AQL statement (Figure 7). Subsequent to a connect feed
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

        connect feed ProcessedTwitterFeed to
        dataset ProcessedTweets ;

        disconnect feed ProcessedTwitterFeed from
        dataset ProcessedTweets ;

The connect feed statement above directs AsterixDB to persist
the ProcessedTwitterFeed feed in the ProcessedTweets dataset.
If it is required (by the high-level application) to also retain the raw
tweets obtained from Twitter, the end user may additionally choose
to connect TwitterFeed to a (different) dataset. Having a set of primary
and secondary feeds offers the flexibility to do so. Let us
assume that the application needs to persist TwitterFeed and that,
to do so, the end user makes another use of the connect feed statement.
A logical view of the continuous flow of data established by
connecting the feeds to their respective target datasets is shown in
Figure 8.

The flow of data from a feed into a dataset can be terminated
explicitly by use of the disconnect feed statement.
Disconnecting a feed from a particular dataset does not interrupt
the flow of data from the feed to any other dataset(s), nor does it
impact other connected feeds in the lineage.

####Policies for Feed Ingestion####
Multiple feeds may be concurrently operational on an AsterixDB
cluster, each competing for resources (CPU cycles, network bandwidth,
disk IO) to maintain pace with their respective data sources.
A data management system must be able to manage a set of concurrent
feeds and make dynamic decisions related to the allocation of
resources, resolving resource bottlenecks and the handling of failures.
Each feed has its own set of constraints, influenced largely
by the nature of its data source and the application(s) that intend
to consume and process the ingested data. Consider an application
that intends to discover the trending topics on Twitter by analyzing
the ProcessedTwitterFeed feed. Losing a few tweets may be
acceptable. In contrast, when ingesting from a data source that
provides a click-stream of ad clicks, losing data would translate to
a loss of revenue for an application that tracks revenue by charging
advertisers per click.

AsterixDB allows a data feed to have an associated ingestion
policy that is expressed as a collection of parameters and associated
values. An ingestion policy dictates the runtime behavior of
the feed in response to resource bottlenecks and failures. AsterixDB provides
a list of policy parameters (Table 1) that help customize the
system’s runtime behavior when handling excess records. AsterixDB
provides a set of built-in policies, each constructed by setting
appropriate value(s) for the policy parameter(s) from the table below.



####Policy Parameters


- *excess.records.spill*: Set to true if records that cannot be processed by an operator for lack of resources (referred to as excess records hereafter) should be persisted to the local disk for deferred processing. (Default: false)

- *excess.records.discard*: Set to true if excess records should be discarded. (Default: false)

- *excess.records.throttle*: Set to true if rate of arrival of records is required to be reduced in an adaptive manner to prevent having any excess records (Default: false)

- *excess.records.elastic*: Set to true if the system should attempt to resolve resource bottlenecks by re-structuring and/or rescheduling the feed ingestion pipeline. (Default: false)

- *recover.soft.failure*:  Set to true if the feed must attempt to survive any runtime exception. A false value permits an early termination of a feed in such an event. (Default: true)

- *recover.soft.failure*:  Set to true if the feed must attempt to survive a hardware failures (loss of AsterixDB node(s)). A false value permits the early termination of a feed in the event of a hardware failure (Default: false)

Note that the end user may choose to form a custom policy. E.g.
it is possible in AsterixDB to create a custom policy that spills excess
records to disk and subsequently resorts to throttling if the
spillage crosses a configured threshold. In all cases, the desired
ingestion policy is specified as part of the connect feed statement
(Figure 9) or else the ‘Basic’ policy will be chosen as the default.
It is worth noting that a feed can be connected to a dataset at any
time, which is independent from other related feeds in the hierarchy.


        connect feed TwitterFeed to dataset Tweets
        using policy Basic ;


## <a id="WritingAnExternalUDF">Writing an External UDF</a> ###

A Java UDF in AsterixDB is required to implement an prescribe interface. We shall next write a basic UDF that extracts the hashtags contained in the tweet's text and appends each into an unordered list. The list is added as an additional attribute to the tweet to form the augment version - ProcessedTweet.

    package edu.uci.ics.asterix.external.library;

    import edu.uci.ics.asterix.external.library.java.JObjects.JRecord;
    import edu.uci.ics.asterix.external.library.java.JObjects.JString;
    import edu.uci.ics.asterix.external.library.java.JObjects.JUnorderedList;
    import edu.uci.ics.asterix.external.library.java.JTypeTag;

    public class HashTagsFunction implements IExternalScalarFunction {

    private JUnorderedList list = null;

    @Override
    public void initialize(IFunctionHelper functionHelper) {
        list = new JUnorderedList(functionHelper.getObject(JTypeTag.STRING));
    }

    @Override
    public void deinitialize() {
    }

    @Override
    public void evaluate(IFunctionHelper functionHelper) throws Exception {
        list.clear();
        JRecord inputRecord = (JRecord) functionHelper.getArgument(0);
        JString text = (JString) inputRecord.getValueByName("message_text");

        // extraction of hashtags
        String[] tokens = text.getValue().split(" ");
        for (String tk : tokens) {
            if (tk.startsWith("#")) {
                JString newField = (JString) functionHelper.getObject(JTypeTag.STRING);
                newField.setValue(tk);
                list.add(newField);
            }
        }

        // forming the return value - an augmented tweet with an additional attribute - topics
        JRecord result = (JRecord) functionHelper.getResultObject();
        result.setField("tweetid", inputRecord.getFields()[0]);
        result.setField("user", inputRecord.getFields()[1]);
        result.setField("location_lat", inputRecord.getFields()[2]);
        result.setField("location_long", inputRecord.getFields()[3]);
        result.setField("send_time", inputRecord.getFields()[4]);
        result.setField("message_text", inputRecord.getFields()[5]);
        result.setField("topics", list);

        functionHelper.setResult(result);
    }


## <a id="CreatingAnAsterixDBLibrary">Creating an AsterixDB Library</a> ###

We need to install our Java UDF so that we may use it in AQL statements/queries. An AsterixDB library has a pre-defined structure which is as follows.


- jar file: A jar file that would contain the class files for your UDF source code.
- library descriptor.xml:  This is a descriptor that provide meta-information about the library.

        <externalLibrary xmlns="library">
            <language>JAVA</language>
            <libraryFunctions>
                <libraryFunction>
                    <function_type>SCALAR</function_type>
                    <name>addFeatures</name>
                    <arguments>Tweet</arguments>
                    <return_type>ProcessedTweet</return_type>
                    <definition>edu.uci.ics.asterix.external.library.AddHashTagsFactory
                    </definition>
                </libraryFunction>
            </libraryFunctions>
        </externalLibrary>


- lib: other dependency jars

If the Java UDF requires additional dependency jars, you may add them under a "lib" folder is required.

We create a zip bundle that contains the jar file and the library descriptor xml file. The zip would have the following structure.

    $ unzip -l ./tweetlib.zip
    Archive:  ./tweetlib.zip
      Length     Date   Time    Name
     --------    ----   ----    ----
       760817  04-23-14 17:16   hash-tags.jar
          405  04-23-14 17:16   tweet.xml
     --------                   -------
       761222                   2 files


###Installing an AsterixDB Library###

We assume you have followed the [http://asterixdb.ics.uci.edu/documentation/install.html instructions] to set up a running AsterixDB instance. Let us refer your AsterixDB instance by the name "my_asterix".


- Step 1: Stop the AsterixDB instance if it is in the ACTIVE state.

        $ managix stop -n my_asterix


- Step 2: Install the library using Managix install command. Just to illustrate, we use the help command to look up the syntax

        $ managix help  -cmd install
        Installs a library to an asterix instance.
        Options
        n  Name of Asterix Instance
        d  Name of the dataverse under which the library will be installed
        l  Name of the library
        p  Path to library zip bundle


Above is a sample output and explains the usage and the required parameters. Each library has a name and is installed under a dataverse. Recall that we had created a dataverse by the name - "feeds" prior to  creating our datatypes and dataset. We shall name our library - "testlib".

We assume you have a library zip bundle that needs to be installed.
To install the library, use the Managix install command. An example is shown below.

    $ managix install -n my_asterix -d feeds -l testlib -p <put the absolute path of the library zip bundle here>

You should see the following message:

    INFO: Installed library testlib

We shall next start our AsterixDB instance using the start command as shown below.

    $ managix start -n my_asterix

You may now use the AsterixDB library in AQL statements and queries. To look at the installed artifacts, you may execute the following query at the AsterixDB web-console.

    for $x in dataset Metadata.Function
    return $x

    for $x in dataset Metadata.Library
    return $x

Our library is now installed and is ready to be used.
