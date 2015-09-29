# Support for User Defined Functions in AsterixDB #

## <a id="#toc">Table of Contents</a> ##
* [Using UDF to preprocess feed-collected data](#PreprocessingCollectedData)
* [Writing an External UDF](#WritingAnExternalUDF)
* [Creating an AsterixDB Library](#CreatingAnAsterixDBLibrary)
* [Installing an AsterixDB Library](#installingUDF)

In this document, we describe the support for implementing, using, and installing user-defined functions (UDF) in
AsterixDB. We will explain how we can use UDFs to preprocess, e.g., data collected using feeds (see the [feeds tutorial](feeds/tutorial.html)).


### <a name="installingUDF">Installing an AsterixDB Library</a>###

We assume you have followed the [installation instructions](../install.html) to set up a running AsterixDB instance. Let us refer your AsterixDB instance by the name "my_asterix".

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

        $ managix install -n my_asterix -d feeds -l testlib -p extlibs/asterix-external-data-0.8.7-binary-assembly.zip

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


## <a id="PreprocessingCollectedData">Preprocessing Collected Data</a> ###

In the following we assume that you already created the `TwitterFeed` and its corresponding data types and dataset following the instruction explained in the [feeds tutorial](feeds/tutorial.html).

A feed definition may optionally include the specification of a
user-defined function that is to be applied to each feed record prior
to persistence. Examples of pre-processing might include adding
attributes, filtering out records, sampling, sentiment analysis, feature
extraction, etc. We can express a UDF, which can be defined in AQL or in a programming
language such as Java, to perform such pre-processing. An AQL UDF is a good fit when
pre-processing a record requires the result of a query (join or aggregate)
over data contained in AsterixDB datasets. More sophisticated
processing such as sentiment analysis of text is better handled
by providing a Java UDF. A Java UDF has an initialization phase
that allows the UDF to access any resources it may need to initialize
itself prior to being used in a data flow. It is assumed by the
AsterixDB compiler to be stateless and thus usable as an embarrassingly
parallel black box. In contrast, the AsterixDB compiler can
reason about an AQL UDF and involve the use of indexes during
its invocation.

We consider an example transformation of a raw tweet into its
lightweight version called `ProcessedTweet`, which is defined next.

        use dataverse feeds;

        create type ProcessedTweet if not exists as open {
            id: string,
            user_name:string,
            location:point,
            created_at:string,
            message_text:string,
            country: string,
            topics: {{string}}
        };

        create dataset ProcessedTweets(ProcessedTweet)
        primary key id;

The processing required in transforming a collected tweet to its lighter version of type `ProcessedTweet` involves extracting the topics or hash-tags (if any) in a tweet
and collecting them in the referred "topics" attribute for the tweet.
Additionally, the latitude and longitude values (doubles) are combined into the spatial point type. Note that spatial data types are considered as first-class citizens that come with the support for creating indexes. Next we show a revised version of our example TwitterFeed that involves the use of a UDF. We assume that the UDF that contains the transformation logic into a "ProcessedTweet" is available as a Java UDF inside an AsterixDB library named 'testlib'. We defer the writing of a Java UDF and its installation as part of an AsterixDB library to a later section of this document.

        use dataverse feeds;

        create feed ProcessedTwitterFeed if not exists
        using "push_twitter"
        (("type-name"="Tweet"),
        ("consumer.key"="************"),
        ("consumer.secret"="**************"),
        ("access.token"="**********"),
        ("access.token.secret"="*************"))

        apply function testlib#addHashTagsInPlace;

Note that a feed adaptor and a UDF act as pluggable components. These
contribute towards providing a generic "plug-and-play" model where
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
example AQL statement that redefines the previous feed
"ProcessedTwitterFeed" in terms of their
respective parent feed (TwitterFeed).

        use dataverse feeds;

        drop feed ProcessedTwitterFeed if exists;

        create secondary feed ProcessedTwitterFeed from feed TwitterFeed
        apply function testlib#addHashTags;

        connect feed ProcessedTwitterFeed to dataset ProcessedTweets;

The `addHashTags` function is already provided in the example UDF.To see what records
are being inserted into the dataset, we can perform a simple dataset scan after
allowing a few moments for the feed to start ingesting data:

        use dataverse feeds;

        for $i in dataset ProcessedTweets limit 10 return $i;

For an example of how to write a Java UDF from scratch, the source for the example
UDF that has been used in this tutorial is available [here] (https://github.com/apache/incubator-asterixdb/tree/master/asterix-external-data/src/test/java/org/apache/asterix/external/library)

## <a name="installingUDF">Unstalling an AsterixDB Library</a>###

To uninstall a library, use the Managix uninstall command as follows:

        $ managix stop -n my_asterix

        $ managix uninstall -n my_asterix -d feeds -l testlib


