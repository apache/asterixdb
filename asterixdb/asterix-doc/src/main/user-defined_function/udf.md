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

## <a name="introduction">Introduction</a>##

Apache AsterixDB supports two languages for writing user-defined functions (UDFs): SQL++ and Java.
A user can encapsulate data processing logic into a UDF and invoke it
later repeatedly. For SQL++ functions, a user can refer to [SQL++ Functions](sqlpp/manual.html#Functions)
for their usages. In this document, we
focus on how to install/invoke/uninstall a Java function library using the Ansible script that we provide.


## <a name="installingUDF">Installing an UDF Library</a>##

UDFs have to be installed offline.
This section describes the process assuming that you have followed the preceding [ansible installation instructions](ansible.html)
to deploy an AsterixDB instance on your local machine or cluster. Here are the
instructions to install an UDF library:

- Step 1: Stop the AsterixDB instance if it is ACTIVE.

        $ bin/stop.sh

- Step 2: Deploy the UDF package.

        $ bin/udf.sh -m i -d DATAVERSE_NAME -l LIBRARY_NAME -p UDF_PACKAGE_PATH

- Step 3: Start AsterixDB

        $ bin/start.sh

After AsterixDB starts, you can use the following query to check whether your UDFs have been sucessfully registered with the system.

        SELECT * FROM Metadata.`Function`;

In the AsterixDB source release, we provide several sample UDFs that you can try out.
You need to build the AsterixDB source to get the compiled UDF package. It can be found under
the `asterixdb-external` sub-project. Assuming that these UDFs have been installed into the `udfs` dataverse and `testlib` library,
here is an example that uses the sample UDF `mysum` to compute the sum of two input integers.

        use udfs;

        testlib#mysum(3,4);

## <a id="UDFOnFeeds">Attaching a UDF on Data Feeds</a> ##

In [Data Ingestion using feeds](feeds.html), we introduced an efficient way for users to get data into AsterixDB. In
some use cases, users may want to pre-process the incoming data before storing it into the dataset. To meet this need,
AsterixDB allows
the user to attach a UDF onto the ingestion pipeline. Following the example in [Data Ingestion](feeds.html), here we
show an example of how to attach a UDF that extracts the user names mentioned from the incoming Tweet text, storing the
processed Tweets into a dataset.

We start by creating the datatype and dataset that will be used for the feed and UDF. One thing to keep in mind is that
data flows from the feed to the UDF and then to the dataset. This means that the feed's datatype
should be the same as the input type of the UDF, and the output datatype of the UDF should be the same as the dataset's
datatype. Thus, users should make sure that their datatypes are consistent in the UDF configuration. Users can also
take advantage of open datatypes in AsterixDB by creating a minimum description of the data for simplicity.
Here we use open datatypes:

        use udfs;

        create type TweetType if not exists as open {
            id: int64
        };

        create dataset ProcessedTweets(TweetType) primary key id;

As the `TweetType` is an open datatype, processed Tweets can be stored into the dataset after they are annotated
with an extra attribute. Given the datatype and dataset above, we can create a Twitter Feed with the same datatype.
Please refer to section [Data Ingestion](feeds.html) if you have any trouble in creating feeds.

        use udfs;

        create feed TwitterFeed with {
          "adapter-name": "push_twitter",
          "type-name": "TweetType",
          "format": "twitter-status",
          "consumer.key": "************",
          "consumer.secret": "************",
          "access.token": "**********",
          "access.token.secret": "*************"
        };

After creating the feed, we attach the UDF onto the feed pipeline and start the feed with following statements:

        use udfs;

        connect feed TwitterFeed to dataset ProcessedTweets apply function udfs#addMentionedUsers;

        start feed TwitterFeed;

You can check the annotated Tweets by querying the `ProcessedTweets` dataset:

        SELECT * FROM ProcessedTweets LIMIT 10;

## <a name="udfConfiguration">A quick look of the UDF configuration</a>##

AsterixDB uses an XML configuration file to describe the UDFs. A user can use it to define and reuse their compiled UDFs
for different purposes. Here is a snippet of the configuration used in our [previous example](#UDFOnFeeds):

        <libraryFunction>
          <name>addMentionedUsers</name>
          <function_type>SCALAR</function_type>
          <argument_type>TweetType</argument_type>
          <return_type>TweetType</return_type>
          <definition>org.apache.asterix.external.library.AddMentionedUsersFactory</definition>
          <parameters>text</parameters>
        </libraryFunction>

Here are the explanations of the fields in the configuration file:

       name: The proper name that is used for invoke the function.
       function_type: The type of the function.
       argument_type: The datatype of the arguments passed in. If there is more than one parameter, separate them with comma(s), e.g., `AINT32,AINT32`.
       return_type: The datatype of the returning value.
       definition: A reference to the function factory.
       parameters: The parameters passed into the function.

In our feeds example, we passed in `"text"` as a parameter to the function so it knows which field to look at to get the Tweet text.
If the Twitter API were to change its field names in the future, we can accommodate that change by simply modifying the configuration file
instead of recompiling the whole UDF package. This feature can be further utilized in use cases where a user has a Machine Learning
algorithm with different trained model files. If you are interested, You can find more examples [here](https://github.com/apache/asterixdb/tree/master/asterixdb/asterix-external-data/src/test/java/org/apache/asterix/external/library)

## <a name="uninstall">Unstalling an UDF Library</a>##

If you want to uninstall the UDF library, put AsterixDB into `INACTVIVE` mode and run following command:

        $ bin/udf.sh -m u -d DATAVERSE_NAME -l LIBRARY_NAME


