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

## <a name="introduction">Introduction</a>

Apache AsterixDB supports three languages for writing user-defined functions (UDFs): SQL++, Java, and Python
A user can encapsulate data processing logic into a UDF and invoke it
later repeatedly. For SQL++ functions, a user can refer to [SQL++ Functions](sqlpp/manual.html#Functions)
for their usages. This document will focus on UDFs in languages other than SQL++


## <a name="authentication">Endpoints and Authentication</a>

The UDF API endpoint used to deploy functions is not enabled by default until authentication has been configured properly.
Even if the endpoint is enabled, it is only accessible on the loopback interface on each NC to restrict access.

To enable it, we need to set the path to the credential file and populate it with our username and password.

The credential file is a simple `/etc/passwd` style text file with usernames and corresponding `bcrypt` hashed and salted
passwords. You can populate this on your own if you would like, but the `asterixhelper` utility can write the entries as
well. We can invoke `asterixhelper` like so:

    $ bin/asterixhelper -u admin -p admin -cp opt/local/conf add_credential

Then, in your `cc.conf`, in the `[cc]` section, add the correct `credential.file` path

    [nc]
    address = 127.0.0.1
    ...
    ...
    credential.file = conf/passwd

Now,restart the cluster if it was already started to allow the Cluster Controller to find the new credentials.


## <a name="installingUDF">Installing a Java UDF Library</a>

To install a UDF package to the cluster, we need to send a Multipart Form-data HTTP request to the `/admin/udf` endpoint
of the CC at the normal API port (`19004` by default). Any suitable tool will do, but for the example here I will use
`curl` which is widely available.

For example, to install a library with the following criteria:

* `udfs` dataverse name
* with a new Library name of `testlib`
* from `lib.zip` in the present working directory
* to the cluster at `localhost` with API port `19004` of the Asterix CC
* with credentials being a username and password of `admin:admin`

we would execute

    curl -v -u admin:admin -X POST -F 'data=@./lib.zip' -F 'type=java' localhost:19004/admin/udf/udfs/testlib

Any response other than `200` indicates an error in deployment.

In the AsterixDB source release, we provide several sample UDFs that you can try out.
You need to build the AsterixDB source to get the compiled UDF package. It can be found under
the `asterix-external-data` sub-project under the path
`asterixdb/asterix-external-data/src/test/java/org/apache/asterix/external/library`.
After compilation, the UDFs will be packed in a zip file at
`asterixdb/asterix-external-data/target/asterix-external-data-$VERSION-testlib.zip`
which you can use to upload to the AsterixDB cluster.

Assuming that these UDFs have been installed into the `testlib` library in`udfs` dataverse,
here is an example that uses the sample UDF `mysum` to compute the sum of two input integers.

    USE udfs;

    CREATE FUNCTION mysum(a: int32, b: int32)
    RETURNS int32
      AS "org.apache.asterix.external.library.MySumFactory" AT testlib;

## <a id="PythonUDF">Creating a Python UDF</a>

Python UDFs need to be rolled into a [shiv](https://github.com/linkedin/shiv) package with all their dependencies.
By default AsterixDB will use the Python interpreter located at `/usr/bin/python3`. This can be changed in the cluster
config `[common]` section using the `python.path` configuration variable.

First, let's devise a function that we would like to use in AsterixDB, `sentiment_mod.py`

    import os
    from typing import Tuple
    class sent_model:

        def __init__(self):
            good_words = os.path.join(os.path.dirname(__file__), 'good.txt')
            with open(good_words) as f:
                self.whitelist = f.read().splitlines()

        def sentiment(self, arg: Tuple[str])-> str:
            words = arg[0].split()
            for word in words:
                if word in self.whitelist:
                    return 'great'

            return 'eh'


Furthermore, let's assume 'good.txt' contains the following entries

    spam
    eggs
    ham

Now, in the module directory, execute `shiv` with all the dependencies of the module listed. We don't actually use
scikit-learn here (our method is obviously better!), but it's just included as an example of a real dependency.

    shiv -o lib.pyz --site-packages . scikit-learn

Then, deploy it the same as the Java UDF was, with the library name `pylib` in `udfs` dataverse

    curl -v -u admin:admin -X POST -F 'data=@./lib.pyz' -F 'type=python' localhost:19002/admin/udf/udfs/pylib

With the library deployed, we can define a function within it for use. For example, to expose the Python function
`sentiment` in the module `sentiment_mod` in the class `sent_model`, the `CREATE FUNCTION` would be as follows

    USE udfs;

    CREATE FUNCTION sentiment(a)
    RETURNS TweetType
      AS "sentiment_mod", "sent_model.sentiment" AT pylib;

By default, AsterixDB will treat all external functions as deterministic. It means the function must return the same
result for the same input, irrespective of when or how many times the function is called on that input.
This particular function behaves the same on each input, so it satisfies the deterministic property.
This enables better optimization of queries including this function.
If a function is not deterministic then it should be declared as such by using a `WITH` sub-clause:

    USE udfs;

    CREATE FUNCTION sentiment(text)
      AS "sentiment_mod", "sent_model.sentiment" AT pylib
      WITH { "deterministic": false }

With the function now defined, it can then be used as any other scalar SQL++ function would be. For example:

    USE udfs;

    INSERT INTO Tweets([
      {"id":1, "msg":"spam is great"},
      {"id":2, "msg":"i will not eat green eggs and ham"},
      {"id":3, "msg":"bacon is better"}
    ]);

    SELECT t.msg as msg, sentiment(t.msg) as sentiment
    FROM Tweets t;

## <a name="pytpes">Python Type Mappings</a>

Currently only a subset of AsterixDB types are supported in Python UDFs. The supported types are as follows:

- Integer types (int8,16,32,64)
- Floating point types (float, double)
- String
- Boolean
- Arrays, Sets (cast to lists)
- Objects (cast to dict)

Unsupported types can be cast to these in SQL++ first in order to be passed to a Python UDF

## <a name="execution">Execution Model For UDFs</a>

AsterixDB queries are deployed across the cluster as Hyracks jobs. A Hyracks job has a lifecycle that can be simplified
for the purposes of UDFs to
 - A pre-run phase which allocates resources, `open`
 - The time during which the job has data flowing through it, `nextFrame`
 - Cleanup and shutdown in `close`.

If a SQL++ function is defined as a member of a class in the library, the class will be instantiated
during `open`. The class will exist in memory for the lifetime of the query. Therefore if your function needs to reference
files or other data that would be costly to load per-call, making it a member variable that is initialized in the constructor
of the object will greatly increase the performance of the SQL++ function.

For each function invoked during a query, there will be an independent instance of the function per data partition. This
means that the function must not assume there is any global state or that it can assume things about the layout
of the data. The execution of the function will be parallel to the same degree as the level of data parallelism in the
cluster.

After initialization, the function bound in the SQL++ function definition is called once per tuple during the query
execution (i.e. `nextFrame`). Unless the function specifies `null-call` in the `WITH` clause, `NULL` values will be
skipped.

At the close of the query, the function is torn down and not re-used in any way. All functions should assume that
nothing will persist in-memory outside of the lifetime of a query, and any behavior contrary to this is undefined.

## <a id="UDFOnFeeds">Attaching a UDF on Data Feeds</a>

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

    USE udfs;

    CREATE TYPE TweetType IF NOT EXISTS AS OPEN {
        id: int64
    };

    CREATE DATASET ProcessedTweets(TweetType) PRIMARY KEY id;

As the `TweetType` is an open datatype, processed Tweets can be stored into the dataset after they are annotated
with an extra attribute. Given the datatype and dataset above, we can create a Twitter Feed with the same datatype.
Please refer to section [Data Ingestion](feeds.html) if you have any trouble in creating feeds.

    USE udfs;

    CREATE FEED TwitterFeed WITH {
      "adapter-name": "push_twitter",
      "type-name": "TweetType",
      "format": "twitter-status",
      "consumer.key": "************",
      "consumer.secret": "************",
      "access.token": "**********",
      "access.token.secret": "*************"
    };

Then we define the function we want to apply to the feed

    USE udfs;

    CREATE FUNCTION addMentionedUsers(t: TweetType)
      AS "org.apache.asterix.external.library.AddMentionedUsersFactory" AT testlib
      WITH { "resources": { "textFieldName": "text" } };

After creating the feed, we attach the UDF onto the feed pipeline and start the feed with following statements:

    USE udfs;

    CONNECT FEED TwitterFeed TO DATASET ProcessedTweets APPLY FUNCTION addMentionedUsers;

    START FEED TwitterFeed;

You can check the annotated Tweets by querying the `ProcessedTweets` dataset:

    SELECT * FROM ProcessedTweets LIMIT 10;
    
## <a name="adapter">Installing a user-defined Feed Adapter</a>

First, upload a zip file packaged the same way as a Java UDF, but also containing the adapter you would like to use.
Next, issue a `CREATE ADAPTER` statement referencing the class name. For example:

    CREATE ADAPTER TweetAdapter
      AS "org.apache.asterix.external.library.adapter.TestTypedAdapterFactory" AT testlib;
      

Then, the adapter can be used like any other adapter in a feed.

    CREATE FEED TweetFeed WITH {
      "adapter-name": "TweetAdapter",
      "type-name" : "TweetType",
      "num_output_records": 4
    };

## <a name="uninstall">Unstalling an UDF Library</a>

If you want to uninstall the UDF library, simply issue a `DELETE` against the endpoint you `POST`ed against once all
functions declared with the library are removed. First we'll drop the function we declared earlier:

    USE udfs;
    DROP FUNCTION mysum(a,b);

Then issue the proper `DELETE` request

    curl -u admin:admin -X DELETE localhost:19002/admin/udf/udfs/testlib

The library will also be dropped if you drop the dataverse entirely.
