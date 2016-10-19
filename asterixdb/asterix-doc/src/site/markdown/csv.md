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

# CSV Support in AsterixDB

## Introduction - Defining a datatype for CSV

AsterixDB supports the CSV format for both data input and query result
output. In both cases, the structure of the CSV data must be defined
using a named ADM object datatype. The CSV format, limitations, and
MIME type are defined by [RFC
4180](https://tools.ietf.org/html/rfc4180).

CSV is not as expressive as the full Asterix Data Model, meaning that
not all data which can be represented in ADM can also be represented
as CSV. So the form of this datatype is limited. First, obviously it
may not contain any nested objects or lists, as CSV has no way to
represent nested data structures. All fields in the object type must
be primitive. Second, the set of supported primitive types is limited
to numerics (`int8`, `int16`, `int32`, `int64`, `float`, `double`) and
`string`.  On output, a few additional primitive types (`boolean`,
datetime types) are supported and will be represented as strings.

For the purposes of this document, we will use the following dataverse
and datatype definitions:

    drop dataverse csv if exists;
    create dataverse csv;
    use dataverse csv;

    create type "csv_type" as closed {
        "id": int32,
        "money": float,
        "name": string
    };

    create dataset "csv_set" ("csv_type") primary key "id";

Note: There is no explicit restriction against using an open datatype
for CSV purposes, and you may have optional fields in the datatype
(eg., `id: int32?`).  However, the CSV format itself is rigid, so
using either of these datatype features introduces possible failure
modes on output which will be discussed below.

## CSV Input

CSV data may be loaded into a dataset using the normal "load dataset"
mechanisms, utilizing the builtin "delimited-text" format. See
[Accessing External Data](aql/externaldata.html) for more
details. Note that comma is the default value for the "delimiter"
parameter, so it does not need to be explicitly specified.

In this case, the datatype used to interpret the CSV data is the
datatype associated with the dataset being loaded. So, to load a file
that we have stored locally on the NC into our example dataset:

    use dataverse csv;

    load dataset "csv_set" using localfs
    (("path"="127.0.0.1:///tmp/my_sample.csv"),
     ("format"="delimited-text"));

So, if the file `/tmp/my_sample.csv` contained

    1,18.50,"Peter Krabnitz"
    2,74.50,"Jesse Stevens"

then the preceding query would load it into the dataset `csv_set`.

If your CSV file has a header (that is, the first line contains a set
of field names, rather than actual data), you can instruct Asterix to
ignore this header by adding the parameter `"header"="true"`, eg.

    load dataset "csv_set" using localfs
    (("path"="127.0.0.1:///tmp/my_header_sample.csv"),
     ("format"="delimited-text"),
     ("header"="true"));

CSV data may also be loaded from HDFS; see [Accessing External
Data](aql/externaldata.html) for details.  However please note that
CSV files on HDFS cannot have headers. Attempting to specify
"header"="true" when reading from HDFS could result in non-header
lines of data being skipped as well.

## CSV Output

Any query may be rendered as CSV when using AsterixDB's HTTP
interface.  To do so, there are two steps required: specify the object
type which defines the schema of your CSV, and request that Asterix
use the CSV output format.

#### Output Object Type

Background: The result of any AQL query is an unordered list of
_instances_, where each _instance_ is an instance of an AQL
datatype. When requesting CSV output, there are some restrictions on
the legal datatypes in this unordered list due to the limited
expressability of CSV:

1. Each instance must be of a object type.
2. Each instance must be of the _same_ object type.
3. The object type must conform to the content and type restrictions
mentioned in the introduction.

While it would be possible to structure your query to cast all result
instances to a given type, it is not necessary. AQL offers a built-in
feature which will automatically cast all top-level instances in the
result to a specified named ADM object type. To enable this feature,
use a `set` statement prior to the query to set the parameter
`output-record-type` to the name of an ADM type. This type must have
already been defined in the current dataverse.

For example, the following request will ensure that all result
instances are cast to the `csv_type` type declared earlier:

    use dataverse csv;
    set output-record-type "csv_type";

    for $n in dataset "csv_set" return $n;

In this case the casting is redundant since by definition every value
in `csv_set` is already of type `csv_type`. But consider a more
complex query where the result values are created by joining fields
from different underlying datasets, etc.

Two notes about `output-record-type`:

1. This feature is not strictly related to CSV; it may be used with
any output formats (in which case, any object datatype may be
specified, not subject to the limitations specified in the
introduction of this page).
2. When the CSV output format is requested, `output-record-type` is in
fact required, not optional. This is because the type is used to
determine the field names for the CSV header and to ensure that the
ordering of fields in the output is consistent (which is obviously
vital for the CSV to make any sense).

#### Request the CSV Output Format

When sending requests to the Asterix HTTP API, Asterix decides what
format to use for rendering the results in one of two ways:

* A HTTP query parameter named "output", which must be set to one of
  the following values: `JSON`, `CSV`, or `ADM`.

* Based on the [`Accept` HTTP header](http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.1)

By default, Asterix will produce JSON output.  To select CSV output,
pass the parameter `output=CSV`, or set the `Accept` header on your
request to the MIME type `text/csv`. The details of how to accomplish
this will of course depend on what tools you are using to contact the
HTTP API.  Here is an example from a Unix shell prompt using the
command-line utility "curl" and specifying the "output query parameter:

    curl -G "http://localhost:19002/query" \
        --data-urlencode 'output=CSV' \
        --data-urlencode 'query=use dataverse csv;
              set output-record-type "csv_type";
              for $n in dataset csv_set return $n;'

Alternately, the same query using the `Accept` header:

    curl -G -H "Accept: text/csv" "http://localhost:19002/query" \
        --data-urlencode 'query=use dataverse csv;
              set output-record-type "csv_type";
              for $n in dataset csv_set return $n;'

Similarly, a trivial Java program to execute the above sample query
and selecting CSV output via the `Accept` header would be:

    import java.net.HttpURLConnection;
    import java.net.URL;
    import java.net.URLEncoder;
    import java.io.BufferedReader;
    import java.io.InputStream;
    import java.io.InputStreamReader;

    public class AsterixExample {
        public static void main(String[] args) throws Exception {
            String query = "use dataverse csv; " +
                "set output-record-type \"csv_type\";" +
                "for $n in dataset csv_set return $n";
            URL asterix = new URL("http://localhost:19002/query?query=" +
                                  URLEncoder.encode(query, "UTF-8"));
            HttpURLConnection conn = (HttpURLConnection) asterix.openConnection();
            conn.setRequestProperty("Accept", "text/csv");
            BufferedReader result = new BufferedReader
                (new InputStreamReader(conn.getInputStream()));
            String line;
            while ((line = result.readLine()) != null) {
                System.out.println(line);
            }
            result.close();
        }
    }

For either of the above examples, the output would be:

    1,18.5,"Peter Krabnitz"
    2,74.5,"Jesse Stevens"

assuming you had already run the previous examples to create the
dataverse and populate the dataset.

#### Outputting CSV with a Header

By default, AsterixDB will produce CSV results with no header line.
If you want a header, you may explicitly request it in one of two ways:

* By passing the HTTP query parameter "header" with the value "present"

* By specifying the MIME type {{text/csv; header=present}} in your
HTTP Accept: header.  This is consistent with RFC 4180.

#### Issues with open datatypes and optional fields

As mentioned earlier, CSV is a rigid format. It cannot express objects
with different numbers of fields, which ADM allows through both open
datatypes and optional fields.

If your output object type contains optional fields, this will not
result in any errors. If the output data of a query does not contain
values for an optional field, this will be represented in CSV as
`null`.

If your output object type is open, this will also not result in any
errors. If the output data of a query contains any open fields, the
corresponding rows in the resulting CSV will contain more
comma-separated values than the others. On each such row, the data
from the closed fields in the type will be output first in the normal
order, followed by the data from the open fields in an arbitrary
order.

According to RFC 4180 this is not strictly valid CSV (Section 2, rule
4, "Each line _should_ contain the same number of fields throughout
the file").  Hence it will likely not be handled consistently by all
CSV processors. Some may throw a parsing error. If you attempt to load
this data into AsterixDB later using `load dataset`, the extra fields
will be silently ignored. For this reason it is recommended that you
use only closed datatypes as output object types. AsterixDB allows to
use an open object type only to support cases where the type already
exists for other parts of your application.
