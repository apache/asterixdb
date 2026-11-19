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

# AsterixDB 102: A Cloud Mode Primer

This tutorial follows [AsterixDB 101](primer-sqlpp.html). It runs one AsterixDB node in cloud deployment mode with
[Adobe S3Mock](https://github.com/adobe/S3Mock) as its durable object store. A second S3Mock instance and Nessie provide
a local Iceberg table and catalog. By the end, you will have created native cloud collections, exported JSON, Parquet,
and CSV, imported JSON, queried external objects and Iceberg snapshots, measured native storage, and proved that native
data survives replacement of the AsterixDB container.

The setup is for learning and local testing. S3Mock and this single-process Nessie configuration are not a
production deployment.

## What Cloud Mode Changes

In the normal local configuration, AsterixDB's durable files live on the node's disks. In cloud mode, object storage is
the durable source of truth. Local I/O devices are a cache that AsterixDB can recreate from object storage. The
`cloud.storage.cache.policy` setting controls population: `eager` downloads durable files during recovery, `lazy`
fetches on first access, and `selective` balances the two based on file use. The compose stack chooses `selective`.
Under disk pressure the cache sweeper can evict inactive local index files; a later query fetches them again. The
compose stack gives S3Mock a named volume, but deliberately gives AsterixDB no persistent volume, so replacing the
AsterixDB container is an even stronger test than a sweep: it removes the entire local cache.

The S3Mock bucket has two independent uses in this tutorial:

* AsterixDB manages its own internal objects for native collections. Do not edit these objects.
* `COPY TO`, `COPY FROM`, and external collections access user-owned objects under the `primer/` prefix.

Iceberg is intentionally separate. Its data files and metadata live in a second S3Mock service's `iceberg-container`
bucket, while Nessie tracks the table catalog. This separation makes it clear that AsterixDB's native cloud storage and
an external Iceberg lakehouse are different storage lifecycles.

## Prerequisites

Install Git, Maven 3.3.9 or newer, JDK 21, and either Docker Compose or Podman Compose. This source revision
compiles with Java 21 and the generated container uses a Java 21 runtime; the startup log warns for untested JRE
versions. The commands below are written for Docker; every one of them has a Podman equivalent, and where the two
differ by more than the executable name the Podman form is given alongside.

Clone and build the current source tree:

```shell
git clone https://github.com/apache/asterixdb.git
cd asterixdb
mvn clean install -DskipTests -Ppodman.img -Dpodman.skip
```

That produces the server assembly the container image is built from, and installs the modules into the local Maven
repository for the Iceberg seeder to compile against later. The `podman.img` profile also copies the assembly and the
container entrypoint into `asterixdb/asterix-podman/target/`, where the `Containerfile` expects them; `-Dpodman.skip`
leaves the image build itself to the next step. Build the `apache/asterixdb:latest` image from that
module directory:

```shell
cd asterixdb/asterix-podman
docker build -t apache/asterixdb:latest -f src/main/resources/Containerfile .
# or, identically, with Podman:
podman build -t apache/asterixdb:latest -f src/main/resources/Containerfile .
```

The complete, versioned example is under `src/site/resources/cloud-mode`. Its principal files are:

| File | Purpose |
|---|---|
| [compose.yml](../cloud-mode/compose.yml) | AsterixDB, the durable S3Mock store, the Iceberg S3Mock store, and Nessie |
| [primer.sqlpp](../cloud-mode/primer.sqlpp) | Every SQL++ statement in the walkthrough, in order |
| [IcebergSeeder.java](../cloud-mode/IcebergSeeder.java) | Creates the Iceberg table and Delta files that the lakehouse sections query |

Work through the sections below by running each example yourself; that is the point of the exercise, and every
deterministic result is given alongside it. `primer.sqlpp` collects the same statements in order if you would rather
paste them from a file.

Start the services from the example directory; the remaining `docker compose` commands also run from there:

```shell
cd ../asterix-doc/src/site/resources/cloud-mode
docker compose up -d
docker compose ps
```

The first start pulls the pinned S3Mock and Nessie images and can take a few minutes. Wait until `storage` and
`iceberg-storage` are healthy and `asterix` is running. Then open `http://localhost:19006`, or check the query API:

```shell
curl --fail --get http://localhost:19002/query/service \
  --data-urlencode 'statement=SELECT VALUE "cloud mode is ready";'
```

The response contains:

```json
{"results":["cloud mode is ready"],"status":"success"}
```

The important cloud settings embedded in the compose file are:

| Setting | Purpose |
|---|---|
| `JAVA_OPTS=-Xmx2g` | Sizes the node's heap explicitly, rather than letting the JVM derive it from the container's share of host memory. |
| `cloud.deployment=true` | Selects the cloud-aware storage implementation. |
| `cloud.storage.scheme=s3` | Uses the S3 object-store client. |
| `cloud.storage.endpoint=http://storage:9090` | Sends S3 requests to the mock service on the compose network. |
| `cloud.storage.bucket=asterixdb-test` | Names the bucket containing AsterixDB's durable objects. |
| `cloud.storage.force.path.style=true` | Uses path-style URLs, as required by this local S3 endpoint. |
| `cloud.storage.anonymous.auth=true` | Avoids credentials in this local-only setup. |
| `cloud.storage.cache.policy=selective` | Caches files according to use while retaining object storage as the durable source. |
| `storage.partitioning=static` | Uses static storage partitions for the single-node demo. |
| `storage.partitions.count=3` | Matches one static storage partition to each configured I/O device. |

The heap setting matters on a small Docker or Podman VM. Left to the JVM's own sizing, the node can end up with less
job memory than the final cross-collection query needs, which fails with `HYR0009` reporting a job requirement above
capacity. Raise `-Xmx` further if you extend the walkthrough with heavier queries.

Real S3 deployments normally omit the custom endpoint, use credentials supplied by their environment, use a unique
bucket or prefix, enable TLS, and replace the single-node compose topology with the intended cluster deployment.
AsterixDB also supports `cloud.storage.scheme=gs` for Google Cloud Storage and `azblob` for Azure Blob Storage. This
self-contained primer uses S3 because mature local S3-compatible mocks are readily available.

## Create a Database, Namespace, and Native Collection

Run each example in the web interface, or send it to `/query/service`. Each example is one request, so every example
that uses the tutorial's objects starts with `USE`. Start with a database and a namespace. Namespaces are currently
created with the `DATAVERSE` keyword.

```sql
DROP DATABASE CloudPrimer IF EXISTS;
CREATE DATABASE CloudPrimer;
CREATE DATAVERSE CloudPrimer.Sales;
USE CloudPrimer.Sales;

CREATE TYPE OrderType AS {
  id: bigint,
  customer: string,
  region: string,
  amount: double
};

CREATE COLLECTION Orders(OrderType) PRIMARY KEY id;
CREATE INDEX OrdersByRegion ON Orders(region);
CREATE COLLECTION ColumnOrders(OrderType) PRIMARY KEY id WITH {
  "storage-format": {"format":"column"}
};

INSERT INTO Orders ([
  {"id":1, "customer":"Ada", "region":"west", "amount":125.50},
  {"id":2, "customer":"Linus", "region":"north", "amount":74.25},
  {"id":3, "customer":"Grace", "region":"west", "amount":210.00},
  {"id":4, "customer":"Edsger", "region":"east", "amount":95.00}
]);

INSERT INTO ColumnOrders (
  SELECT VALUE o FROM Orders AS o
);
```

The DDL creates metadata, a primary index, and a secondary index. `ColumnOrders` holds the same records in the columnar
storage format, for a storage diagnostic later on. The insert creates native storage components and
uploads durable data to S3Mock. Verify the logical data:

```sql
USE CloudPrimer.Sales;

SELECT o.region,
       count(*) AS orderCount,
       round(sum(o.amount), 2) AS revenue
FROM Orders AS o
GROUP BY o.region
ORDER BY o.region;
```

Expected result:

```json
[
  {"region":"east","orderCount":1,"revenue":95.0},
  {"region":"north","orderCount":1,"revenue":74.25},
  {"region":"west","orderCount":2,"revenue":335.5}
]
```

## Export with `COPY TO`

`COPY TO` writes query results to user-controlled objects. The target prefix must be empty, so the cleanup section at
the end is useful before repeating the tutorial.

```sql
USE CloudPrimer.Sales;

COPY (
  SELECT o.id, o.customer, o.region, o.amount
  FROM Orders AS o
  ORDER BY o.id
) exported
TO S3
PATH ("primer", "orders-json")
WITH {
  "container":"asterixdb-test",
  "serviceEndpoint":"http://storage:9090",
  "region":"us-east-1",
  "pathStyleAddressing":"true",
  "format":"json"
};
```

The statement has no row result. A successful API response has `"status":"success"`. AsterixDB creates one or more
JSON objects below `primer/orders-json/`; the number and names of objects are execution details and may vary.

Export a typed Parquet copy as well:

```sql
USE CloudPrimer.Sales;

COPY (
  SELECT o.id, o.customer, o.region, o.amount
  FROM Orders AS o
) exported
TO S3
PATH ("primer", "orders-parquet")
TYPE ({
  id: bigint,
  customer: string,
  region: string,
  amount: double
})
WITH {
  "container":"asterixdb-test",
  "serviceEndpoint":"http://storage:9090",
  "region":"us-east-1",
  "pathStyleAddressing":"true",
  "format":"parquet"
};
```

Parquet stores the schema with the file and is usually a better interchange format for analytical tools.

### CSV and partitioned lake-style output

Export pipe-delimited CSV with a header as well. CSV requires an explicit positional output schema, and
`max-objects-per-file` controls file sizing (the supported minimum is 1000):

```sql
USE CloudPrimer.Sales;

COPY (
  SELECT o.id, o.customer, o.region, o.amount FROM Orders AS o ORDER BY o.id
) exported
TO S3
PATH ("primer", "orders-csv")
AS (id bigint, customer string, region string, amount double)
WITH {
  "container":"asterixdb-test",
  "serviceEndpoint":"http://storage:9090",
  "region":"us-east-1",
  "pathStyleAddressing":"true",
  "format":"csv",
  "header":"true",
  "delimiter":"|",
  "max-objects-per-file":1000
};
```

`OVER (PARTITION BY ...)` binds typed path placeholders and creates a lake-style directory hierarchy. This example
writes gzip-compressed JSON below one path per region:

```sql
USE CloudPrimer.Sales;

COPY Orders AS o
TO S3
PATH ("primer", "orders-by-region", region)
OVER (PARTITION BY o.region AS region)
WITH {
  "container":"asterixdb-test",
  "serviceEndpoint":"http://storage:9090",
  "region":"us-east-1",
  "pathStyleAddressing":"true",
  "format":"json",
  "compression":"gzip"
};
```

The next section reads these outputs back through external collections.

## Query User-Owned Objects as External Collections

External collections keep metadata in AsterixDB but leave the records in their source objects. They are read when a
query runs; they are not part of AsterixDB's native cloud storage.

```sql
USE CloudPrimer.Sales;

CREATE TYPE OpenOrderType AS {
};

CREATE EXTERNAL COLLECTION ExportedOrders(OpenOrderType) USING S3 (
  ("container"="asterixdb-test"),
  ("definition"="primer/orders-json"),
  ("format"="json"),
  ("serviceEndpoint"="http://storage:9090"),
  ("region"="us-east-1"),
  ("pathStyleAddressing"="true")
);

SELECT VALUE count(*) FROM ExportedOrders;
```

Expected result:

```json
[4]
```

The empty open type accepts fields discovered in JSON records. The Parquet export reads back the same way, with a
different `definition` and `format`:

```sql
USE CloudPrimer.Sales;

CREATE EXTERNAL COLLECTION ExportedOrdersParquet(OpenOrderType) USING S3 (
  ("container"="asterixdb-test"),
  ("definition"="primer/orders-parquet"),
  ("format"="parquet"),
  ("serviceEndpoint"="http://storage:9090"),
  ("region"="us-east-1"),
  ("pathStyleAddressing"="true")
);

SELECT VALUE count(*) FROM ExportedOrdersParquet;
```

Expected result:

```json
[4]
```

CSV carries no field types, so its external collection declares `OrderType` and repeats the header and delimiter
settings used for the export. The partitioned export is plain JSON under one prefix; gzip is detected from
the object suffix:

```sql
USE CloudPrimer.Sales;

CREATE EXTERNAL COLLECTION ExportedOrdersCsv(OrderType) USING S3 (
  ("container"="asterixdb-test"),
  ("definition"="primer/orders-csv"),
  ("format"="csv"),
  ("header"="true"),
  ("delimiter"="|"),
  ("serviceEndpoint"="http://storage:9090"),
  ("region"="us-east-1"),
  ("pathStyleAddressing"="true")
);

CREATE EXTERNAL COLLECTION PartitionedOrders(OpenOrderType) USING S3 (
  ("container"="asterixdb-test"),
  ("definition"="primer/orders-by-region"),
  ("format"="json"),
  ("serviceEndpoint"="http://storage:9090"),
  ("region"="us-east-1"),
  ("pathStyleAddressing"="true")
);

SELECT VALUE {
  "csvRows": (SELECT VALUE count(*) FROM ExportedOrdersCsv)[0],
  "partitionedRows": (SELECT VALUE count(*) FROM PartitionedOrders)[0]
};
```

Expected result:

```json
[{"csvRows":4,"partitionedRows":4}]
```

## Import with `COPY FROM`

Unlike an external collection, `COPY FROM` materializes records in a native collection. Create an empty target and load
the JSON export:

```sql
USE CloudPrimer.Sales;

CREATE COLLECTION ImportedOrders(OrderType) PRIMARY KEY id;

COPY ImportedOrders
FROM S3
PATH ("primer/orders-json")
WITH {
  "container":"asterixdb-test",
  "serviceEndpoint":"http://storage:9090",
  "region":"us-east-1",
  "pathStyleAddressing":"true",
  "format":"json"
};

SELECT VALUE count(*) FROM ImportedOrders;
```

Expected result:

```json
[4]
```

`ImportedOrders` is now independent of the exported files. Its durable native files are managed by AsterixDB in cloud
storage.

## Inspect Native Storage Size

The `storage_size` data-source function reports bytes used by a native collection and its indexes. The exact number
depends on the build and storage layout, so test relationships rather than a fixed value.

```sql
SELECT s.size > 0 AS hasCloudStorage
FROM storage_size("CloudPrimer", "Sales", "Orders") AS s;
```

Expected result:

```json
[{"hasCloudStorage":true}]
```

To measure only the primary index, pass its collection name as the fourth argument. The byte count depends on the
build, so no expected value is shown:

```sql
SELECT VALUE s.size
FROM storage_size("CloudPrimer", "Sales", "Orders", "Orders") AS s;
```

For lower-level diagnostics, `storage_components` returns the native collection's LSM disk components. It is a private
diagnostic function, so enable private functions explicitly:

```sql
SET `import-private-functions` "true";

SELECT VALUE count(*) > 0
FROM storage_components("Sales", "Orders", "CloudPrimer") AS c;
```

Expected result:

```json
[true]
```

Component identifiers and counts vary as flushes and merges occur, so the query checks only that at least one component
exists.

Three more private diagnostics expose index entries, per-partition records, and column statistics:

```sql
SET `import-private-functions` "true";

SELECT VALUE {
  "secondaryIndexEntries": (
    SELECT VALUE count(*)
    FROM dump_index("Sales", "Orders", "OrdersByRegion", "CloudPrimer") AS entry
  )[0],
  "partitionRows":
      (SELECT VALUE count(*) FROM query_partition("CloudPrimer", "Sales", "Orders", 0) AS row)[0]
    + (SELECT VALUE count(*) FROM query_partition("CloudPrimer", "Sales", "Orders", 1) AS row)[0]
    + (SELECT VALUE count(*) FROM query_partition("CloudPrimer", "Sales", "Orders", 2) AS row)[0],
  "columnEstimatePartitions": (
    SELECT VALUE count(*)
    FROM estimated_collection_column_count(
      "CloudPrimer", "Sales", "ColumnOrders"
    ) AS estimate
  )[0]
};
```

Expected result:

```json
[{"secondaryIndexEntries":4,"partitionRows":4,"columnEstimatePartitions":3}]
```

That is four secondary-index entries, four records across the three static partitions, and three column-estimate
records, one per partition. `estimated_collection_column_count` applies only to column storage, which is why the setup
created `ColumnOrders`. These functions are diagnostic APIs; applications should not depend on their physical record
shapes.

## Mutate Data and Exercise the Secondary Index

Before the container replacement at the end of the primer, overwrite an existing key, insert a new key, and delete
another key. This exercises more of the LSM lifecycle than an initial insert alone:

```sql
USE CloudPrimer.Sales;

UPSERT INTO Orders ([
  {"id":2, "customer":"Linus", "region":"north", "amount":80.00},
  {"id":5, "customer":"Margaret", "region":"north", "amount":150.00}
]);

DELETE FROM Orders AS o WHERE o.id = 4;

SELECT VALUE o
FROM Orders AS o
WHERE o.region = "north"
ORDER BY o.id;
```

Expected result:

```json
[
  {"id":2,"customer":"Linus","region":"north","amount":80.0},
  {"id":5,"customer":"Margaret","region":"north","amount":150.0}
]
```

The predicate is on `region`, the field indexed by `OrdersByRegion`. To confirm that the query uses the index, run it
again prefixed with `EXPLAIN`: the optimized plan searches `OrdersByRegion` instead of scanning `Orders`.

## Iceberg

The compose stack includes the two additional pieces an Iceberg demonstration needs:

* `iceberg-storage` is a second S3Mock service, which creates its `iceberg-container` bucket at startup from
  `COM_ADOBE_TESTING_S3MOCK_STORE_INITIAL_BUCKETS`. It is kept separate from `storage` so that the Iceberg lakehouse
  and AsterixDB's own durable store cannot be confused for one another.
* `nessie` is the Iceberg catalog. `IcebergSeeder.java` creates `cloud_primer.orders`, writes three rows, commits the
  first snapshot, writes three more rows, and commits the second snapshot.

The seeder runs on the host against this checkout's Iceberg dependencies. With the stack up, compile and run it from
the repository root; the subshell leaves you in the example directory afterwards:

```shell
(cd "$(git rev-parse --show-toplevel)"
app=asterixdb/asterix-app
mvn -q -pl $app -DskipTests test-compile dependency:build-classpath \
  -Dmdep.includeScope=test -Dmdep.outputFile="$PWD/$app/target/test-classpath.txt"
cp="$app/target/test-classes:$app/target/classes:$(cat $app/target/test-classpath.txt)"
javac -cp "$cp" -d $app/target/cloud-primer-seeder asterixdb/asterix-doc/src/site/resources/cloud-mode/IcebergSeeder.java
java -cp "$app/target/cloud-primer-seeder:$cp" IcebergSeeder $app/target/iceberg-snapshots.properties
cat $app/target/iceberg-snapshots.properties)
```

The seeder reaches Nessie at `localhost:19120` and `iceberg-storage` at `localhost:8001`, the host ports the compose
file publishes. It builds the Delta files under `target/generated_delta_files` in the repository root before uploading
them. The properties file it writes holds `SNAPSHOT_1`, its commit time `SNAPSHOT_1_TIMESTAMP` in epoch
milliseconds, and `SNAPSHOT_2`. The IDs differ on every run, so the time-travel statements below use placeholders:
substitute `SNAPSHOT_1` for `<first-snapshot-id>` and `SNAPSHOT_1_TIMESTAMP` for `<first-snapshot-epoch-millis>`.

First register the Nessie catalog in AsterixDB:

```sql
USE CloudPrimer.Sales;

CREATE CATALOG CloudPrimerIceberg
TYPE Iceberg
SOURCE NESSIE
WITH {
  "uri":"http://nessie:19120/api/v2",
  "warehouse":"s3://iceberg-container/nessie/warehouse"
};
```

An Iceberg external collection combines that catalog with the S3 properties needed to read table metadata and Parquet
data. The open type exposes all table fields:

```sql
USE CloudPrimer.Sales;

CREATE TYPE IcebergRow AS {
};

CREATE EXTERNAL COLLECTION IcebergLatest(IcebergRow) USING S3 (
  ("accessKeyId"="dummyAccessKey"),
  ("secretAccessKey"="dummySecretKey"),
  ("region"="us-west-2"),
  ("serviceEndpoint"="http://iceberg-storage:9090"),
  ("pathStyleAddressing"="true"),
  ("inputStreamType"="classic"),
  ("changeDetectionMode"="none"),
  ("table-format"="iceberg"),
  ("namespace"="cloud_primer"),
  ("tableName"="orders"),
  ("catalogName"="CloudPrimerIceberg"),
  ("format"="parquet"),
  ("decimal-to-double"="true"),
  ("date-to-int"="true"),
  ("time-to-int"="true"),
  ("timestamp-to-long"="true")
);
```

`IcebergLatest` reads the table's current snapshot, which holds all six rows:

```sql
USE CloudPrimer.Sales;

SELECT VALUE count(*) FROM IcebergLatest;
```

Expected result:

```json
[6]
```

Projection, predicates, grouping, ordering, and aggregation are normal SQL++ operations:

```sql
USE CloudPrimer.Sales;

SELECT i.region, count(*) AS orderCount, round(sum(i.amount), 2) AS revenue
FROM IcebergLatest AS i
WHERE i.amount >= 90
GROUP BY i.region
ORDER BY i.region;
```

Expected result:

```json
[
  {"region":"east","orderCount":1,"revenue":95.0},
  {"region":"north","orderCount":1,"revenue":180.0},
  {"region":"west","orderCount":2,"revenue":335.5}
]
```

### Snapshot selection and time travel

There are three ways to read an older snapshot. A collection can pin one with `snapshotId`. This collection is
`IcebergLatest` with `SNAPSHOT_1` pinned:

```sql
USE CloudPrimer.Sales;

CREATE EXTERNAL COLLECTION IcebergAtFirstSnapshot(IcebergRow) USING S3 (
  ("accessKeyId"="dummyAccessKey"),
  ("secretAccessKey"="dummySecretKey"),
  ("region"="us-west-2"),
  ("serviceEndpoint"="http://iceberg-storage:9090"),
  ("pathStyleAddressing"="true"),
  ("inputStreamType"="classic"),
  ("changeDetectionMode"="none"),
  ("table-format"="iceberg"),
  ("namespace"="cloud_primer"),
  ("tableName"="orders"),
  ("catalogName"="CloudPrimerIceberg"),
  ("format"="parquet"),
  ("snapshotId"="<first-snapshot-id>"),
  ("decimal-to-double"="true"),
  ("date-to-int"="true"),
  ("time-to-int"="true"),
  ("timestamp-to-long"="true")
);

SELECT VALUE i FROM IcebergAtFirstSnapshot AS i ORDER BY i.id;
```

It returns the three rows of the first commit:

```json
[
  {"id":1,"customer":"Ada","region":"west","amount":125.5},
  {"id":2,"customer":"Linus","region":"north","amount":74.25},
  {"id":3,"customer":"Grace","region":"west","amount":210.0}
]
```

Alternatively, a statement can override the collection's default:

```sql
USE CloudPrimer.Sales;

SELECT VALUE i
FROM IcebergLatest AS i AT snapshot "<first-snapshot-id>"
ORDER BY i.id;
```

The alias appears before `AT snapshot`. The result again contains only IDs 1 through 3, as above. The statement
snapshot takes priority over a collection setting, which is useful for reproducible analysis without creating metadata
for every snapshot.

`AT timestamp` resolves the newest snapshot at or before a point in time. With the first commit's timestamp it returns
the same three rows:

```sql
USE CloudPrimer.Sales;

SELECT VALUE i
FROM IcebergLatest AS i AT timestamp "<first-snapshot-epoch-millis>"
ORDER BY i.id;
```

The runtime also accepts date and date-time timestamp strings; invalid formats remain covered by the Iceberg negative
suite.

Catalog metadata is stored natively in AsterixDB and therefore survives the AsterixDB container replacement. The
Iceberg table itself is external: Nessie and `iceberg-storage` must remain available for subsequent Iceberg reads. This
differs from `Orders`, whose durable collection files are owned by AsterixDB in `storage`.

Inspect the registered record through the metadata collection:

```sql
SELECT VALUE {
  "catalogName": c.CatalogName,
  "catalogType": c.CatalogType
}
FROM Metadata.Catalog AS c
WHERE c.CatalogName = "CloudPrimerIceberg";
```

Expected result:

```json
[{"catalogName":"CloudPrimerIceberg","catalogType":"Iceberg"}]
```

The record's other fields, such as the creator UUID and timestamp, differ from run to run, so the query projects only
the stable ones.

### Delta Lake

The seeder also reuses the Delta test-suite generator to create a one-row Delta transaction log and Parquet file, then
uploads them below `delta/orders` in `iceberg-container`. Register it without a separate catalog:

```sql
USE CloudPrimer.Sales;

CREATE TYPE DeltaRow AS {
};

CREATE EXTERNAL COLLECTION DeltaOrders(DeltaRow) USING S3 (
  ("accessKeyId"="dummyAccessKey"),
  ("secretAccessKey"="dummySecretKey"),
  ("container"="iceberg-container"),
  ("definition"="delta/orders"),
  ("region"="us-west-2"),
  ("serviceEndpoint"="http://iceberg-storage:9090"),
  ("pathStyleAddressing"="true"),
  ("table-format"="delta")
);

SELECT VALUE d FROM DeltaOrders AS d ORDER BY d.id;
```

Expected result:

```json
[{"id":0,"name":"Cooper"}]
```

### Other catalog sources

The Iceberg runtime test suite covers catalog DDL for `AWS_GLUE`, `AWS_GLUE_REST`, `S3_TABLES`,
`BIGLAKE_METASTORE`, `NESSIE`, and `NESSIE_REST`. This local primer executes Nessie because it is self-contained.
The AWS and Google catalog forms require real service endpoints and credentials and should be tested in their
provider-specific integration environments, not with dummy compose credentials.

Iceberg and Delta external collections are read-only in this workflow: query them with SQL++, but do not `INSERT`,
`UPSERT`, `DELETE`, or `COPY TO` them.

## Prove Stop/Start Persistence

Stop and remove the AsterixDB container while preserving the S3Mock service and named volume:

```shell
docker compose stop asterix
docker compose rm -f asterix
docker compose up -d asterix
```

Wait for the query API, then run:

```sql
USE CloudPrimer.Sales;

SELECT VALUE {
  "nativeOrders": (SELECT VALUE count(*) FROM Orders)[0],
  "nativeNorthOrders": (SELECT VALUE count(*) FROM Orders o WHERE o.region = "north")[0],
  "nativeRevenue": (SELECT VALUE round(sum(o.amount), 2) FROM Orders o)[0],
  "importedOrders": (SELECT VALUE count(*) FROM ImportedOrders)[0],
  "externalOrders": (SELECT VALUE count(*) FROM ExportedOrders)[0],
  "icebergRows": (SELECT VALUE count(*) FROM IcebergLatest)[0],
  "firstIcebergSnapshotRows": (SELECT VALUE count(*) FROM IcebergAtFirstSnapshot)[0]
};
```

Expected result:

```json
[{"nativeOrders":4,"nativeNorthOrders":2,"nativeRevenue":565.5,
  "importedOrders":4,"externalOrders":4,"icebergRows":6,"firstIcebergSnapshotRows":3}]
```

This is the key cloud-mode observation: the new AsterixDB container has no old local cache, yet mutated native records,
metadata, indexes, and user-owned export objects remain available because S3Mock and its volume retained the bucket.

`docker compose down` also preserves named volumes by default. `docker compose down -v` deletes the S3Mock volume and
therefore destroys this tutorial's durable data. The Iceberg side is not durable in either case: `iceberg-storage` has
no volume and Nessie keeps its catalog in memory, so after a `down` and `up` run the seeder again and recreate
`IcebergAtFirstSnapshot` with the new `SNAPSHOT_1`.

## Relationship to the Test Suites

This primer is an integration narrative, not a replacement for the regression suites. Its cases are derived from and
cross-check these areas:

| Primer behavior | Regression coverage |
|---|---|
| Cloud bootstrap, native DDL/DML, indexes, cache and recovery | cloud-storage suites and cloud-storage-only suite |
| S3 external JSON/Parquet and `COPY FROM`/`COPY TO` | external-dataset S3 and COPY suites |
| `storage_size` | storage-size runtime cases |
| Nessie catalog, Parquet scan and supported catalog DDL | Iceberg suite |
| Collection `snapshotId`, statement `AT snapshot` and `AT timestamp` | Iceberg snapshots cases |
| Delta transaction log and Parquet scan | external-dataset Delta cases |
| Bad endpoints, snapshot formats and conversion options | Iceberg negative cases |

The primer intentionally covers only positive, deterministic cases. Negative cases often assert exact error
codes/messages and remain better suited to the test harness. Likewise, cloud-provider authentication, throttling, and
eventual-consistency scenarios cannot be faithfully represented by local mocks.

## Cleanup and Troubleshooting

Remove SQL++ metadata before repeating the examples:

```sql
DROP DATABASE CloudPrimer IF EXISTS;
```

That does not remove user-owned `COPY TO` objects. To reset the entire local exercise, including exports and native
cloud data, remove the stack and its named volume:

```shell
docker compose down -v
```

Common problems:

* If port 19002 or 19006 is already in use, stop the other AsterixDB process or move the host ports by setting
  `ASTERIX_QUERY_PORT` and `ASTERIX_WEB_PORT` before `docker compose up`.
* Do not continue until the readiness query returns `"status":"success"`. A `"Cluster is UNUSABLE"` response means
  startup did not complete; inspect the AsterixDB log before running the walkthrough.
* If `asterix` repeatedly restarts, inspect `docker compose logs asterix storage`; it normally means S3Mock is not
  healthy or the endpoint/bucket settings do not match.
* If `COPY TO` says the target is not empty, reset the named volume or choose a new path such as
  `primer/orders-json-2`.
* `storage` is the S3 endpoint hostname only inside the compose network. Host-side tools need `localhost` and an
  explicitly published S3Mock port.
* Do not use `docker compose down -v` when testing persistence; `-v` intentionally deletes the durable store.
