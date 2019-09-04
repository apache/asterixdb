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

# Filter-Based LSM Index Acceleration

## <a id="toc">Table of Contents</a>

* [Motivation](#Motivation)
* [Filters in AsterixDB](#FiltersInAsterixDB)
* [Filters and Merge Policies](#FiltersAndMergePolicies)

## <a id="Motivation">Motivation</a> <font size="4"><a href="#toc">[Back to TOC]</a></font>

Traditional relational databases usually employ conventional index
structures such as B+ trees due to their low read latency.  However,
such traditional index structures use in-place writes to perform
updates, resulting in costly random writes to disk. Today's emerging
applications often involve insert-intensive workloads for which the
cost of random writes prohibits efficient ingestion of
data. Consequently, popular NoSQL systems such as Cassandra, HBase,
LevelDB, BigTable, etc. have adopted Log-Structured Merge (LSM) Trees
as their storage structure. LSM-trees avoids the cost of random writes
by batching updates into a component of the index that resides in main
memory -- an *in-memory component*. When the space occupancy of
the in-memory component exceeds a specified threshold, its entries are
*flushed* to disk forming a new component -- a *disk component*. As
disk components accumulate on disk, they are periodically merged
together subject to a *merge policy* that decides when and what to
merge. The benefit of the LSM-trees comes at the cost of possibly
sacrificing read efficiency, but, it has been shown in previous
studies that these inefficiencies can be mostly mitigated.

AsterixDB has also embraced LSM-trees, not just by using them as
primary indexes, but also by using the same LSM-ification technique
for all of its secondary index structures. In particular, AsterixDB
adopted a generic framework for converting a class of indexes (that
includes conventional B+ trees, R trees, and inverted indexes) into
LSM-based secondary indexes, allowing higher data ingestion rates. In
fact, for certain index structures, our results have shown that using
an LSM-based version of an index can be made to significantly
outperform its conventional counterpart for *both* ingestion
and query speed (an example of such an index being the R-tree for
spatial data).

Since an LSM-based index naturally partitions data into multiple disk
components, it is possible, when answering certain queries, to exploit
partitioning to only access some components and safely filter out the
remaining components, thus reducing query times. For instance,
referring to our
[TinySocial](primer.html#ADM:_Modeling_Semistructured_Data_in_AsterixDB)
example, suppose a user always retrieves tweets from the
`TweetMessages` dataset based on the `send-time` field (e.g., tweets
posted in the last 24 hours). Since there is not a secondary index on
the `send-time` field, the only available option for AsterixDB would
be to scan the whole `TweetMessages` dataset and then apply the
predicate as a post-processing step. However, if disk components of
the primary index were tagged with the minimum and maximum timestamp
values of the objects they contain, we could utilize the tagged
information to directly access the primary index and prune components
that do not match the query predicate. Thus, we could save substantial
cost by avoiding scanning the whole dataset and only access the
relevant components. We simply call such tagging information that are
associated with components, filters. (Note that even if there were a
secondary index on `send-time` field, using filters could save
substantial cost by avoiding accessing the secondary index, followed
by probing the primary index for every fetched entry.) Moreover, the
same filtering technique can also be used with any secondary LSM index
(e.g., an LSM R-tree), in case the query contains multiple predicates
(e.g., spatial and temporal predicates), to obtain similar pruning
power.

## <a id="FiltersInAsterixDB">Filters in AsterixDB</a> <font size="4"><a href="#toc">[Back to TOC]</a></font>

We have added support for LSM-based filters to all of AsterixDB's
index types. To enable the use of filters, the user must specify the
filter's key when creating a dataset, as shown below:

#### Creating a Dataset with a Filter  ####

        create dataset Tweets(TweetType) primary key tweetid with filter on send-time;

Filters can be created on any totally ordered datatype (i.e., any
field that can be indexed using a B+ -tree), such as integers,
doubles, floats, UUIDs, datetimes, etc.

When a dataset with a filter is created, the name of the filter's key
field is persisted in the `Metadata.Dataset` dataset (which is the metadata
dataset that stores the details of each dataset in an AsterixDB
instance) so that DML operations against the dataset can recognize the
existence of filters and can update them or utilize them
accordingly. Creating a dataset with a filter in AsterixDB implies
that the primary and all secondary indexes of that dataset will
maintain filters on their disk components. Once a filtered dataset is
created, the user can use the dataset normally (just like any other
dataset). AsterixDB will automatically maintain the filters and will
leverage them to efficiently answer queries whenever possible (i.e.,
when a query has predicates on the filter's key).

## <a id="FiltersAndMergePolicies">Filters and Merge Policies</a> <font size="4"><a href="#toc">[Back to TOC]</a></font>

The AsterixDB default merge policy, the prefix merge policy, relies on
component sizes and the number of components to decide which
components to merge. This merge policy has proven to provide excellent
performance for both ingestion and queries. However, when evaluating
our filtering solution with the prefix policy, we observed a behavior
that can reduce filter effectiveness. In particular, we noticed that
under the prefix merge policy, the disk components of a secondary
index tend to be constantly merged into a single component. This is
because the prefix policy relies on a single size parameter for all of
the indexes of a dataset. This parameter is typically chosen based on
the sizes of the disk components of the primary index, which tend to
be much larger than the sizes of the secondary indexes' disk
components. This difference caused the prefix merge policy to behave
similarly to the constant merge policy (i.e., relatively poorly) when
applied to secondary indexes in the sense that the secondary indexes
are constantly merged into a single disk component. Consequently, the
effectiveness of filters on secondary indexes was greatly reduced
under the prefix-merge policy, but they were still effective when
probing the primary index.  Based on this behavior, we developed a new
merge policy, an improved version of the prefix policy, called the
correlated-prefix policy. The basic idea of this policy is that it
delegates the decision of merging the disk components of all the
indexes in a dataset to the primary index. When the policy decides
that the primary index needs to be merged (using the same decision
criteria as for the prefix policy), then it will issue successive
merge requests to the I/O scheduler on behalf of all other indexes
associated with the same dataset. The end result is that secondary
indexes will always have the same number of disk components as their
primary index under the correlated-prefix merge policy. This has
improved query performance, since disk components of secondary indexes
now have a much better chance of being pruned.
