/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.algebricks.core.algebra.base;

public enum PhysicalOperatorTag {
    AGGREGATE,
    ASSIGN,
    BROADCAST_EXCHANGE,
    BTREE_SEARCH,
    STATS,
    DATASOURCE_SCAN,
    DISTRIBUTE_RESULT,
    EMPTY_TUPLE_SOURCE,
    EXTERNAL_GROUP_BY,
    IN_MEMORY_HASH_JOIN,
    HASH_GROUP_BY,
    HASH_PARTITION_EXCHANGE,
    HASH_PARTITION_MERGE_EXCHANGE,
    HYBRID_HASH_JOIN,
    HDFS_READER,
    IN_MEMORY_STABLE_SORT,
    MICRO_PRE_CLUSTERED_GROUP_BY,
    NESTED_LOOP,
    NESTED_TUPLE_SOURCE,
    ONE_TO_ONE_EXCHANGE,
    PRE_SORTED_DISTINCT_BY,
    PRE_CLUSTERED_GROUP_BY,
    RANGE_PARTITION_EXCHANGE,
    RANDOM_MERGE_EXCHANGE,
    RTREE_SEARCH,
    RUNNING_AGGREGATE,
    SORT_MERGE_EXCHANGE,
    SINK,
    SINK_WRITE,
    SPLIT,
    STABLE_SORT,
    STREAM_LIMIT,
    STREAM_SELECT,
    STREAM_PROJECT,
    STRING_STREAM_SCRIPT,
    SUBPLAN,
    UNION_ALL,
    UNNEST,
    WRITE_RESULT,
    INSERT_DELETE,
    INDEX_INSERT_DELETE,
    UPDATE,
    SINGLE_PARTITION_INVERTED_INDEX_SEARCH,
    LENGTH_PARTITIONED_INVERTED_INDEX_SEARCH,
    PARTITIONINGSPLIT,
    EXTENSION_OPERATOR
}
