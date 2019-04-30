/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.algebricks.core.algebra.base;

public enum PhysicalOperatorTag {
    AGGREGATE,
    ASSIGN,
    BROADCAST_EXCHANGE,
    BTREE_SEARCH,
    BULKLOAD,
    DATASOURCE_SCAN,
    DISTRIBUTE_RESULT,
    EMPTY_TUPLE_SOURCE,
    DELEGATE_OPERATOR,
    EXTERNAL_GROUP_BY,
    EXTERNAL_LOOKUP,
    FORWARD,
    HASH_PARTITION_EXCHANGE,
    HASH_PARTITION_MERGE_EXCHANGE,
    HDFS_READER,
    HYBRID_HASH_JOIN,
    IN_MEMORY_HASH_JOIN,
    MICRO_STABLE_SORT,
    INDEX_BULKLOAD,
    INDEX_INSERT_DELETE,
    INSERT_DELETE,
    LENGTH_PARTITIONED_INVERTED_INDEX_SEARCH,
    MATERIALIZE,
    MICRO_PRE_CLUSTERED_GROUP_BY,
    MICRO_PRE_SORTED_DISTINCT_BY,
    MICRO_UNION_ALL,
    NESTED_LOOP,
    NESTED_TUPLE_SOURCE,
    ONE_TO_ONE_EXCHANGE,
    PRE_CLUSTERED_GROUP_BY,
    PRE_SORTED_DISTINCT_BY,
    RANDOM_PARTITION_EXCHANGE,
    RANDOM_MERGE_EXCHANGE,
    RANGE_PARTITION_EXCHANGE,
    RANGE_PARTITION_MERGE_EXCHANGE,
    SEQUENTIAL_MERGE_EXCHANGE,
    REPLICATE,
    RTREE_SEARCH,
    RUNNING_AGGREGATE,
    SINGLE_PARTITION_INVERTED_INDEX_SEARCH,
    SINK,
    SINK_WRITE,
    SORT_GROUP_BY,
    SORT_MERGE_EXCHANGE,
    SPLIT,
    STABLE_SORT,
    STATS,
    STREAM_LIMIT,
    STREAM_PROJECT,
    STREAM_SELECT,
    STRING_STREAM_SCRIPT,
    SUBPLAN,
    TOKENIZE,
    UNION_ALL,
    UNNEST,
    LEFT_OUTER_UNNEST,
    UPDATE,
    WRITE_RESULT,
    INTERSECT,
    WINDOW,
    WINDOW_STREAM
}
