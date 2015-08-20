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

public enum LogicalOperatorTag {
    AGGREGATE,
    ASSIGN,
    DATASOURCESCAN,
    DISTINCT,
    DISTRIBUTE_RESULT,
    EMPTYTUPLESOURCE,
    EXCHANGE,
    EXTENSION_OPERATOR,
    EXTERNAL_LOOKUP,
    GROUP,
    INDEX_INSERT_DELETE,
    INNERJOIN,
    INSERT_DELETE,
    LEFTOUTERJOIN,
    LIMIT,
    MATERIALIZE,
    NESTEDTUPLESOURCE,
    ORDER,
    PARTITIONINGSPLIT,
    PROJECT,
    REPLICATE,
    RUNNINGAGGREGATE,
    SCRIPT,
    SELECT,
    SINK,
    SUBPLAN,
    TOKENIZE,
    UNIONALL,
    UNNEST,
    UNNEST_MAP,
    UPDATE,
    WRITE,
    WRITE_RESULT,
}