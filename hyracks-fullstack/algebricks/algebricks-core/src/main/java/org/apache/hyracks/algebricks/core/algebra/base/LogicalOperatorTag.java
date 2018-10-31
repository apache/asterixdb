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

public enum LogicalOperatorTag {
    AGGREGATE,
    ASSIGN,
    DATASOURCESCAN,
    DISTINCT,
    DISTRIBUTE_RESULT,
    EMPTYTUPLESOURCE,
    EXCHANGE,
    DELEGATE_OPERATOR,
    EXTERNAL_LOOKUP,
    FORWARD,
    GROUP,
    INDEX_INSERT_DELETE_UPSERT,
    INNERJOIN,
    INSERT_DELETE_UPSERT,
    LEFTOUTERJOIN,
    LEFT_OUTER_UNNEST_MAP,
    LIMIT,
    MATERIALIZE,
    NESTEDTUPLESOURCE,
    ORDER,
    PROJECT,
    REPLICATE,
    RUNNINGAGGREGATE,
    SCRIPT,
    SELECT,
    SINK,
    SPLIT,
    SUBPLAN,
    TOKENIZE,
    UNIONALL,
    UNNEST,
    LEFT_OUTER_UNNEST,
    UNNEST_MAP,
    UPDATE,
    WRITE,
    WRITE_RESULT,
    INTERSECT,
    WINDOW
}
