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
package org.apache.asterix.lang.common.base;

public interface Clause extends ILangExpression {
    public ClauseType getClauseType();

    public enum ClauseType {
        FOR_CLAUSE,
        LET_CLAUSE,
        WHERE_CLAUSE,
        GROUP_BY_CLAUSE,
        DISTINCT_BY_CLAUSE,
        ORDER_BY_CLAUSE,
        LIMIT_CLAUSE,
        UPDATE_CLAUSE,

        // SQL related clause
        FROM_CLAUSE,
        FROM_TERM,
        HAVING_CLAUSE,
        JOIN_CLAUSE,
        NEST_CLAUSE,
        PROJECTION,
        SELECT_BLOCK,
        SELECT_CLAUSE,
        SELECT_ELEMENT,
        SELECT_REGULAR,
        SELECT_SET_OPERATION,
        UNNEST_CLAUSE
    }

}
