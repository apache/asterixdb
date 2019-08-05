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

package org.apache.asterix.lang.sqlpp.parser;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public enum SqlppHint {

    // optimizer hints
    AUTO_HINT("auto"),
    BROADCAST_JOIN_HINT("bcast"),
    COMPOSE_VAL_FILES_HINT("compose-val-files"),
    DATE_BETWEEN_YEARS_HINT("date-between-years"),
    DATETIME_ADD_RAND_HOURS_HINT("datetime-add-rand-hours"),
    DATETIME_BETWEEN_YEARS_HINT("datetime-between-years"),
    HASH_GROUP_BY_HINT("hash"),
    INDEXED_NESTED_LOOP_JOIN_HINT("indexnl"),
    INMEMORY_HINT("inmem"),
    INSERT_RAND_INT_HINT("insert-rand-int"),
    INTERVAL_HINT("interval"),
    LIST_HINT("list"),
    LIST_VAL_FILE_HINT("list-val-file"),
    RANGE_HINT("range"),
    SKIP_SECONDARY_INDEX_SEARCH_HINT("skip-index"),
    VAL_FILE_HINT("val-files"),
    VAL_FILE_SAME_INDEX_HINT("val-file-same-idx"),
    GEN_FIELDS_HINT("gen-fields"),

    // data generator hints
    DGEN_HINT("dgen");

    private static final Map<String, SqlppHint> ID_MAP = createIdentifierMap(values());

    private final String id;

    SqlppHint(String id) {
        Objects.requireNonNull(id);
        this.id = id;
    }

    public String getIdentifier() {
        return id;
    }

    @Override
    public String toString() {
        return getIdentifier();
    }

    public static SqlppHint findByIdentifier(String id) {
        return ID_MAP.get(id);
    }

    private static Map<String, SqlppHint> createIdentifierMap(SqlppHint[] values) {
        Map<String, SqlppHint> map = new HashMap<>();
        for (SqlppHint hint : values) {
            map.put(hint.getIdentifier(), hint);
        }
        return map;
    }

    public static int findParamStart(String str) {
        for (int i = 0, ln = str.length(); i < ln; i++) {
            if (!isIdentifierChar(str.charAt(i))) {
                return i;
            }
        }
        return -1;
    }

    public static boolean isIdentifierChar(char c) {
        return Character.isJavaIdentifierStart(c) || c == '-';
    }
}
