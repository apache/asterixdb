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

public interface Statement extends ILangExpression {
    /**
     * The statement kind.
     *
     * @return kind
     */
    Kind getKind();

    /**
     * get a byte representing the statement category.
     * Each category describes the type of modifications this statement does.
     *
     * @return kind byte
     */
    byte getCategory();

    class Category {
        /**
         * no modifications
         */
        public static final byte QUERY = 0x01;
        /**
         * modify data
         */
        public static final byte UPDATE = 0x02;
        /**
         * modify metadata
         */
        public static final byte DDL = 0x04;
        /**
         * modify anything
         */
        public static final byte PROCEDURE = 0x08;

        private Category() {
        }

        public static String toString(byte category) {
            switch (category) {
                case QUERY:
                    return "query";
                case UPDATE:
                    return "update";
                case DDL:
                    return "ddl";
                case PROCEDURE:
                    return "procedure";
                default:
                    throw new IllegalArgumentException(String.valueOf(category));
            }
        }
    }

    enum Kind {
        SET("SET", true),
        FUNCTION_DECL("DECLARE FUNCTION", false),
        VIEW_DECL("DECLARE VIEW", false),
        DATAVERSE_DECL("USE", true),
        CREATE_DATABASE("CREATE DATABASE", false),
        DATABASE_DROP("DROP DATABASE", false),
        CREATE_DATAVERSE("CREATE SCOPE", false),
        DATAVERSE_DROP("DROP SCOPE", false),
        CREATE_DATASET("CREATE COLLECTION", false),
        DATASET_DROP("DROP COLLECTION", false),
        CREATE_INDEX("CREATE INDEX", false),
        INDEX_DROP("DROP INDEX", false),
        CREATE_VIEW("CREATE VIEW", false),
        VIEW_DROP("DROP VIEW", false),
        INSERT("INSERT", false),
        UPSERT("UPSERT", false),
        UPDATE("UPDATE", false),
        DELETE("DELETE", false),
        TRUNCATE("TRUNCATE COLLECTION", false),
        LOAD("LOAD", false),
        QUERY("QUERY", true),
        CREATE_NODEGROUP("CREATE NODEGROUP", false),
        NODEGROUP_DROP("DROP NODEGROUP", false),
        CREATE_TYPE("CREATE TYPE", false),
        TYPE_DROP("DROP TYPE", false),
        CREATE_FULL_TEXT_FILTER("CREATE FULLTEXT FILTER", false),
        CREATE_FULL_TEXT_CONFIG("CREATE FULLTEXT CONFIG", false),
        FULL_TEXT_FILTER_DROP("DROP FULLTEXT FILTER", false),
        FULL_TEXT_CONFIG_DROP("DROP FULLTEXT CONFIG", false),
        CREATE_FEED("CREATE FEED", false),
        DROP_FEED("DROP FEED", false),
        START_FEED("START FEED", false),
        STOP_FEED("STOP FEED", false),
        CONNECT_FEED("CONNECT FEED", false),
        DISCONNECT_FEED("DISCONNECT FEED", false),
        CREATE_FEED_POLICY("CREATE INGESTION POLICY", false),
        DROP_FEED_POLICY("DROP INGESTION POLICY", false),
        CREATE_FUNCTION("CREATE FUNCTION", false),
        FUNCTION_DROP("DROP FUNCTION", false),
        CREATE_ADAPTER("CREATE ADAPTER", false),
        ADAPTER_DROP("DROP ADAPTER", false),
        CREATE_LIBRARY("CREATE LIBRARY", false),
        LIBRARY_DROP("DROP LIBRARY", false),
        CREATE_SYNONYM("CREATE SYNONYM", false),
        SYNONYM_DROP("DROP SYNONYM", false),
        ANALYZE("ANALYZE", false),
        ANALYZE_DROP("ANALYZE DROP", false),
        COMPACT("COMPACT", false),
        SUBSCRIBE_FEED("SUBSCRIBE FEED", false),
        EXTENSION("EXTENSION", false),
        COPY_FROM("COPY FROM", false),
        COPY_TO("COPY TO", true),
        CATALOG_CREATE("CATALOG CREATE", false),
        CATALOG_DROP("CATALOG DROP", false),
        CRS_CREATE("CRS_CREATE", false),
        CRS_DROP("CRS_DROP", false);

        private final String displayName;
        private final boolean isSupportAsync;

        Kind(String displayName, boolean isSupportAsync) {
            this.displayName = displayName;
            this.isSupportAsync = isSupportAsync;
        }

        public String getDisplayName() {
            return displayName;
        }

        public boolean isSupportAsync() {
            return isSupportAsync;
        }
    }
}
