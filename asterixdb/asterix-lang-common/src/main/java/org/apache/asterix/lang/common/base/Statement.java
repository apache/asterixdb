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
     *  get a byte representing the statement category.
     *  Each category describes the type of modifications this statement does.
     *
     * @return kind byte
     */
    byte getCategory();

    class Category {
        /** no modifications */
        public static final byte QUERY = 0x01;
        /** modify data */
        public static final byte UPDATE = 0x02;
        /** modify metadata */
        public static final byte DDL = 0x04;
        /** modify anything */
        public static final byte PROCEDURE = 0x08;

        private Category() {
        }
    }

    enum Kind {
        DATASET_DECL,
        DATAVERSE_DECL,
        DATAVERSE_DROP,
        DATASET_DROP,
        DELETE,
        INSERT,
        UPSERT,
        UPDATE,
        FUNCTION_DECL,
        LOAD,
        NODEGROUP_DECL,
        NODEGROUP_DROP,
        QUERY,
        SET,
        TYPE_DECL,
        TYPE_DROP,
        WRITE,
        CREATE_INDEX,
        CREATE_DATAVERSE,
        INDEX_DROP,
        CREATE_FEED,
        DROP_FEED,
        START_FEED,
        STOP_FEED,
        CONNECT_FEED,
        DISCONNECT_FEED,
        CREATE_FEED_POLICY,
        DROP_FEED_POLICY,
        CREATE_FUNCTION,
        FUNCTION_DROP,
        COMPACT,
        EXTERNAL_DATASET_REFRESH,
        SUBSCRIBE_FEED,
        EXTENSION,
    }
}
