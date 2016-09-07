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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public interface Statement extends ILangExpression {
    public static final List<Byte> KINDS = Collections.unmodifiableList(Kind.range(Kind.DATASET_DECL, Kind.RUN));

    /**
     * get a byte representing the statement kind
     * Note: bytes 0x00 - 0x7f are reserved for core asterix statements
     * Use negative bytes for extension statements
     *
     * @return kind byte
     */
    public byte getKind();

    /**
     *  get a byte representing the statement category.
     *  Each category describes the type of modifications this statement does.
     *
     * @return kind byte
     */
    public byte getCategory();

    public class Category {
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

    public class Kind {
        public static final byte DATASET_DECL = 0x00;
        public static final byte DATAVERSE_DECL = 0x01;
        public static final byte DATAVERSE_DROP = 0x02;
        public static final byte DATASET_DROP = 0x03;
        public static final byte DELETE = 0x04;
        public static final byte INSERT = 0x05;
        public static final byte UPSERT = 0x06;
        public static final byte UPDATE = 0x07;
        public static final byte DML_CMD_LIST = 0x08;
        public static final byte FUNCTION_DECL = 0x09;
        public static final byte LOAD = 0x0a;
        public static final byte NODEGROUP_DECL = 0x0b;
        public static final byte NODEGROUP_DROP = 0x0c;
        public static final byte QUERY = 0x0d;
        public static final byte SET = 0x0e;
        public static final byte TYPE_DECL = 0x0f;
        public static final byte TYPE_DROP = 0x10;
        public static final byte WRITE = 0x11;
        public static final byte CREATE_INDEX = 0x12;
        public static final byte INDEX_DECL = 0x13;
        public static final byte CREATE_DATAVERSE = 0x14;
        public static final byte INDEX_DROP = 0x15;
        public static final byte CREATE_PRIMARY_FEED = 0x16;
        public static final byte CREATE_SECONDARY_FEED = 0x17;
        public static final byte DROP_FEED = 0x18;
        public static final byte CONNECT_FEED = 0x19;
        public static final byte DISCONNECT_FEED = 0x1a;
        public static final byte SUBSCRIBE_FEED = 0x1b;
        public static final byte CREATE_FEED_POLICY = 0x1c;
        public static final byte DROP_FEED_POLICY = 0x1d;
        public static final byte CREATE_FUNCTION = 0x1e;
        public static final byte FUNCTION_DROP = 0x1f;
        public static final byte COMPACT = 0x20;
        public static final byte EXTERNAL_DATASET_REFRESH = 0x21;
        public static final byte RUN = 0x22;
        public static final byte EXTENSION = 0x23;

        private Kind() {
        }

        /**
         * Generate a list of Bytes from start to end
         *
         * @param start
         * @param end
         * @return
         */
        private static List<Byte> range(byte start, byte end) {
            ArrayList<Byte> bytes = new ArrayList<>();
            for (byte b = start; b <= end; b++) {
                bytes.add(b);
            }
            return bytes;
        }
    }
}
