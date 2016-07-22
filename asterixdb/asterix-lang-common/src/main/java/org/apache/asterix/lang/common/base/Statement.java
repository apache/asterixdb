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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public abstract class Statement implements ILangExpression {
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
    public static final List<Byte> VALUES = Collections.unmodifiableList(
            Arrays.asList(DATASET_DECL, DATAVERSE_DECL, DATAVERSE_DROP, DATASET_DROP, DELETE, INSERT, UPSERT, UPDATE,
                    DML_CMD_LIST, FUNCTION_DECL, LOAD, NODEGROUP_DECL, NODEGROUP_DROP, QUERY, SET, TYPE_DECL, TYPE_DROP,
                    WRITE, CREATE_INDEX, INDEX_DECL, CREATE_DATAVERSE, INDEX_DROP, CREATE_PRIMARY_FEED,
                    CREATE_SECONDARY_FEED, DROP_FEED, CONNECT_FEED, DISCONNECT_FEED, SUBSCRIBE_FEED, CREATE_FEED_POLICY,
                    DROP_FEED_POLICY, CREATE_FUNCTION, FUNCTION_DROP, COMPACT, EXTERNAL_DATASET_REFRESH, RUN));

    public abstract byte getKind();
}
