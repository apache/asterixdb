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
package org.apache.asterix.om.lazy.metadata;

import org.apache.asterix.om.api.IRowMetadata;
import org.apache.asterix.om.types.ARecordType;

public abstract class AbstractRowMetadata implements IRowMetadata {
    protected static final int WRITERS_POINTER = 0;
    protected static final int FIELD_NAMES_POINTER = WRITERS_POINTER + Integer.BYTES;
    protected static final int SCHEMA_POINTER = FIELD_NAMES_POINTER + Integer.BYTES;
    protected static final int META_SCHEMA_POINTER = SCHEMA_POINTER + Integer.BYTES;
    protected static final int PATH_INFO_POINTER = META_SCHEMA_POINTER + Integer.BYTES;
    protected static final int OFFSETS_SIZE = PATH_INFO_POINTER + Integer.BYTES;
//    private final ARecordType datasetType;
//    private final ARecordType metaType;

//    private final int numberOfPrimaryKeys;
    private final int recordFieldIndex;

    protected AbstractRowMetadata() {
//        this.datasetType = datasetType;
//        this.metaType = metaType;
//        this.numberOfPrimaryKeys = numberOfPrimaryKeys;
        this.recordFieldIndex = 0;
    }

//    public final ARecordType getDatasetType() {
//        return datasetType;
//    }
//
//    public final ARecordType getMetaType() {
//        return metaType;
//    }
//
//    public final int getNumberOfPrimaryKeys() {
//        return numberOfPrimaryKeys;
//    }
//
    public final int getRecordFieldIndex() {
        return recordFieldIndex;
    }

//    public final int getMetaRecordFieldIndex() {
//        return recordFieldIndex + 1;
//    }

    public abstract int getNumberOfColumns();
}