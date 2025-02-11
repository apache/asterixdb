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
package org.apache.asterix.metadata.declared;

import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.algebricks.core.algebra.metadata.IWriteDataSink;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class WriteDataSink implements IExternalWriteDataSink {
    private final String adapterName;
    private final Map<String, String> configuration;
    private final ARecordType itemType;
    private final ARecordType parquetSchema;
    private final SourceLocation sourceLoc;

    public WriteDataSink(String adapterName, Map<String, String> configuration, ARecordType itemType,
            ARecordType parquetSchema, SourceLocation sourceLoc) {
        this.adapterName = adapterName;
        this.configuration = configuration;
        this.itemType = itemType;
        this.parquetSchema = parquetSchema;
        this.sourceLoc = sourceLoc;
    }

    private WriteDataSink(WriteDataSink writeDataSink) {
        this.adapterName = writeDataSink.getAdapterName();
        this.configuration = new HashMap<>(writeDataSink.configuration);
        this.itemType = writeDataSink.itemType;
        this.parquetSchema = writeDataSink.parquetSchema;
        this.sourceLoc = writeDataSink.sourceLoc;
    }

    @Override
    public ARecordType getItemType() {
        return itemType;
    }

    @Override
    public ARecordType getParquetSchema() {
        return parquetSchema;
    }

    @Override
    public SourceLocation getSourceLoc() {
        return sourceLoc;
    }

    @Override
    public final String getAdapterName() {
        return adapterName;
    }

    @Override
    public final Map<String, String> getConfiguration() {
        return configuration;
    }

    @Override
    public IWriteDataSink createCopy() {
        return new WriteDataSink(this);
    }
}
