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
package org.apache.asterix.external.writer.printer.parquet;

import java.util.Map;

import org.apache.asterix.external.input.record.reader.hdfs.parquet.AsterixParquetRuntimeException;
import org.apache.asterix.om.types.IAType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

public class ObjectWriteSupport extends WriteSupport<IValueReference> {
    private MessageType schema;

    private RecordConsumer recordConsumer;
    private Map<String, String> extraMetaData;
    ParquetRecordLazyVisitor parquetRecordLazyVisitor;

    public ObjectWriteSupport(MessageType schema, IAType typeInfo, Map<String, String> extraMetaData) {
        this.schema = schema;
        this.extraMetaData = extraMetaData;
        parquetRecordLazyVisitor = new ParquetRecordLazyVisitor(schema, typeInfo);
    }

    public String getName() {
        return "asterix";
    }

    public WriteSupport.WriteContext init(Configuration configuration) {
        return new WriteSupport.WriteContext(this.schema, this.extraMetaData);
    }

    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.recordConsumer = recordConsumer;
    }

    @Override
    public void write(IValueReference valueReference) {
        try {
            parquetRecordLazyVisitor.consumeRecord(valueReference, recordConsumer);
        } catch (HyracksDataException e) {
            throw new AsterixParquetRuntimeException(e);
        }
    }

}
