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
package org.apache.asterix.external.indexing;

import java.io.IOException;

import org.apache.asterix.external.api.IExternalIndexer;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.input.record.reader.HDFSRecordReader;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;

public class FileOffsetIndexer implements IExternalIndexer {

    private static final long serialVersionUID = 1L;
    public static final int NUM_OF_FIELDS = 2;
    protected AMutableInt32 fileNumber = new AMutableInt32(0);
    protected AMutableInt64 offset = new AMutableInt64(0);
    protected RecordReader<?, Writable> recordReader;

    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<IAObject> intSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.AINT32);
    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<IAObject> longSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.AINT64);

    @Override
    public void reset(IRecordReader<?> reader) throws IOException {
        //TODO: Make it more generic since we can't assume it is always going to be HDFS records.
        @SuppressWarnings("unchecked")
        HDFSRecordReader<?, Writable> hdfsReader = (HDFSRecordReader<?, Writable>) reader;
        fileNumber.setValue(hdfsReader.getSnapshot().get(hdfsReader.getCurrentSplitIndex()).getFileNumber());
        recordReader = hdfsReader.getReader();
        offset.setValue(recordReader.getPos());
    }

    @Override
    public void index(ArrayTupleBuilder tb) throws IOException {
        tb.addField(intSerde, fileNumber);
        tb.addField(longSerde, offset);
        // Get position for next index(tb) call
        offset.setValue(recordReader.getPos());
    }

    @Override
    public int getNumberOfFields() {
        return NUM_OF_FIELDS;
    }

}
