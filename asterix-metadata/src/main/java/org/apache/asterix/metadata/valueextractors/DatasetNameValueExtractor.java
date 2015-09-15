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

package org.apache.asterix.metadata.valueextractors;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;

import org.apache.asterix.common.transactions.JobId;
import org.apache.asterix.dataflow.data.nontagged.serde.AObjectSerializerDeserializer;
import org.apache.asterix.metadata.MetadataException;
import org.apache.asterix.metadata.api.IValueExtractor;
import org.apache.asterix.om.base.AString;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Extracts the value of field 'DatasetName' from an ITupleReference that
 * contains a serialized representation of a Dataset metadata entity.
 */
public class DatasetNameValueExtractor implements IValueExtractor<String> {
    @Override
    public String getValue(JobId jobId, ITupleReference tuple) throws MetadataException, HyracksDataException {
        byte[] serRecord = tuple.getFieldData(2);
        int recordStartOffset = tuple.getFieldStart(2);
        int recordLength = tuple.getFieldLength(2);
        ByteArrayInputStream stream = new ByteArrayInputStream(serRecord, recordStartOffset, recordLength);
        DataInput in = new DataInputStream(stream);
        return (((AString) AObjectSerializerDeserializer.INSTANCE.deserialize(in)).getStringValue());
    }
}
