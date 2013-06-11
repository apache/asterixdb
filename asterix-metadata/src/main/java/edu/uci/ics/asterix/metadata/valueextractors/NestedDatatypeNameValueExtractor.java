/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.asterix.metadata.valueextractors;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;

import edu.uci.ics.asterix.common.transactions.JobId;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AObjectSerializerDeserializer;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.api.IValueExtractor;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Extracts the value of field 'DataypeName' of the first nested type from an
 * ITupleReference that contains a serialized representation of a Datatype
 * metadata entity.
 */
public class NestedDatatypeNameValueExtractor implements IValueExtractor<String> {

    private final String datatypeName;

    public NestedDatatypeNameValueExtractor(String datatypeName) {
        this.datatypeName = datatypeName;
    }

    @Override
    public String getValue(JobId jobId, ITupleReference tuple) throws MetadataException, HyracksDataException {
        byte[] serRecord = tuple.getFieldData(2);
        int recordStartOffset = tuple.getFieldStart(2);
        int recordLength = tuple.getFieldLength(2);
        ByteArrayInputStream stream = new ByteArrayInputStream(serRecord, recordStartOffset, recordLength);
        DataInput in = new DataInputStream(stream);
        String nestedType = ((AString) AObjectSerializerDeserializer.INSTANCE.deserialize(in)).getStringValue();
        if (nestedType.equals(datatypeName)) {
            recordStartOffset = tuple.getFieldStart(1);
            recordLength = tuple.getFieldLength(1);
            stream = new ByteArrayInputStream(serRecord, recordStartOffset, recordLength);
            in = new DataInputStream(stream);
            return ((AString) AObjectSerializerDeserializer.INSTANCE.deserialize(in)).getStringValue();
        }
        return null;
    }
}
