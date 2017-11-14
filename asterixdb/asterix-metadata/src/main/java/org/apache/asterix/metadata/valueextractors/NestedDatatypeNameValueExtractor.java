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
import java.io.IOException;

import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.metadata.api.IValueExtractor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.util.string.UTF8StringReader;

/**
 * Extracts the value of field 'DataypeName' of the first nested type from an
 * ITupleReference that contains a serialized representation of a Datatype
 * metadata entity.
 *
 * TODO Is this class used?
 */
public class NestedDatatypeNameValueExtractor implements IValueExtractor<String> {

    private final String datatypeName;

    public NestedDatatypeNameValueExtractor(String datatypeName) {
        this.datatypeName = datatypeName;
    }

    private final UTF8StringReader reader = new UTF8StringReader();

    @Override
    public String getValue(TxnId txnId, ITupleReference tuple) throws AlgebricksException, HyracksDataException {
        byte[] serRecord = tuple.getFieldData(2);
        int recordStartOffset = tuple.getFieldStart(2);
        int recordLength = tuple.getFieldLength(2);
        ByteArrayInputStream stream = new ByteArrayInputStream(serRecord, recordStartOffset, recordLength);
        DataInput in = new DataInputStream(stream);
        try {
            String nestedType = reader.readUTF(in);
            if (nestedType.equals(datatypeName)) {
                recordStartOffset = tuple.getFieldStart(1);
                recordLength = tuple.getFieldLength(1);
                stream = new ByteArrayInputStream(serRecord, recordStartOffset, recordLength);
                in = new DataInputStream(stream);
                return reader.readUTF(in);
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
        return null;
    }
}
