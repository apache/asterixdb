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

import java.io.DataInputStream;
import java.nio.ByteBuffer;

import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.util.ByteBufferInputStream;

public class RecordIdReader {

    private final static byte MISSING_BYTE = ATypeTag.SERIALIZED_MISSING_TYPE_TAG;
    protected FrameTupleAccessor tupleAccessor;
    protected int fieldSlotsLength;
    protected int[] ridFields;
    protected RecordId rid;
    protected RecordDescriptor inRecDesc;
    protected ByteBufferInputStream bbis;
    protected DataInputStream dis;
    protected int tupleStartOffset;
    protected ByteBuffer frameBuffer;

    public RecordIdReader(int[] ridFields) {
        this.ridFields = ridFields;
        this.rid = new RecordId();
    }

    public void set(FrameTupleAccessor accessor, RecordDescriptor inRecDesc) {
        this.tupleAccessor = accessor;
        this.fieldSlotsLength = accessor.getFieldSlotsLength();
        this.inRecDesc = inRecDesc;
        this.bbis = new ByteBufferInputStream();
        this.dis = new DataInputStream(bbis);
    }

    public RecordId read(int index) throws HyracksDataException {
        tupleStartOffset = tupleAccessor.getTupleStartOffset(index) + fieldSlotsLength;
        int fileNumberStartOffset =
                tupleAccessor.getFieldStartOffset(index, ridFields[IndexingConstants.FILE_NUMBER_FIELD_INDEX]);
        frameBuffer = tupleAccessor.getBuffer();
        if (frameBuffer.get(tupleStartOffset + fileNumberStartOffset) == MISSING_BYTE) {
            return null;
        }
        // Get file number
        bbis.setByteBuffer(frameBuffer, tupleStartOffset + fileNumberStartOffset);
        rid.setFileId(
                ((AInt32) inRecDesc.getFields()[ridFields[IndexingConstants.FILE_NUMBER_FIELD_INDEX]].deserialize(dis))
                        .getIntegerValue());
        // Get record group offset
        bbis.setByteBuffer(frameBuffer, tupleStartOffset
                + tupleAccessor.getFieldStartOffset(index, ridFields[IndexingConstants.RECORD_OFFSET_FIELD_INDEX]));
        rid.setOffset(((AInt64) inRecDesc.getFields()[ridFields[IndexingConstants.RECORD_OFFSET_FIELD_INDEX]]
                .deserialize(dis)).getLongValue());
        return rid;
    }
}
