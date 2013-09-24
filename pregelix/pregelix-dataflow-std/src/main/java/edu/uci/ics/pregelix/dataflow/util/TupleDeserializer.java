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

package edu.uci.ics.pregelix.dataflow.util;

import java.io.DataInputStream;
import java.io.IOException;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

public class TupleDeserializer {
    private static String ERROR_MSG = "Out-of-bound read in your Writable implementations of types for vertex id, vertex value, edge value or message --- check your readFields and write implmenetation";
    private Object[] record;
    private RecordDescriptor recordDescriptor;
    private ResetableByteArrayInputStream bbis;
    private DataInputStream di;

    public TupleDeserializer(RecordDescriptor recordDescriptor) {
        this.recordDescriptor = recordDescriptor;
        this.bbis = new ResetableByteArrayInputStream();
        this.di = new DataInputStream(bbis);
        this.record = new Object[recordDescriptor.getFields().length];
    }

    public Object[] deserializeRecord(ITupleReference tupleRef) throws HyracksDataException {
        try {
            for (int i = 0; i < tupleRef.getFieldCount(); ++i) {
                byte[] data = tupleRef.getFieldData(i);
                int offset = tupleRef.getFieldStart(i);
                int len = tupleRef.getFieldLength(i);
                bbis.setByteArray(data, offset);

                int availableBefore = bbis.available();
                Object instance = recordDescriptor.getFields()[i].deserialize(di);
                int availableAfter = bbis.available();
                if (availableBefore - availableAfter > len) {
                    throw new IllegalStateException(ERROR_MSG);
                }

                record[i] = instance;
            }
            return record;
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    public Object[] deserializeRecord(IFrameTupleAccessor left, int tIndex, ITupleReference right)
            throws HyracksDataException {
        try {
            /** skip vertex id field in deserialization */
            byte[] data = left.getBuffer().array();
            int tStart = left.getTupleStartOffset(tIndex) + left.getFieldSlotsLength();
            int leftFieldCount = left.getFieldCount();
            int fStart = tStart;
            for (int i = 1; i < leftFieldCount; ++i) {
                /**
                 * reset the input
                 */
                fStart = tStart + left.getFieldStartOffset(tIndex, i);
                int fieldLength = left.getFieldLength(tIndex, i);
                bbis.setByteArray(data, fStart);

                /**
                 * do deserialization
                 */
                int availableBefore = bbis.available();
                Object instance = recordDescriptor.getFields()[i].deserialize(di);
                int availableAfter = bbis.available();
                if (availableBefore - availableAfter > fieldLength) {
                    throw new IllegalStateException(ERROR_MSG);

                }
                record[i] = instance;
            }
            /** skip vertex id field in deserialization */
            for (int i = leftFieldCount + 1; i < record.length; ++i) {
                byte[] rightData = right.getFieldData(i - leftFieldCount);
                int rightOffset = right.getFieldStart(i - leftFieldCount);
                int len = right.getFieldLength(i - leftFieldCount);
                bbis.setByteArray(rightData, rightOffset);

                int availableBefore = bbis.available();
                Object instance = recordDescriptor.getFields()[i].deserialize(di);
                int availableAfter = bbis.available();
                if (availableBefore - availableAfter > len) {
                    throw new IllegalStateException(ERROR_MSG);
                }
                record[i] = instance;
            }
            return record;
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    public Object[] deserializeRecord(ArrayTupleBuilder tb, ITupleReference right) throws HyracksDataException {
        try {
            byte[] data = tb.getByteArray();
            int[] offset = tb.getFieldEndOffsets();
            int start = 0;
            /** skip vertex id fields in deserialization */
            for (int i = 1; i < offset.length; ++i) {
                /**
                 * reset the input
                 */
                start = offset[i - 1];
                bbis.setByteArray(data, start);
                int fieldLength = i == 0 ? offset[0] : offset[i] - offset[i - 1];

                /**
                 * do deserialization
                 */
                int availableBefore = bbis.available();
                Object instance = recordDescriptor.getFields()[i].deserialize(di);
                int availableAfter = bbis.available();
                if (availableBefore - availableAfter > fieldLength) {
                    throw new IllegalStateException(ERROR_MSG);
                }
                record[i] = instance;
            }
            /** skip vertex id fields in deserialization */
            for (int i = offset.length + 1; i < record.length; ++i) {
                byte[] rightData = right.getFieldData(i - offset.length);
                int rightOffset = right.getFieldStart(i - offset.length);
                bbis.setByteArray(rightData, rightOffset);
                int fieldLength = right.getFieldLength(i - offset.length);

                int availableBefore = bbis.available();
                Object instance = recordDescriptor.getFields()[i].deserialize(di);
                int availableAfter = bbis.available();
                if (availableBefore - availableAfter > fieldLength) {
                    throw new IllegalStateException(ERROR_MSG);
                }
                record[i] = instance;
            }
            return record;
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }
}
