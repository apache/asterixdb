/*
 * Copyright 2009-2010 by The Regents of the University of California
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
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameConstants;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

public class TupleDeserializer {
    private static final Logger LOGGER = Logger.getLogger(TupleDeserializer.class.getName());

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
        for (int i = 0; i < tupleRef.getFieldCount(); ++i) {
            byte[] data = tupleRef.getFieldData(i);
            int offset = tupleRef.getFieldStart(i);
            bbis.setByteArray(data, offset);

            Object instance = recordDescriptor.getFields()[i].deserialize(di);
            if (LOGGER.isLoggable(Level.FINEST)) {
                LOGGER.finest(i + " " + instance);
            }
            record[i] = instance;
            if (FrameConstants.DEBUG_FRAME_IO) {
                try {
                    if (di.readInt() != FrameConstants.FRAME_FIELD_MAGIC) {
                        throw new HyracksDataException("Field magic mismatch");
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return record;
    }

    public Object[] deserializeRecord(IFrameTupleAccessor left, int tIndex, ITupleReference right)
            throws HyracksDataException {
        byte[] data = left.getBuffer().array();
        int tStart = left.getTupleStartOffset(tIndex) + left.getFieldSlotsLength();
        int leftFieldCount = left.getFieldCount();
        int fStart = tStart;
        for (int i = 0; i < leftFieldCount; ++i) {
            /**
             * reset the input
             */
            fStart = tStart + left.getFieldStartOffset(tIndex, i);
            bbis.setByteArray(data, fStart);

            /**
             * do deserialization
             */
            Object instance = recordDescriptor.getFields()[i].deserialize(di);
            if (LOGGER.isLoggable(Level.FINEST)) {
                LOGGER.finest(i + " " + instance);
            }
            record[i] = instance;
            if (FrameConstants.DEBUG_FRAME_IO) {
                try {
                    if (di.readInt() != FrameConstants.FRAME_FIELD_MAGIC) {
                        throw new HyracksDataException("Field magic mismatch");
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        for (int i = leftFieldCount; i < record.length; ++i) {
            byte[] rightData = right.getFieldData(i - leftFieldCount);
            int rightOffset = right.getFieldStart(i - leftFieldCount);
            bbis.setByteArray(rightData, rightOffset);

            Object instance = recordDescriptor.getFields()[i].deserialize(di);
            if (LOGGER.isLoggable(Level.FINEST)) {
                LOGGER.finest(i + " " + instance);
            }
            record[i] = instance;
            if (FrameConstants.DEBUG_FRAME_IO) {
                try {
                    if (di.readInt() != FrameConstants.FRAME_FIELD_MAGIC) {
                        throw new HyracksDataException("Field magic mismatch");
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return record;
    }

    public Object[] deserializeRecord(ArrayTupleBuilder tb, ITupleReference right) throws HyracksDataException {
        byte[] data = tb.getByteArray();
        int[] offset = tb.getFieldEndOffsets();
        int start = 0;
        for (int i = 0; i < offset.length; ++i) {
            /**
             * reset the input
             */
            bbis.setByteArray(data, start);
            start = offset[i];

            /**
             * do deserialization
             */
            Object instance = recordDescriptor.getFields()[i].deserialize(di);
            if (LOGGER.isLoggable(Level.FINEST)) {
                LOGGER.finest(i + " " + instance);
            }
            record[i] = instance;
            if (FrameConstants.DEBUG_FRAME_IO) {
                try {
                    if (di.readInt() != FrameConstants.FRAME_FIELD_MAGIC) {
                        throw new HyracksDataException("Field magic mismatch");
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        for (int i = offset.length; i < record.length; ++i) {
            byte[] rightData = right.getFieldData(i - offset.length);
            int rightOffset = right.getFieldStart(i - offset.length);
            bbis.setByteArray(rightData, rightOffset);

            Object instance = recordDescriptor.getFields()[i].deserialize(di);
            if (LOGGER.isLoggable(Level.FINEST)) {
                LOGGER.finest(i + " " + instance);
            }
            record[i] = instance;
            if (FrameConstants.DEBUG_FRAME_IO) {
                try {
                    if (di.readInt() != FrameConstants.FRAME_FIELD_MAGIC) {
                        throw new HyracksDataException("Field magic mismatch");
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return record;
    }
}
