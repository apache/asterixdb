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
package edu.uci.ics.hyracks.comm.io;

import java.io.DataOutputStream;
import java.io.IOException;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class ArrayTupleBuilder {
    private final ByteArrayAccessibleOutputStream baaos;
    private final DataOutputStream dos;
    private final int[] fEndOffsets;
    private int nextField;

    public ArrayTupleBuilder(int nFields) {
        baaos = new ByteArrayAccessibleOutputStream();
        dos = new DataOutputStream(baaos);
        fEndOffsets = new int[nFields];
    }

    public void reset() {
        nextField = 0;
        baaos.reset();
    }

    public int[] getFieldEndOffsets() {
        return fEndOffsets;
    }

    public byte[] getByteArray() {
        return baaos.getByteArray();
    }

    public int getSize() {
        return baaos.size();
    }

    public void addField(FrameTupleAccessor accessor, int tIndex, int fIndex) throws HyracksDataException {
        int startOffset = accessor.getTupleStartOffset(tIndex);
        int fStartOffset = accessor.getFieldStartOffset(tIndex, fIndex);
        int fLen = accessor.getFieldEndOffset(tIndex, fIndex) - fStartOffset;
        try {
            dos.write(accessor.getBuffer().array(), startOffset + accessor.getFieldSlotsLength() + fStartOffset, fLen);
            if (FrameConstants.DEBUG_FRAME_IO) {
                dos.writeInt(FrameConstants.FRAME_FIELD_MAGIC);
            }
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
        fEndOffsets[nextField++] = baaos.size();
    }

    public <T> void addField(ISerializerDeserializer<T> serDeser, T instance) throws HyracksDataException {
        serDeser.serialize(instance, dos);
        fEndOffsets[nextField++] = baaos.size();
    }
}