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
package org.apache.hyracks.dataflow.common.comm.io;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hyracks.api.comm.FrameConstants;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.GrowableArray;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Array backed tuple builder.
 *
 * @deprecated Use IFrameFieldAppender.appendField to append fields directly.
 */
@Deprecated
public class ArrayTupleBuilder implements IDataOutputProvider {
    private final GrowableArray fieldData = new GrowableArray();
    private final int[] fEndOffsets;
    private int nextField;

    public ArrayTupleBuilder(int nFields) {
        fEndOffsets = new int[nFields];
    }

    /**
     * Resets the builder.
     * reset() must be called before attempting to create a new tuple.
     */
    public void reset() {
        nextField = 0;
        fieldData.reset();
    }

    /**
     * Get the end offsets of the fields in this tuple.
     *
     * @return end offsets of the fields.
     */
    public int[] getFieldEndOffsets() {
        return fEndOffsets;
    }

    /**
     * Get the data area in this builder.
     *
     * @return Data byte array.
     */
    public byte[] getByteArray() {
        return fieldData.getByteArray();
    }

    /**
     * Get the size of the data area.
     *
     * @return data area size.
     */
    public int getSize() {
        return fieldData.getLength();
    }

    /**
     * Add a field to the tuple from a field in a frame.
     *
     * @param accessor
     *            - Frame that contains the field to be copied into the tuple
     *            builder.
     * @param tIndex
     *            - Tuple index of the tuple that contains the field to be
     *            copied.
     * @param fIndex
     *            - Field index of the field to be copied.
     * @throws HyracksDataException
     */
    public void addField(IFrameTupleAccessor accessor, int tIndex, int fIndex) throws HyracksDataException {
        int startOffset = accessor.getTupleStartOffset(tIndex);
        int fStartOffset = accessor.getFieldStartOffset(tIndex, fIndex);
        int fLen = accessor.getFieldEndOffset(tIndex, fIndex) - fStartOffset;
        try {
            fieldData.getDataOutput().write(accessor.getBuffer().array(),
                    startOffset + accessor.getFieldSlotsLength() + fStartOffset, fLen);
            if (FrameConstants.DEBUG_FRAME_IO) {
                fieldData.getDataOutput().writeInt(FrameConstants.FRAME_FIELD_MAGIC);
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
        fEndOffsets[nextField++] = fieldData.getLength();
    }

    /**
     * Add a field to the tuple from a field in another tuple
     *
     * @param accessor
     *            - Tuple that contains the field to be copied into the tuple
     *            builder.
     * @param fIndex
     *            - Field index of the field to be copied.
     * @throws HyracksDataException
     */
    public void addField(ITupleReference accessor, int fIndex) throws HyracksDataException {
        addField(accessor.getFieldData(fIndex), accessor.getFieldStart(fIndex), accessor.getFieldLength(fIndex));
    }

    /**
     * Add a field to the tuple by serializing the given object using the given
     * serializer.
     *
     * @param serDeser
     *            - Serializer
     * @param instance
     *            - Object to serialize
     * @throws HyracksDataException
     */
    public <T> void addField(ISerializerDeserializer<T> serDeser, T instance) throws HyracksDataException {
        serDeser.serialize(instance, fieldData.getDataOutput());
        fEndOffsets[nextField++] = fieldData.getLength();
    }

    /**
     * Add a field to the tuple by copying the data bytes from a byte array.
     *
     * @param bytes
     *            - Byte array to copy the field data from.
     * @param start
     *            - Start offset of the field to be copied in the byte array.
     * @param length
     *            - Length of the field to be copied.
     * @throws HyracksDataException
     */
    public void addField(byte[] bytes, int start, int length) throws HyracksDataException {
        try {
            fieldData.getDataOutput().write(bytes, start, length);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
        fEndOffsets[nextField++] = fieldData.getLength();
    }

    /**
     * Get the {@link DataOutput} interface to write into the tuple builder.
     */
    @Override
    public DataOutput getDataOutput() {
        return fieldData.getDataOutput();
    }

    /**
     * Get the growable array storing the field data.
     */
    public GrowableArray getFieldData() {
        return fieldData;
    }

    /**
     * Sets the byte offset of the end of the field into the field offset array.
     * Make sure this method is called when the {@link DataOutput} interface is
     * used to add field data into the tuple, at the end of adding a field's
     * data.
     */
    public void addFieldEndOffset() {
        fEndOffsets[nextField++] = fieldData.getLength();
    }

    /**
     * Adds a new field and fills it with the content of the passed value
     */
    public void addField(IValueReference data) throws HyracksDataException {
        addField(data.getByteArray(), data.getStartOffset(), data.getLength());
    }
}
