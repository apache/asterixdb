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

package org.apache.hyracks.storage.am.vector.frames;

import java.util.Arrays;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.storage.am.btree.frames.OrderedSlotManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.vector.api.IVTreeLeafFrame;
import org.apache.hyracks.storage.am.vector.api.VTreeStaticTupleConstants;

/**
 * VTree leaf frame (bottom of the static structure).
 * <p>
 * Page layout: base header (from {@link VTreeNSMFrame}) followed by a 4-byte next-leaf pointer and a 1-byte
 * overflow flag. Tuples come in two shapes:
 * <ul>
 *   <li>Non-quantized (3 fields): {@code <cid, full_precision_centroid, metadata_page_pointer>}</li>
 *   <li>Quantized (4 fields):     {@code <cid, full_precision_centroid, quantized_bytes, metadata_page_pointer>}</li>
 * </ul>
 * In both cases the metadata-page pointer is the last field.
 */
public class VTreeLeafFrame extends VTreeNSMFrame implements IVTreeLeafFrame {
    // Offset (bytes from page start) of the 4-byte next-leaf pointer.
    protected static final int NEXT_PAGE_OFFSET = CENTROID_ID_OFFSET + Integer.BYTES;
    // Offset of the 1-byte overflow flag (0 = no overflow, non-zero = chained).
    protected static final int OVERFLOW_FLAG_OFFSET = NEXT_PAGE_OFFSET + Integer.BYTES;

    public VTreeLeafFrame(ITreeIndexTupleWriter tupleWriter) {
        super(tupleWriter, new OrderedSlotManager());
    }

    @Override
    public void initBuffer(byte level) {
        super.initBuffer(level);
        buf.putInt(NEXT_PAGE_OFFSET, -1);
        buf.put(OVERFLOW_FLAG_OFFSET, (byte) 0);
    }

    @Override
    public int getPageHeaderSize() {
        return OVERFLOW_FLAG_OFFSET + Byte.BYTES;
    }

    @Override
    public void setNextLeaf(int nextLeafPage) {
        buf.putInt(NEXT_PAGE_OFFSET, nextLeafPage);
    }

    @Override
    public int getNextLeaf() {
        return buf.getInt(NEXT_PAGE_OFFSET);
    }

    @Override
    public void setOverflowFlagBit(boolean overflowFlag) {
        buf.put(OVERFLOW_FLAG_OFFSET, (byte) (overflowFlag ? 1 : 0));
    }

    @Override
    public boolean getOverflowFlagBit() {
        return buf.get(OVERFLOW_FLAG_OFFSET) != 0;
    }

    @Override
    public int getMetadataPagePointer(int tupleIndex) throws HyracksDataException {
        frameTuple.resetByTupleIndex(this, tupleIndex);
        // Metadata page pointer is stored as the last field of the leaf tuple.
        int metadataPtrFieldIndex = frameTuple.getFieldCount() - 1;
        return IntegerPointable.getInteger(frameTuple.getFieldData(metadataPtrFieldIndex),
                frameTuple.getFieldStart(metadataPtrFieldIndex));
    }

    @Override
    public void setMetadataPagePointer(int tupleIndex, int metadataPageId) throws HyracksDataException {
        frameTuple.resetByTupleIndex(this, tupleIndex);
        int metadataPtrFieldIndex = frameTuple.getFieldCount() - 1;
        IntegerPointable.setInteger(frameTuple.getFieldData(metadataPtrFieldIndex),
                frameTuple.getFieldStart(metadataPtrFieldIndex), metadataPageId);
    }

    @Override
    public int getCentroidId(int tupleIndex) throws HyracksDataException {
        frameTuple.resetByTupleIndex(this, tupleIndex);
        // Centroid ID is the first field of the leaf tuple.
        int cidField = VTreeStaticTupleConstants.CENTROID_ID_FIELD;
        return IntegerPointable.getInteger(frameTuple.getFieldData(cidField), frameTuple.getFieldStart(cidField));
    }

    @Override
    public ITreeIndexTupleReference getFrameTuple() {
        return frameTuple;
    }

    @Override
    public byte[] getQuantizedCentroidBytes(int tupleIndex) throws HyracksDataException {
        frameTuple.resetByTupleIndex(this, tupleIndex);
        // A non-quantized leaf is [cid, embedding, metadataPtr] with the pointer at index
        // LEAF_QUANTIZED_BYTES_FIELD; a quantized leaf inserts the bytes field there and pushes the
        // pointer out, so it has at least LEAF_QUANTIZED_BYTES_FIELD + 2 fields (bytes + trailing ptr).
        if (frameTuple.getFieldCount() < VTreeStaticTupleConstants.LEAF_QUANTIZED_BYTES_FIELD + 2) {
            // Non-quantized tuple has no quantized-bytes field.
            return null;
        }
        // Quantized bytes live in field 2 of the quantized layout (>=4 fields):
        // [cid, embedding, quantizedBytes, (neighborList,) metadataPtr].
        int fieldIndex = VTreeStaticTupleConstants.LEAF_QUANTIZED_BYTES_FIELD;
        byte[] fieldData = frameTuple.getFieldData(fieldIndex);
        int fieldStart = frameTuple.getFieldStart(fieldIndex);
        // The field is a ByteArraySerializerDeserializer payload: a VarLen length prefix then the content
        // bytes. Read the length via ByteArrayPointable and copy the content directly, avoiding the per-tuple
        // DataInputStream/ByteArrayInputStream allocation on the search path.
        int contentLength = ByteArrayPointable.getContentLength(fieldData, fieldStart);
        int metaLength = ByteArrayPointable.getNumberBytesToStoreMeta(contentLength);
        return Arrays.copyOfRange(fieldData, fieldStart + metaLength, fieldStart + metaLength + contentLength);
    }

    @Override
    public String printHeader() {
        StringBuilder strBuilder = new StringBuilder(super.printHeader());
        strBuilder.append("nextLeaf:          ").append(getNextLeaf()).append('\n');
        return strBuilder.toString();
    }
}
