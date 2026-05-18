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

package org.apache.hyracks.storage.am.vector.impls;

import java.io.DataOutput;
import java.nio.ByteBuffer;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.vector.api.IVTreeDataTupleBuilder;
import org.apache.hyracks.storage.am.vector.api.VTreeQuantizationParams;
import org.apache.hyracks.storage.am.vector.utils.VTreeDataTupleAccessor;
import org.apache.hyracks.util.encoding.VarLenIntEncoderDecoder;

/**
 * Transforms input tuples from operator format to VTree data page storage format.
 *
 * Input tuple format: [vector, include_fields..., pk]
 *
 * Non-quantized output: [distance, centroidId, pk, include_fields...]
 * Quantized output:     [distance, centroidId, quantized_distance, quantized_embedding, pk, include_fields...]
 *
 * The returned ITupleReference is valid until the next call to {@link #buildDataTuple}.
 */
public class VTreeDataTupleBuilder implements IVTreeDataTupleBuilder {

    private final int numIncludeFields;

    private final boolean isQuantized;
    private final VTreeQuantizationParams quantizationParams;
    private final ArrayTupleBuilder tupleBuilder;
    private final ArrayTupleReference tupleRef;
    // Reused per-tuple scratch (insert/bulk-load hot path): a 5-byte VarLen length-prefix buffer (max varlen
    // width of a 32-bit int), the quantized-embedding buffer, and the test-only double-serialization buffer.
    // Each produced value is copied into the tuple before the next call, so the buffers are safe to reuse.
    private final byte[] varlenMeta = new byte[5];
    private byte[] quantizeScratch;
    private ByteBuffer fallbackBuf;

    public VTreeDataTupleBuilder(int numIncludeFields, boolean isQuantized,
            VTreeQuantizationParams quantizationParams) {
        this.numIncludeFields = numIncludeFields;
        this.isQuantized = isQuantized;
        this.quantizationParams = quantizationParams;
        // Field order and count are owned by VTreeDataTupleAccessor: [secondary fields][1 PK][includes].
        int fieldCount = new VTreeDataTupleAccessor(isQuantized).numSecondaryFields() + 1 + numIncludeFields;

        this.tupleBuilder = new ArrayTupleBuilder(fieldCount);
        this.tupleRef = new ArrayTupleReference();
    }

    @Override
    public ITupleReference buildDataTuple(double[] vector, double distance, int centroidId,
            ITupleReference originalTuple) throws HyracksDataException {
        try {
            tupleBuilder.reset();
            DataOutput dos = tupleBuilder.getDataOutput();

            // Field 0: distance (raw double)
            dos.writeDouble(distance);
            tupleBuilder.addFieldEndOffset();

            // Field 1: centroidId (raw int)
            dos.writeInt(centroidId);
            tupleBuilder.addFieldEndOffset();

            if (isQuantized) {
                writeQuantizedFields(dos, vector, distance);
            }

            // PK field (at position 1 + numIncludeFields in input)
            int pkFieldIndex = 1 + numIncludeFields;
            tupleBuilder.addField(originalTuple.getFieldData(pkFieldIndex), originalTuple.getFieldStart(pkFieldIndex),
                    originalTuple.getFieldLength(pkFieldIndex));

            // Include fields (from input fields 1..numIncludeFields)
            for (int i = 0; i < numIncludeFields; i++) {
                int srcFieldIndex = 1 + i;
                tupleBuilder.addField(originalTuple.getFieldData(srcFieldIndex),
                        originalTuple.getFieldStart(srcFieldIndex), originalTuple.getFieldLength(srcFieldIndex));
            }

            tupleRef.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
            return tupleRef;

        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    private void writeQuantizedFields(DataOutput dos, double[] vector, double distance) throws Exception {
        // Field 2: quantized distance (raw double)
        dos.writeDouble(distance);
        tupleBuilder.addFieldEndOffset();

        // Field 3: quantized embedding (VarLen length prefix + content bytes). Encode the prefix into the
        // reused varlenMeta buffer and write it straight to the output instead of allocating a byte[] per call.
        byte[] quantizedEmbedding = quantizeVector(vector);
        int metaLength = VarLenIntEncoderDecoder.encode(quantizedEmbedding.length, varlenMeta, 0);
        dos.write(varlenMeta, 0, metaLength);
        dos.write(quantizedEmbedding);
        tupleBuilder.addFieldEndOffset();
    }

    private byte[] quantizeVector(double[] vector) {
        if (quantizationParams != null) {
            float minQ = quantizationParams.minQuantile();
            float maxQ = quantizationParams.maxQuantile();
            float alpha = quantizationParams.alpha();
            int bits = quantizationParams.bits();

            int levels = 1 << bits;
            if (quantizeScratch == null || quantizeScratch.length != vector.length) {
                quantizeScratch = new byte[vector.length];
            }
            byte[] result = quantizeScratch;
            for (int i = 0; i < vector.length; i++) {
                double value = Math.max(minQ, Math.min(maxQ, vector[i]));
                int quantizedValue = Math.toIntExact(Math.round((value - minQ) * alpha));
                quantizedValue = Math.max(0, Math.min(levels - 1, quantizedValue));
                result[i] = (byte) quantizedValue;
            }
            return result;
        }
        // Fallback: serialize full-precision vector as raw big-endian doubles (tests), reusing the buffer.
        int byteLen = vector.length * Double.BYTES;
        if (fallbackBuf == null || fallbackBuf.capacity() != byteLen) {
            fallbackBuf = ByteBuffer.allocate(byteLen);
        }
        fallbackBuf.clear();
        for (double d : vector) {
            fallbackBuf.putDouble(d);
        }
        return fallbackBuf.array();
    }
}
