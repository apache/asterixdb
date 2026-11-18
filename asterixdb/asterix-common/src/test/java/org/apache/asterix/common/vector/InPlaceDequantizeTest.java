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
package org.apache.asterix.common.vector;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.Integer64SerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.vector.api.IVTreeQuantizer;
import org.apache.hyracks.storage.am.vector.api.VTreeQuantizationParams;
import org.apache.hyracks.storage.am.vector.impls.VTreeDataTupleBuilder;
import org.apache.hyracks.storage.am.vector.utils.NoOpVectorQuantizer;
import org.apache.hyracks.storage.am.vector.utils.VTreeDataTupleAccessor;
import org.junit.Assert;
import org.junit.Test;

/**
 * The top-K scan decodes each stored code in place into one reused array instead of copying the code and
 * allocating a fresh vector per candidate. The in-place path must give exactly the values of the allocating one,
 * read from a real data tuple, so the approximate distances (and hence the candidate set) do not change.
 */
public class InPlaceDequantizeTest {

    private static final float MIN_Q = -1.25f;
    private static final float MAX_Q = 2.5f;
    private static final double[] VECTOR = { MIN_Q, MAX_Q, 0.375, -0.9, 1.703, 0.0088, 2.4999, -1.2499 };

    @Test
    public void inPlaceDecodeOfAStoredTupleMatchesTheAllocatingPathForSq8() throws HyracksDataException {
        assertInPlaceMatches(VectorQuantization.SQ8.bits());
    }

    @Test
    public void inPlaceDecodeOfAStoredTupleMatchesTheAllocatingPathForSq4() throws HyracksDataException {
        assertInPlaceMatches(VectorQuantization.SQ4.bits());
    }

    /** The scratch array is reused across candidates, so every slot of a dirty one must be overwritten. */
    @Test
    public void inPlaceDecodeOverwritesADirtyScratchArray() throws HyracksDataException {
        int bits = VectorQuantization.SQ8.bits();
        ITupleReference tuple = dataTuple(bits);
        VTreeDataTupleAccessor accessor = new VTreeDataTupleAccessor(true);
        IVTreeQuantizer quantizer = quantizer(bits);

        double[] scratch = new double[VECTOR.length];
        Arrays.fill(scratch, Double.NaN);
        decodeInPlace(quantizer, accessor, tuple, scratch);

        Assert.assertArrayEquals(quantizer.dequantize(accessor.getQuantizedEmbedding(tuple)), scratch, 0.0);
    }

    @Test
    public void aCodeOfTheWrongLengthIsRejected() {
        int bits = VectorQuantization.SQ8.bits();
        byte[] page = new byte[VECTOR.length + 4];
        double[] scratch = new double[VECTOR.length];
        Assert.assertThrows(HyracksDataException.class,
                () -> quantizer(bits).dequantize(page, 2, VECTOR.length - 1, scratch));
    }

    @Test
    public void aTooSmallDestinationIsRejected() {
        int bits = VectorQuantization.SQ8.bits();
        byte[] page = new byte[VECTOR.length];
        double[] scratch = new double[VECTOR.length - 1];
        Assert.assertThrows(HyracksDataException.class,
                () -> quantizer(bits).dequantize(page, 0, VECTOR.length, scratch));
    }

    /** Quantizers that do not override the in-place method fall back to the interface default. */
    @Test
    public void theInterfaceDefaultDecodesAnOffsetRange() throws HyracksDataException {
        ByteBuffer page = ByteBuffer.allocate(3 + VECTOR.length * Double.BYTES);
        page.position(3);
        for (double v : VECTOR) {
            page.putDouble(v);
        }
        double[] scratch = new double[VECTOR.length];
        NoOpVectorQuantizer.INSTANCE.dequantize(page.array(), 3, VECTOR.length * Double.BYTES, scratch);
        Assert.assertArrayEquals(VECTOR, scratch, 0.0);
    }

    private void assertInPlaceMatches(int bits) throws HyracksDataException {
        ITupleReference tuple = dataTuple(bits);
        VTreeDataTupleAccessor accessor = new VTreeDataTupleAccessor(true);
        IVTreeQuantizer quantizer = quantizer(bits);

        // The code sits after the field's length prefix, so a non-zero start is what this exercises.
        Assert.assertTrue("the code should not start at offset 0 of the tuple buffer",
                accessor.getQuantizedEmbeddingStart(tuple) > 0);
        Assert.assertEquals(VECTOR.length, accessor.getQuantizedEmbeddingLength(tuple));

        double[] expected = quantizer.dequantize(accessor.getQuantizedEmbedding(tuple));
        double[] scratch = new double[VECTOR.length];
        decodeInPlace(quantizer, accessor, tuple, scratch);
        Assert.assertArrayEquals(expected, scratch, 0.0);
    }

    private static void decodeInPlace(IVTreeQuantizer quantizer, VTreeDataTupleAccessor accessor, ITupleReference tuple,
            double[] scratch) throws HyracksDataException {
        quantizer.dequantize(tuple.getFieldData(accessor.quantizedEmbeddingField()),
                accessor.getQuantizedEmbeddingStart(tuple), accessor.getQuantizedEmbeddingLength(tuple), scratch);
    }

    private static float alpha(int bits) {
        return ((1 << bits) - 1) / (MAX_Q - MIN_Q);
    }

    private static IVTreeQuantizer quantizer(int bits) {
        OptimizedScalarQuantizationCodec.Params params =
                new OptimizedScalarQuantizationCodec.Params(bits, VECTOR.length, 1000, 0.9f, MIN_Q, MAX_Q, alpha(bits));
        return new OptimizedScalarQuantizer(params, OptimizedScalarQuantizationCodec.SimilarityFunction.EUCLIDEAN);
    }

    /** A stored data tuple built by the production DML encoder: {@code [dist, cid, qdist, code, pk]}. */
    private static ITupleReference dataTuple(int bits) throws HyracksDataException {
        VTreeQuantizationParams params = new VTreeQuantizationParams(MIN_Q, MAX_Q, alpha(bits), 0.9f, bits, 1000);
        VTreeDataTupleBuilder builder = new VTreeDataTupleBuilder(0, 1, true, params);
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(2);
        ArrayTupleReference input = new ArrayTupleReference();
        ISerializerDeserializer[] serdes =
                { DoubleArraySerializerDeserializer.INSTANCE, Integer64SerializerDeserializer.INSTANCE };
        TupleUtils.createTuple(tupleBuilder, input, serdes, new Object[] { VECTOR, 42L });
        return builder.buildDataTuple(VECTOR, 0.5, 7, input);
    }
}
