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

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.Integer64SerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.vector.api.VTreeQuantizationParams;
import org.apache.hyracks.storage.am.vector.impls.VTreeDataTupleBuilder;
import org.apache.hyracks.storage.am.vector.utils.VTreeDataTupleAccessor;
import org.apache.hyracks.storage.am.vector.utils.VTreeScalarQuantization;
import org.junit.Assert;
import org.junit.Test;

/**
 * The bulk-load encoder ({@link OptimizedScalarQuantizationCodec}) and the DML encoder ({@code
 * VTreeDataTupleBuilder}) must produce the same codes for the same index. A DML-inserted row on another scale
 * biases every approximate distance against it, which shows only as lost recall. The DML side runs through the
 * real tuple builder and accessor, so the varlen framing of the embedding field is compared too.
 */
public class QuantizationPathAgreementTest {

    private static final float MIN_Q = -1.25f;
    private static final float MAX_Q = 2.5f;

    /**
     * Both quantile endpoints, one value beyond each so the clamp is compared, and interior values whose
     * pre-round products have a fractional part, so a truncating encoder cannot agree with a rounding one
     * ({@link #requireFixtureIsSensitiveToRounding} checks this).
     */
    private static final double[] VECTOR =
            { MIN_Q, MAX_Q, MIN_Q - 10.0, MAX_Q + 10.0, 0.375, -0.9, 1.703, 0.0088, 2.4999, -1.2499 };

    /** SQ8: 256 levels, the default. */
    @Test
    public void bothPathsProduceTheSameCodesForSq8() throws HyracksDataException {
        assertPathsAgree(VectorQuantization.SQ8.bits(), VECTOR);
    }

    /** SQ4: 16 levels in a byte[], where a wrong level count is easy to miss. */
    @Test
    public void bothPathsProduceTheSameCodesForSq4() throws HyracksDataException {
        assertPathsAgree(VectorQuantization.SQ4.bits(), VECTOR);
    }

    /** Every quantization the product offers, so adding one to the enum lands here. */
    @Test
    public void bothPathsAgreeForEveryQuantizationTheProductOffers() throws HyracksDataException {
        for (VectorQuantization quantization : VectorQuantization.values()) {
            assertPathsAgree(quantization.bits(), VECTOR);
        }
    }

    /**
     * The endpoints pin the scale, which the two paths agreeing cannot do when both share a wrong {@code alpha}
     * or level count.
     */
    @Test
    public void theQuantileEndpointsMapToTheEndsOfTheCodeRange() {
        for (VectorQuantization quantization : VectorQuantization.values()) {
            int levels = 1 << quantization.bits();
            float alpha = (levels - 1) / (MAX_Q - MIN_Q);

            Assert.assertEquals("minQ must encode to 0 for " + quantization, 0,
                    VTreeScalarQuantization.encodeDimension(MIN_Q, MIN_Q, MAX_Q, alpha, levels));
            Assert.assertEquals("maxQ must encode to the top code for " + quantization, levels - 1,
                    VTreeScalarQuantization.encodeDimension(MAX_Q, MIN_Q, MAX_Q, alpha, levels));
            // Beyond the range the clamp holds the end codes.
            Assert.assertEquals(0, VTreeScalarQuantization.encodeDimension(MIN_Q - 1e6, MIN_Q, MAX_Q, alpha, levels));
            Assert.assertEquals(levels - 1,
                    VTreeScalarQuantization.encodeDimension(MAX_Q + 1e6, MIN_Q, MAX_Q, alpha, levels));
        }
    }

    /** Decode is the inverse to within one quantization step, which is what bounds the recall loss. */
    @Test
    public void decodeInvertsEncodeToWithinOneStep() {
        for (VectorQuantization quantization : VectorQuantization.values()) {
            int levels = 1 << quantization.bits();
            float alpha = (levels - 1) / (MAX_Q - MIN_Q);
            double step = 1.0 / alpha;

            for (double value : new double[] { MIN_Q, MAX_Q, 0.0, 1.0, 0.37, -0.9 }) {
                long code = VTreeScalarQuantization.encodeDimension(value, MIN_Q, MAX_Q, alpha, levels);
                double decoded = VTreeScalarQuantization.decodeDimension(code, alpha, MIN_Q);
                Assert.assertEquals("round-trip error above one step for " + value + " at " + quantization, value,
                        decoded, step / 2 + 1e-6);
            }
        }
    }

    /**
     * Leaf storage is {@code byte[]}, so the DML path refuses codes wider than a byte. No {@code
     * VectorQuantization} reaches this today, and the guard makes a wider scheme fail loudly.
     */
    @Test
    public void aCodeWiderThanAByteIsRefusedByTheDmlPath() {
        VTreeQuantizationParams tooWide = new VTreeQuantizationParams(MIN_Q, MAX_Q, 1.0f, 0.9f, 16, 1000);
        Assert.assertThrows(IllegalArgumentException.class, () -> new VTreeDataTupleBuilder(0, 1, true, tooWide));
    }

    /** Encodes {@code vector} through both paths with the same parameters and requires identical bytes. */
    private void assertPathsAgree(int bits, double[] vector) throws HyracksDataException {
        int levels = 1 << bits;
        float alpha = (levels - 1) / (MAX_Q - MIN_Q);

        requireFixtureIsSensitiveToRounding(vector, bits, alpha);

        byte[] fromBulkLoad = bulkLoadCodes(vector, bits, alpha);
        byte[] fromDml = dmlCodes(vector, bits, alpha);

        Assert.assertEquals("code count", fromBulkLoad.length, fromDml.length);
        Assert.assertArrayEquals("the bulk-load and DML paths disagree at " + bits
                + " bits; a row inserted by DML would not be" + " comparable with a bulk-loaded row", fromBulkLoad,
                fromDml);
        // A constant code vector would make the comparison above hold for any constant encoder.
        boolean allEqual = true;
        for (byte code : fromDml) {
            allEqual &= code == fromDml[0];
        }
        Assert.assertFalse("the fixture produced a constant code vector, so it proves nothing", allEqual);
    }

    /**
     * Requires a dimension whose pre-round product rounds differently from its floor, so the byte-for-byte
     * comparison sees the rounding rule.
     */
    private void requireFixtureIsSensitiveToRounding(double[] vector, int bits, float alpha) {
        int sensitive = 0;
        for (double value : vector) {
            double product = (Math.max(MIN_Q, Math.min(MAX_Q, value)) - MIN_Q) * alpha;
            if (Math.round(product) != (long) Math.floor(product)) {
                sensitive++;
            }
        }
        Assert.assertTrue("the fixture is insensitive to the rounding rule at " + bits + " bits: every"
                + " dimension's pre-round product is an exact integer, so a truncating encoder would agree"
                + " with a rounding one", sensitive > 0);
    }

    /** The bulk-load / static-structure encoder. */
    private byte[] bulkLoadCodes(double[] vector, int bits, float alpha) throws HyracksDataException {
        OptimizedScalarQuantizationCodec.Params params =
                new OptimizedScalarQuantizationCodec.Params(bits, vector.length, 1000, 0.9f, MIN_Q, MAX_Q, alpha);
        OptimizedScalarQuantizationCodec.QuantizedVector quantized = OptimizedScalarQuantizationCodec
                .quantizeVector(vector, params, OptimizedScalarQuantizationCodec.SimilarityFunction.EUCLIDEAN);
        Assert.assertTrue("SQ4/SQ8 must encode to byte[]", quantized.quantizedBytes instanceof byte[]);
        return (byte[]) quantized.quantizedBytes;
    }

    /**
     * The DML insert encoder, driven through the real tuple builder and read back with the production
     * accessor so the varlen framing of the embedding field is covered too.
     */
    private byte[] dmlCodes(double[] vector, int bits, float alpha) throws HyracksDataException {
        VTreeQuantizationParams params = new VTreeQuantizationParams(MIN_Q, MAX_Q, alpha, 0.9f, bits, 1000);
        VTreeDataTupleBuilder builder = new VTreeDataTupleBuilder(0, 1, true, params);

        ITupleReference dataTuple = builder.buildDataTuple(vector, 0.5, 7, inputTuple(vector, 42L));

        return new VTreeDataTupleAccessor(true).getQuantizedEmbedding(dataTuple);
    }

    /** {@code [vector, pk]}: the operator-side input layout with no include fields. */
    private static ITupleReference inputTuple(double[] vector, long primaryKey) throws HyracksDataException {
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(2);
        ArrayTupleReference tupleRef = new ArrayTupleReference();
        ISerializerDeserializer[] serdes =
                { DoubleArraySerializerDeserializer.INSTANCE, Integer64SerializerDeserializer.INSTANCE };
        TupleUtils.createTuple(tupleBuilder, tupleRef, serdes, new Object[] { vector, primaryKey });
        return tupleRef;
    }
}
