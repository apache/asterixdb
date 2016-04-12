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

package org.apache.asterix.dataflow.data.nontagged.comparators;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.asterix.dataflow.data.nontagged.serde.AObjectSerializerDeserializer;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AInterval;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.junit.Assert;
import org.junit.Test;

import junit.framework.TestCase;

public class AqlBinaryComparatorFactoryTest extends TestCase {

    @SuppressWarnings("rawtypes")
    private final ISerializerDeserializer serde = AObjectSerializerDeserializer.INSTANCE;

    /*
     * The following points (X) will be tested for this interval (+).
     *
     * ----X---XXX---X---XXX---X----
     * ---------+++++++++++---------
     */
    private final int INTERVAL_LENGTH = Byte.BYTES + Byte.BYTES + Long.BYTES + Long.BYTES;
    private final int INTEGER_LENGTH = Byte.BYTES + Long.BYTES;
    private final AInterval[] INTERVALS = new AInterval[] { new AInterval(10, 15, (byte) 16),
            new AInterval(10, 20, (byte) 16), new AInterval(15, 20, (byte) 16) };
    private final AInt64[] INTEGERS = new AInt64[] { new AInt64(10l), new AInt64(15l), new AInt64(20l) };

    @SuppressWarnings("unused")
    private byte[] getIntervalBytes() throws HyracksDataException {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutput dos = new DataOutputStream(bos);
            for (int i = 0; i < INTERVALS.length; ++i) {
                serde.serialize(INTERVALS[i], dos);
            }
            bos.close();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    private byte[] getIntegerBytes() throws HyracksDataException {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutput dos = new DataOutputStream(bos);
            for (int i = 0; i < INTEGERS.length; ++i) {
                serde.serialize(INTEGERS[i], dos);
            }
            bos.close();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    private void executeBinaryComparatorTests(IBinaryComparator bc, byte[] bytes, int fieldLength, int[][] results)
            throws HyracksDataException {
        for (int i = 0; i < results.length; ++i) {
            int leftOffset = i * fieldLength;
            for (int j = 0; j < results[i].length; ++j) {
                int rightOffset = j * fieldLength;
                int c = bc.compare(bytes, leftOffset, fieldLength, bytes, rightOffset, fieldLength);
                Assert.assertEquals("results[" + i + "][" + j + "]", results[i][j], c);
            }
        }
    }

    //    private final AInterval[] INTERVALS = new AInterval[] { new AInterval(10, 20, (byte) 16),
    //            new AInterval(10, 15, (byte) 16), new AInterval(15, 20, (byte) 16) };
    //    private final AInt64[] INTEGERS = new AInt64[] { new AInt64(10l), new AInt64(15l), new AInt64(20l) };
    //
    @Test
    public void testIntervalAsc() throws HyracksDataException {
        IBinaryComparator bc = AObjectAscBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        byte[] bytes = getIntervalBytes();
        int[][] results = new int[3][];
        results[0] = new int[] { 0, -1, -1 };
        results[1] = new int[] { 1, 0, -1 };
        results[2] = new int[] { 1, 1, 0 };
        executeBinaryComparatorTests(bc, bytes, INTERVAL_LENGTH, results);
    }

    @Test
    public void testIntervalDesc() throws HyracksDataException {
        IBinaryComparator bc = AObjectDescBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        byte[] bytes = getIntervalBytes();
        int[][] results = new int[3][];
        results[0] = new int[] { 0, 1, 1 };
        results[1] = new int[] { -1, 0, 1 };
        results[2] = new int[] { -1, -1, 0 };
        executeBinaryComparatorTests(bc, bytes, INTERVAL_LENGTH, results);
    }

    @Test
    public void testIntegerAsc() throws HyracksDataException {
        IBinaryComparator bc = AObjectAscBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        byte[] bytes = getIntegerBytes();
        int[][] results = new int[3][];
        results[0] = new int[] { 0, -1, -1 };
        results[1] = new int[] { 1, 0, -1 };
        results[2] = new int[] { 1, 1, 0 };
        executeBinaryComparatorTests(bc, bytes, INTEGER_LENGTH, results);
    }

    @Test
    public void testIngeterDesc() throws HyracksDataException {
        IBinaryComparator bc = AObjectDescBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        byte[] bytes = getIntegerBytes();
        int[][] results = new int[3][];
        results[0] = new int[] { 0, 1, 1 };
        results[1] = new int[] { -1, 0, 1 };
        results[2] = new int[] { -1, -1, 0 };
        executeBinaryComparatorTests(bc, bytes, INTEGER_LENGTH, results);
    }

}
