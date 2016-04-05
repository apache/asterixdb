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

package org.apache.asterix.dataflow.data.nontagged.comparators.rangeinterval;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AIntervalSerializerDeserializer;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AInterval;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.junit.Assert;
import org.junit.Test;

import junit.framework.TestCase;

public class RangeIntervalBinaryComparatorFactoryTest extends TestCase {

    private final ISerializerDeserializer<AInterval> intervalSerde = AIntervalSerializerDeserializer.INSTANCE;
    private final ISerializerDeserializer<AInt64> intSerde = AInt64SerializerDeserializer.INSTANCE;

    /*
     * The following points (X) will be tested for this interval (+).
     *
     * ----X---XXX---X---XXX---X----
     * ---------+++++++++++---------
     */
    private final AInterval INTERVAL = new AInterval(10, 20, (byte) 16);
    private final int INTERVAL_OFFSET = 0;
    private int INTERVAL_LENGTH;
    private final int POINT_LENGTH = Long.BYTES;
    private final AInt64[] MAP_POINTS = new AInt64[] { new AInt64(5l), new AInt64(9l), new AInt64(10l), new AInt64(11l),
            new AInt64(15l), new AInt64(19l), new AInt64(20l), new AInt64(21l), new AInt64(25l) };

    private byte[] getIntervalBytes() throws HyracksDataException {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutput dos = new DataOutputStream(bos);
            intervalSerde.serialize(INTERVAL, dos);
            bos.close();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    private byte[] getIntegerMapBytes() throws HyracksDataException {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutput dos = new DataOutputStream(bos);
            for (int i = 0; i < MAP_POINTS.length; ++i) {
                intSerde.serialize(MAP_POINTS[i], dos);
            }
            bos.close();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    private void executeBinaryComparatorTests(IBinaryComparator bc, int[] results) throws HyracksDataException {
        // Bytes for interval
        byte[] intervalBytes = getIntervalBytes();
        INTERVAL_LENGTH = AIntervalSerializerDeserializer.getIntervalLength(intervalBytes, INTERVAL_OFFSET);

        // Bytes for map points
        byte[] intBytes = getIntegerMapBytes();

        for (int i = 0; i < results.length; ++i) {
            int point_offset = i * POINT_LENGTH;
            int c = bc.compare(intervalBytes, INTERVAL_OFFSET, INTERVAL_LENGTH, intBytes, point_offset, POINT_LENGTH);
            Assert.assertEquals(INTERVAL + " compares to map point (" + MAP_POINTS[i].getLongValue() + ")", results[i],
                    c);
        }
    }

    @Test
    public void testAscMin() throws HyracksDataException {
        IBinaryComparator bc = IntervalAscRangeBinaryComparatorFactory.INSTANCE.createMinBinaryComparator();
        int[] results = new int[] { 1, 1, 0, -1, -1, -1, -1, -1, -1 };
        executeBinaryComparatorTests(bc, results);
    }

    @Test
    public void testAscMax() throws HyracksDataException {
        IBinaryComparator bc = IntervalAscRangeBinaryComparatorFactory.INSTANCE.createMaxBinaryComparator();
        int[] results = new int[] { 1, 1, 1, 1, 1, 1, 0, -1, -1 };
        executeBinaryComparatorTests(bc, results);
    }

    @Test
    public void testDescMin() throws HyracksDataException {
        IBinaryComparator bc = IntervalDescRangeBinaryComparatorFactory.INSTANCE.createMinBinaryComparator();
        int[] results = new int[] { -1, -1, -1, -1, -1, -1, 0, 1, 1 };
        executeBinaryComparatorTests(bc, results);
    }

    @Test
    public void testDescMax() throws HyracksDataException {
        IBinaryComparator bc = IntervalDescRangeBinaryComparatorFactory.INSTANCE.createMaxBinaryComparator();
        int[] results = new int[] { -1, -1, 0, 1, 1, 1, 1, 1, 1 };
        executeBinaryComparatorTests(bc, results);
    }

}
