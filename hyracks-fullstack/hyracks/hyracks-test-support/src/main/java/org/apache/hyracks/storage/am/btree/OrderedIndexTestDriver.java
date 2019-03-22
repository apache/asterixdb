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

package org.apache.hyracks.storage.am.btree;

import java.util.Random;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import org.apache.hyracks.storage.am.config.AccessMethodTestsConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

@SuppressWarnings("rawtypes")
public abstract class OrderedIndexTestDriver {
    protected final Logger LOGGER = LogManager.getLogger();

    protected static final int numTuplesToInsert = AccessMethodTestsConfig.BTREE_NUM_TUPLES_TO_INSERT;

    protected abstract OrderedIndexTestContext createTestContext(ISerializerDeserializer[] fieldSerdes, int numKeys,
            BTreeLeafFrameType leafType, boolean filtered) throws Exception;

    protected abstract Random getRandom();

    protected abstract void runTest(ISerializerDeserializer[] fieldSerdes, int numKeys, BTreeLeafFrameType leafType,
            ITupleReference lowKey, ITupleReference highKey, ITupleReference prefixLowKey,
            ITupleReference prefixHighKey) throws Exception;

    protected abstract String getTestOpName();

    protected final BTreeLeafFrameType[] leafFrameTypesToTest;

    public OrderedIndexTestDriver(BTreeLeafFrameType[] leafFrameTypesToTest) {
        this.leafFrameTypesToTest = leafFrameTypesToTest;
    }

    @Test
    public void oneIntKeyAndValue() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("BTree " + getTestOpName() + " Test With One Int Key And Value.");
        }

        ISerializerDeserializer[] fieldSerdes =
                { IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
        // Range search in [-1000, 1000]
        ITupleReference lowKey = TupleUtils.createIntegerTuple(-1000);
        ITupleReference highKey = TupleUtils.createIntegerTuple(1000);

        for (BTreeLeafFrameType leafFrameType : leafFrameTypesToTest) {
            runTest(fieldSerdes, 1, leafFrameType, lowKey, highKey, null, null);
        }
    }

    @Test
    public void twoIntKeys() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("BTree " + getTestOpName() + " Test With Two Int Keys.");
        }

        ISerializerDeserializer[] fieldSerdes =
                { IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };

        // Range search in [50 0, 50 500]
        ITupleReference lowKey = TupleUtils.createIntegerTuple(50, 0);
        ITupleReference highKey = TupleUtils.createIntegerTuple(50, 500);

        // Prefix range search in [50, 50]
        ITupleReference prefixLowKey = TupleUtils.createIntegerTuple(50);
        ITupleReference prefixHighKey = TupleUtils.createIntegerTuple(50);

        for (BTreeLeafFrameType leafFrameType : leafFrameTypesToTest) {
            runTest(fieldSerdes, 2, leafFrameType, lowKey, highKey, prefixLowKey, prefixHighKey);
        }
    }

    @Test
    public void twoIntKeysAndValues() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("BTree " + getTestOpName() + " Test With Two Int Keys And Values.");
        }

        ISerializerDeserializer[] fieldSerdes =
                { IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                        IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };

        // Range search in [50 100, 100 100]
        ITupleReference lowKey = TupleUtils.createIntegerTuple(-100, -100);
        ITupleReference highKey = TupleUtils.createIntegerTuple(100, 100);

        // Prefix range search in [50, 50]
        ITupleReference prefixLowKey = TupleUtils.createIntegerTuple(50);
        ITupleReference prefixHighKey = TupleUtils.createIntegerTuple(50);

        for (BTreeLeafFrameType leafFrameType : leafFrameTypesToTest) {
            runTest(fieldSerdes, 2, leafFrameType, lowKey, highKey, prefixLowKey, prefixHighKey);
        }
    }

    @Test
    public void oneStringKeyAndValue() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("BTree " + getTestOpName() + " Test With One String Key And Value.");
        }

        ISerializerDeserializer[] fieldSerdes =
                { new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer() };

        // Range search in ["cbf", cc7"]
        ITupleReference lowKey = TupleUtils.createTuple(fieldSerdes, "cbf");
        ITupleReference highKey = TupleUtils.createTuple(fieldSerdes, "cc7");

        for (BTreeLeafFrameType leafFrameType : leafFrameTypesToTest) {
            runTest(fieldSerdes, 1, leafFrameType, lowKey, highKey, null, null);
        }
    }

    @Test
    public void twoStringKeys() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("BTree " + getTestOpName() + " Test With Two String Keys.");
        }

        ISerializerDeserializer[] fieldSerdes =
                { new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer() };

        // Range search in ["cbf", "ddd", cc7", "eee"]
        ITupleReference lowKey = TupleUtils.createTuple(fieldSerdes, "cbf", "ddd");
        ITupleReference highKey = TupleUtils.createTuple(fieldSerdes, "cc7", "eee");

        // Prefix range search in ["cbf", cc7"]
        ITupleReference prefixLowKey = TupleUtils.createTuple(fieldSerdes, "cbf");
        ITupleReference prefixHighKey = TupleUtils.createTuple(fieldSerdes, "cc7");

        for (BTreeLeafFrameType leafFrameType : leafFrameTypesToTest) {
            runTest(fieldSerdes, 2, leafFrameType, lowKey, highKey, prefixLowKey, prefixHighKey);
        }
    }

    @Test
    public void twoStringKeysAndValues() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("BTree " + getTestOpName() + " Test With Two String Keys And Values.");
        }

        ISerializerDeserializer[] fieldSerdes =
                { new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                        new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer() };

        // Range search in ["cbf", "ddd", cc7", "eee"]
        ITupleReference lowKey = TupleUtils.createTuple(fieldSerdes, "cbf", "ddd");
        ITupleReference highKey = TupleUtils.createTuple(fieldSerdes, "cc7", "eee");

        // Prefix range search in ["cbf", cc7"]
        ITupleReference prefixLowKey = TupleUtils.createTuple(fieldSerdes, "cbf");
        ITupleReference prefixHighKey = TupleUtils.createTuple(fieldSerdes, "cc7");

        for (BTreeLeafFrameType leafFrameType : leafFrameTypesToTest) {
            runTest(fieldSerdes, 2, leafFrameType, lowKey, highKey, prefixLowKey, prefixHighKey);
        }
    }
}
