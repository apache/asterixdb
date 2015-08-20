/*
 * Copyright 2009-2013 by The Regents of the University of California
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

package edu.uci.ics.hyracks.storage.am.rtree;

import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Test;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.util.TupleUtils;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import edu.uci.ics.hyracks.storage.am.config.AccessMethodTestsConfig;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreePolicyType;
import edu.uci.ics.hyracks.storage.am.rtree.util.RTreeUtils;

@SuppressWarnings("rawtypes")
public abstract class AbstractRTreeTestDriver {
    protected final boolean testRstarPolicy;

    public AbstractRTreeTestDriver(boolean testRstarPolicy) {
        this.testRstarPolicy = testRstarPolicy;
    }

    protected final Logger LOGGER = Logger.getLogger(AbstractRTreeTestDriver.class.getName());

    protected static final int numTuplesToInsert = AccessMethodTestsConfig.RTREE_NUM_TUPLES_TO_INSERT;

    protected abstract AbstractRTreeTestContext createTestContext(ISerializerDeserializer[] fieldSerdes,
            IPrimitiveValueProviderFactory[] valueProviderFactories, int numKeys, RTreePolicyType rtreePolicyType)
            throws Exception;

    protected abstract Random getRandom();

    protected abstract void runTest(ISerializerDeserializer[] fieldSerdes,
            IPrimitiveValueProviderFactory[] valueProviderFactories, int numKeys, ITupleReference key,
            RTreePolicyType rtreePolicyType) throws Exception;

    protected abstract String getTestOpName();

    @Test
    public void rtreeTwoDimensionsInt() throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("RTree " + getTestOpName() + " Test With Two Dimensions With Integer Keys.");
        }

        ISerializerDeserializer[] fieldSerdes = { IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };

        int numKeys = 4;
        IPrimitiveValueProviderFactory[] valueProviderFactories = RTreeUtils.createPrimitiveValueProviderFactories(
                numKeys, IntegerPointable.FACTORY);
        // Range search, the rectangle bottom left coordinates are -1000, -1000
        // and the top right coordinates are 1000, 1000
        ITupleReference key = TupleUtils.createIntegerTuple(-1000, -1000, 1000, 1000);

        runTest(fieldSerdes, valueProviderFactories, numKeys, key, RTreePolicyType.RTREE);

    }

    @Test
    public void rtreeTwoDimensionsDouble() throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("RTree " + getTestOpName() + " Test With Two Dimensions With Double Keys.");
        }

        ISerializerDeserializer[] fieldSerdes = { DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE };

        int numKeys = 4;
        IPrimitiveValueProviderFactory[] valueProviderFactories = RTreeUtils.createPrimitiveValueProviderFactories(
                numKeys, DoublePointable.FACTORY);
        // Range search, the rectangle bottom left coordinates are -1000.0,
        // -1000.0 and the top right coordinates are 1000.0, 1000.0
        ITupleReference key = TupleUtils.createDoubleTuple(-1000.0, -1000.0, 1000.0, 1000.0);

        runTest(fieldSerdes, valueProviderFactories, numKeys, key, RTreePolicyType.RTREE);

    }

    @Test
    public void rtreeFourDimensionsDouble() throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("RTree " + getTestOpName() + " Test With Four Dimensions With Double Keys.");
        }

        ISerializerDeserializer[] fieldSerdes = { DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE };

        int numKeys = 8;
        IPrimitiveValueProviderFactory[] valueProviderFactories = RTreeUtils.createPrimitiveValueProviderFactories(
                numKeys, DoublePointable.FACTORY);
        // Range search, the rectangle bottom left coordinates are -1000.0,
        // -1000.0, -1000.0, -1000.0 and the top right coordinates are 1000.0,
        // 1000.0, 1000.0, 1000.0
        ITupleReference key = TupleUtils.createDoubleTuple(-1000.0, -1000.0, -1000.0, -1000.0, 1000.0, 1000.0, 1000.0,
                1000.0);

        runTest(fieldSerdes, valueProviderFactories, numKeys, key, RTreePolicyType.RTREE);
    }

    @Test
    public void rstartreeTwoDimensionsInt() throws Exception {
        if (!testRstarPolicy) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Ignoring RTree " + getTestOpName() + " Test With Two Dimensions With Integer Keys.");
            }
            return;
        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("RTree " + getTestOpName() + " Test With Two Dimensions With Integer Keys.");
        }

        ISerializerDeserializer[] fieldSerdes = { IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };

        int numKeys = 4;
        IPrimitiveValueProviderFactory[] valueProviderFactories = RTreeUtils.createPrimitiveValueProviderFactories(
                numKeys, IntegerPointable.FACTORY);
        // Range search, the rectangle bottom left coordinates are -1000, -1000
        // and the top right coordinates are 1000, 1000
        ITupleReference key = TupleUtils.createIntegerTuple(-1000, -1000, 1000, 1000);

        runTest(fieldSerdes, valueProviderFactories, numKeys, key, RTreePolicyType.RSTARTREE);

    }

    @Test
    public void rstartreeTwoDimensionsDouble() throws Exception {
        if (!testRstarPolicy) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Ignoring RTree " + getTestOpName() + " Test With Two Dimensions With Double Keys.");
            }
            return;
        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("RTree " + getTestOpName() + " Test With Two Dimensions With Double Keys.");
        }

        ISerializerDeserializer[] fieldSerdes = { DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE };

        int numKeys = 4;
        IPrimitiveValueProviderFactory[] valueProviderFactories = RTreeUtils.createPrimitiveValueProviderFactories(
                numKeys, DoublePointable.FACTORY);
        // Range search, the rectangle bottom left coordinates are -1000.0,
        // -1000.0 and the top right coordinates are 1000.0, 1000.0
        ITupleReference key = TupleUtils.createDoubleTuple(-1000.0, -1000.0, 1000.0, 1000.0);

        runTest(fieldSerdes, valueProviderFactories, numKeys, key, RTreePolicyType.RSTARTREE);

    }

    @Test
    public void rstartreeFourDimensionsDouble() throws Exception {
        if (!testRstarPolicy) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Ignoring RTree " + getTestOpName() + " Test With Four Dimensions With Double Keys.");
            }
            return;
        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("RTree " + getTestOpName() + " Test With Four Dimensions With Double Keys.");
        }

        ISerializerDeserializer[] fieldSerdes = { DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE };

        int numKeys = 8;
        IPrimitiveValueProviderFactory[] valueProviderFactories = RTreeUtils.createPrimitiveValueProviderFactories(
                numKeys, DoublePointable.FACTORY);
        // Range search, the rectangle bottom left coordinates are -1000.0,
        // -1000.0, -1000.0, -1000.0 and the top right coordinates are 1000.0,
        // 1000.0, 1000.0, 1000.0
        ITupleReference key = TupleUtils.createDoubleTuple(-1000.0, -1000.0, -1000.0, -1000.0, 1000.0, 1000.0, 1000.0,
                1000.0);

        runTest(fieldSerdes, valueProviderFactories, numKeys, key, RTreePolicyType.RSTARTREE);
    }
}
