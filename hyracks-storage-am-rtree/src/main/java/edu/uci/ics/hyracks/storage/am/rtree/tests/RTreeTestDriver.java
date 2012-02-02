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

package edu.uci.ics.hyracks.storage.am.rtree.tests;

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
import edu.uci.ics.hyracks.storage.am.rtree.util.RTreeUtils;

@SuppressWarnings("rawtypes")
public abstract class RTreeTestDriver {
    protected final Logger LOGGER = Logger.getLogger(RTreeTestDriver.class.getName());

    protected static final int numTuplesToInsert = 10000;

    protected abstract AbstractRTreeTestContext createTestContext(ISerializerDeserializer[] fieldSerdes,
            IPrimitiveValueProviderFactory[] valueProviderFactories, int numKeys) throws Exception;

    protected abstract Random getRandom();

    protected abstract void runTest(ISerializerDeserializer[] fieldSerdes,
            IPrimitiveValueProviderFactory[] valueProviderFactories, int numKeys, ITupleReference key) throws Exception;

    protected abstract String getTestOpName();

    @Test
    public void twoDimensionsInt() throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("RTree " + getTestOpName() + " Test With Two Dimensions With Integer Keys.");
        }

        ISerializerDeserializer[] fieldSerdes = { IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE };

        int numKeys = 4;
        IPrimitiveValueProviderFactory[] valueProviderFactories = RTreeUtils.createPrimitiveValueProviderFactories(
                numKeys, IntegerPointable.FACTORY);
        // Range search, the rectangle bottom left coordinates are -1000, -1000
        // and the top right coordinates are 1000, 1000
        ITupleReference key = TupleUtils.createIntegerTuple(-1000, -1000, 1000, 1000);

        runTest(fieldSerdes, valueProviderFactories, numKeys, key);

    }

    @Test
    public void twoDimensionsDouble() throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("RTree " + getTestOpName() + " Test With Two Dimensions With Double Keys.");
        }

        ISerializerDeserializer[] fieldSerdes = { DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE };

        int numKeys = 4;
        IPrimitiveValueProviderFactory[] valueProviderFactories = RTreeUtils.createPrimitiveValueProviderFactories(
                numKeys, DoublePointable.FACTORY);
        // Range search, the rectangle bottom left coordinates are -1000.0,
        // -1000.0 and the top right coordinates are 1000.0, 1000.0
        ITupleReference key = TupleUtils.createDoubleTuple(-1000.0, -1000.0, 1000.0, 1000.0);

        runTest(fieldSerdes, valueProviderFactories, numKeys, key);

    }

    @Test
    public void fourDimensionsDouble() throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("RTree " + getTestOpName() + " Test With Four Dimensions With Double Keys.");
        }

        ISerializerDeserializer[] fieldSerdes = { DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE };

        int numKeys = 8;
        IPrimitiveValueProviderFactory[] valueProviderFactories = RTreeUtils.createPrimitiveValueProviderFactories(
                numKeys, DoublePointable.FACTORY);
        // Range search, the rectangle bottom left coordinates are -1000.0,
        // -1000.0, -1000.0, -1000.0 and the top right coordinates are 1000.0,
        // 1000.0, 1000.0, 1000.0
        ITupleReference key = TupleUtils.createDoubleTuple(-1000.0, -1000.0, -1000.0, -1000.0, 1000.0, 1000.0, 1000.0,
                1000.0);

        runTest(fieldSerdes, valueProviderFactories, numKeys, key);

    }
}
