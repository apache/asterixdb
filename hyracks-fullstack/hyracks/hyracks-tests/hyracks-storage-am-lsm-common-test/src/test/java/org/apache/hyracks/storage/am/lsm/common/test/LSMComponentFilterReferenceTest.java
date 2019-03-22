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
package org.apache.hyracks.storage.am.lsm.common.test;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.common.tuples.TypeAwareTupleWriter;
import org.apache.hyracks.storage.am.lsm.common.frames.LSMComponentFilterReference;
import org.junit.Assert;
import org.junit.Test;

public class LSMComponentFilterReferenceTest {

    @Test
    public void test() throws HyracksDataException {
        LSMComponentFilterReference filter = new LSMComponentFilterReference(
                new TypeAwareTupleWriter(new ITypeTraits[] { IntegerPointable.TYPE_TRAITS }));
        Assert.assertEquals(filter.getLength(), 0);
        Assert.assertFalse(filter.isMaxTupleSet() || filter.isMinTupleSet());
        filter.writeMaxTuple(TupleUtils.createIntegerTuple(false, Integer.MAX_VALUE));
        Assert.assertFalse(filter.isMinTupleSet());
        Assert.assertTrue(filter.isMaxTupleSet());
        Assert.assertTrue(filter.getLength() == 11);
        filter.writeMinTuple(TupleUtils.createIntegerTuple(false, Integer.MIN_VALUE));
        Assert.assertTrue(filter.isMinTupleSet() && filter.isMaxTupleSet());
        Assert.assertTrue(filter.getLength() == 20);
        byte[] serFilter = filter.getByteArray();
        LSMComponentFilterReference deserFilter = new LSMComponentFilterReference(
                new TypeAwareTupleWriter((new ITypeTraits[] { IntegerPointable.TYPE_TRAITS })));
        deserFilter.set(serFilter, 0, 20);
        Assert.assertTrue(deserFilter.isMaxTupleSet() && deserFilter.isMinTupleSet());
        Assert.assertEquals(
                TupleUtils.deserializeTuple(deserFilter.getMinTuple(),
                        new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE })[0],
                Integer.MIN_VALUE);
        Assert.assertEquals(
                TupleUtils.deserializeTuple(deserFilter.getMaxTuple(),
                        new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE })[0],
                Integer.MAX_VALUE);
    }
}
