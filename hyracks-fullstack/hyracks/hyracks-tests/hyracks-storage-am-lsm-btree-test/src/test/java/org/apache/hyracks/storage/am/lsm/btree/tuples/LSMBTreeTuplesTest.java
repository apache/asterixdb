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

package org.apache.hyracks.storage.am.lsm.btree.tuples;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.SerdeUtils;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.common.datagen.DataGenUtils;
import org.apache.hyracks.storage.am.common.datagen.IFieldValueGenerator;
import org.junit.Test;

@SuppressWarnings("rawtypes")
public class LSMBTreeTuplesTest {

    private final Random rnd = new Random(50);

    private ByteBuffer writeTuple(ITupleReference tuple, LSMBTreeTupleWriter tupleWriter) {
        // Write tuple into a buffer, then later try to read it.
        int bytesRequired = tupleWriter.bytesRequired(tuple);
        byte[] bytes = new byte[bytesRequired];
        ByteBuffer targetBuf = ByteBuffer.wrap(bytes);
        tupleWriter.writeTuple(tuple, bytes, 0);
        return targetBuf;
    }

    private void testLSMBTreeTuple(ISerializerDeserializer[] maxFieldSerdes) throws HyracksDataException {
        // Create a tuple with the max-1 fields for checking setFieldCount() of tuple references later.
        ITypeTraits[] maxTypeTraits = SerdeUtils.serdesToTypeTraits(maxFieldSerdes);
        IFieldValueGenerator[] maxFieldGens = DataGenUtils.getFieldGensFromSerdes(maxFieldSerdes, rnd, false);
        // Generate a tuple with random field values.
        Object[] maxFields = new Object[maxFieldSerdes.length];
        for (int j = 0; j < maxFieldSerdes.length; j++) {
            maxFields[j] = maxFieldGens[j].next();
        }

        // Run test for varying number of fields and keys.
        for (int numKeyFields = 1; numKeyFields < maxFieldSerdes.length; numKeyFields++) {
            // Create tuples with varying number of fields, and try to interpret their bytes with the lsmBTreeTuple.
            for (int numFields = numKeyFields; numFields <= maxFieldSerdes.length; numFields++) {
                // Create and write tuple to bytes using an LSMBTreeTupleWriter.
                LSMBTreeTupleWriter maxMatterTupleWriter =
                        new LSMBTreeTupleWriter(maxTypeTraits, numKeyFields, false, false);
                ITupleReference maxTuple = TupleUtils.createTuple(maxFieldSerdes, (Object[]) maxFields);
                ByteBuffer maxMatterBuf = writeTuple(maxTuple, maxMatterTupleWriter);
                // Tuple reference should work for both matter and antimatter tuples (doesn't matter which factory creates it).
                LSMBTreeTupleReference maxLsmBTreeTuple = maxMatterTupleWriter.createTupleReference();

                ISerializerDeserializer[] fieldSerdes = Arrays.copyOfRange(maxFieldSerdes, 0, numFields);
                ITypeTraits[] typeTraits = SerdeUtils.serdesToTypeTraits(fieldSerdes);
                IFieldValueGenerator[] fieldGens = DataGenUtils.getFieldGensFromSerdes(fieldSerdes, rnd, false);
                // Generate a tuple with random field values.
                Object[] fields = new Object[numFields];
                for (int j = 0; j < numFields; j++) {
                    fields[j] = fieldGens[j].next();
                }
                // Create and write tuple to bytes using an LSMBTreeTupleWriter.
                ITupleReference tuple = TupleUtils.createTuple(fieldSerdes, (Object[]) fields);
                LSMBTreeTupleWriter matterTupleWriter = new LSMBTreeTupleWriter(typeTraits, numKeyFields, false, false);
                LSMBTreeTupleWriter antimatterTupleWriter =
                        new LSMBTreeTupleWriter(typeTraits, numKeyFields, true, false);
                LSMBTreeCopyTupleWriter copyTupleWriter = new LSMBTreeCopyTupleWriter(typeTraits, numKeyFields, false);
                ByteBuffer matterBuf = writeTuple(tuple, matterTupleWriter);
                ByteBuffer antimatterBuf = writeTuple(tuple, antimatterTupleWriter);

                // The antimatter buf should only contain keys, sanity check the size.
                if (numFields != numKeyFields) {
                    assertTrue(antimatterBuf.array().length < matterBuf.array().length);
                }

                // Tuple reference should work for both matter and antimatter tuples (doesn't matter which factory creates it).
                LSMBTreeTupleReference lsmBTreeTuple = matterTupleWriter.createTupleReference();

                // Use LSMBTree tuple reference to interpret the written tuples.
                // Repeat the block inside to test that repeated resetting to matter/antimatter tuples works.
                for (int r = 0; r < 4; r++) {

                    // Check matter tuple with lsmBTreeTuple.
                    lsmBTreeTuple.resetByTupleOffset(matterBuf.array(), 0);
                    checkTuple(lsmBTreeTuple, numFields, false, fieldSerdes, fields);

                    // Create a copy using copyTupleWriter, and verify again.
                    ByteBuffer copyMatterBuf = writeTuple(lsmBTreeTuple, copyTupleWriter);
                    lsmBTreeTuple.resetByTupleOffset(copyMatterBuf.array(), 0);
                    checkTuple(lsmBTreeTuple, numFields, false, fieldSerdes, fields);

                    // Check antimatter tuple with lsmBTreeTuple.
                    lsmBTreeTuple.resetByTupleOffset(antimatterBuf.array(), 0);
                    // Should only contain keys.
                    checkTuple(lsmBTreeTuple, numKeyFields, true, fieldSerdes, fields);

                    // Create a copy using copyTupleWriter, and verify again.
                    ByteBuffer copyAntimatterBuf = writeTuple(lsmBTreeTuple, copyTupleWriter);
                    lsmBTreeTuple.resetByTupleOffset(copyAntimatterBuf.array(), 0);
                    // Should only contain keys.
                    checkTuple(lsmBTreeTuple, numKeyFields, true, fieldSerdes, fields);

                    // Check matter tuple with maxLsmBTreeTuple.
                    // We should be able to manually set a prefix of the fields
                    // (the passed type traits in the tuple factory's constructor).
                    maxLsmBTreeTuple.setFieldCount(numFields);
                    maxLsmBTreeTuple.resetByTupleOffset(matterBuf.array(), 0);
                    checkTuple(maxLsmBTreeTuple, numFields, false, fieldSerdes, fields);

                    // Check antimatter tuple with maxLsmBTreeTuple.
                    maxLsmBTreeTuple.resetByTupleOffset(antimatterBuf.array(), 0);
                    // Should only contain keys.
                    checkTuple(maxLsmBTreeTuple, numKeyFields, true, fieldSerdes, fields);

                    // Resetting maxLsmBTreeTuple should set its field count to
                    // maxFieldSerdes.length, based on the its type traits.
                    maxLsmBTreeTuple.resetByTupleOffset(maxMatterBuf.array(), 0);
                    checkTuple(maxLsmBTreeTuple, maxFieldSerdes.length, false, maxFieldSerdes, maxFields);
                }
            }
        }
    }

    private void checkTuple(LSMBTreeTupleReference tuple, int expectedFieldCount, boolean expectedAntimatter,
            ISerializerDeserializer[] fieldSerdes, Object[] expectedFields) throws HyracksDataException {
        assertEquals(expectedFieldCount, tuple.getFieldCount());
        assertEquals(expectedAntimatter, tuple.isAntimatter());
        Object[] deserMatterTuple = TupleUtils.deserializeTuple(tuple, fieldSerdes);
        for (int j = 0; j < expectedFieldCount; j++) {
            assertEquals(expectedFields[j], deserMatterTuple[j]);
        }
    }

    @Test
    public void testLSMBTreeTuple() throws HyracksDataException {
        ISerializerDeserializer[] intFields =
                new IntegerSerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE,
                        IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                        IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
        testLSMBTreeTuple(intFields);

        ISerializerDeserializer[] stringFields = new ISerializerDeserializer[] { new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer() };
        testLSMBTreeTuple(stringFields);

        ISerializerDeserializer[] mixedFields = new ISerializerDeserializer[] { new UTF8StringSerializerDeserializer(),
                IntegerSerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer(), IntegerSerializerDeserializer.INSTANCE };
        testLSMBTreeTuple(mixedFields);
    }
}
