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

package org.apache.hyracks.storage.am.lsm.invertedindex.ondisk;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.junit.Assert;
import org.junit.Test;

public class FixedSizeFrameTupleTest {

    private static int FRAME_SIZE = 4096;

    private Random rnd = new Random(50);

    /**
     * This test verifies the correct behavior of the FixedSizeFrameTuple class.
     * Frames containing FixedSizeFrameTuple's require neither tuple slots nor
     * field slots. The tests inserts generated data into a frame until the
     * frame is full, and then verifies the frame's contents.
     *
     */
    @Test
    public void singleFieldTest() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(FRAME_SIZE);

        ITypeTraits[] fields = new ITypeTraits[1];
        fields[0] = IntegerPointable.TYPE_TRAITS;

        FixedSizeFrameTupleAppender ftapp = new FixedSizeFrameTupleAppender(FRAME_SIZE, fields);
        FixedSizeFrameTupleAccessor ftacc = new FixedSizeFrameTupleAccessor(FRAME_SIZE, fields);

        boolean frameHasSpace = true;

        ArrayList<Integer> check = new ArrayList<Integer>();

        ftapp.reset(buffer);
        while (frameHasSpace) {
            int val = rnd.nextInt();
            frameHasSpace = ftapp.append(val);
            if (frameHasSpace) {
                check.add(val);
                ftapp.incrementTupleCount(1);
            }
        }

        ftacc.reset(buffer);
        for (int i = 0; i < ftacc.getTupleCount(); i++) {
            int val = IntegerPointable.getInteger(ftacc.getBuffer().array(), ftacc.getTupleStartOffset(i));
            Assert.assertEquals(check.get(i).intValue(), val);
        }
    }
}
