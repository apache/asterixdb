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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Random;

import junit.framework.Assert;

import org.junit.Test;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk.FixedSizeFrameTupleAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk.FixedSizeFrameTupleAppender;

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

        ftapp.reset(buffer, true);
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
            int val = IntegerSerializerDeserializer.getInt(ftacc.getBuffer().array(), ftacc.getTupleStartOffset(i));
            Assert.assertEquals(check.get(i).intValue(), val);
        }
    }
}
