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
package edu.uci.ics.pregelix.example.lib.io;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.util.Random;

import org.junit.Test;

import edu.uci.ics.pregelix.api.graph.MsgList;
import edu.uci.ics.pregelix.api.io.WritableSizable;
import edu.uci.ics.pregelix.example.io.BooleanWritable;
import edu.uci.ics.pregelix.example.io.ByteWritable;
import edu.uci.ics.pregelix.example.io.DoubleWritable;
import edu.uci.ics.pregelix.example.io.IntWritable;
import edu.uci.ics.pregelix.example.io.LongWritable;
import edu.uci.ics.pregelix.example.io.NullWritable;
import edu.uci.ics.pregelix.example.io.VIntWritable;
import edu.uci.ics.pregelix.example.io.VLongWritable;

/**
 * @author yingyib
 */
public class SizeEstimationTest {

    @Test
    public void testVLong() throws Exception {
        Random rand = new Random(System.currentTimeMillis());
        MsgList<WritableSizable> msgList = new MsgList<WritableSizable>();
        msgList.add(new VLongWritable(Long.MAX_VALUE));
        msgList.add(new VLongWritable(Long.MIN_VALUE));
        msgList.add(new VLongWritable(-1));
        for (int i = 0; i < 1000000; i++) {
            msgList.add(new VLongWritable(Math.abs(rand.nextLong())));
        }
        verifyExactSizeEstimation(msgList);
    }

    @Test
    public void testLong() throws Exception {
        Random rand = new Random(System.currentTimeMillis());
        MsgList<WritableSizable> msgList = new MsgList<WritableSizable>();
        for (int i = 0; i < 1000000; i++) {
            msgList.add(new LongWritable(rand.nextLong()));
        }
        verifySizeEstimation(msgList);
    }

    @Test
    public void testBoolean() throws Exception {
        Random rand = new Random(System.currentTimeMillis());
        MsgList<WritableSizable> msgList = new MsgList<WritableSizable>();
        for (int i = 0; i < 1000000; i++) {
            msgList.add(new BooleanWritable(rand.nextBoolean()));
        }
        verifySizeEstimation(msgList);
    }

    @Test
    public void testByte() throws Exception {
        Random rand = new Random(System.currentTimeMillis());
        MsgList<WritableSizable> msgList = new MsgList<WritableSizable>();
        for (int i = 0; i < 1000000; i++) {
            msgList.add(new ByteWritable((byte) rand.nextInt()));
        }
        verifySizeEstimation(msgList);
    }

    @Test
    public void testDouble() throws Exception {
        Random rand = new Random(System.currentTimeMillis());
        MsgList<WritableSizable> msgList = new MsgList<WritableSizable>();
        for (int i = 0; i < 1000000; i++) {
            msgList.add(new DoubleWritable(rand.nextDouble()));
        }
        verifySizeEstimation(msgList);
    }

    @Test
    public void testFloat() throws Exception {
        Random rand = new Random(System.currentTimeMillis());
        MsgList<WritableSizable> msgList = new MsgList<WritableSizable>();
        for (int i = 0; i < 1000000; i++) {
            msgList.add(new DoubleWritable(rand.nextFloat()));
        }
        verifySizeEstimation(msgList);
    }

    @Test
    public void testNull() throws Exception {
        MsgList<WritableSizable> msgList = new MsgList<WritableSizable>();
        for (int i = 0; i < 1000000; i++) {
            msgList.add(NullWritable.get());
        }
        verifySizeEstimation(msgList);
    }

    @Test
    public void testVInt() throws Exception {
        Random rand = new Random(System.currentTimeMillis());
        MsgList<WritableSizable> msgList = new MsgList<WritableSizable>();
        for (int i = 0; i < 1000000; i++) {
            msgList.add(new VIntWritable(rand.nextInt()));
        }
        verifySizeEstimation(msgList);
    }

    @Test
    public void testInt() throws Exception {
        Random rand = new Random(System.currentTimeMillis());
        MsgList<WritableSizable> msgList = new MsgList<WritableSizable>();
        for (int i = 0; i < 1000000; i++) {
            msgList.add(new IntWritable(rand.nextInt()));
        }
        verifySizeEstimation(msgList);
    }

    private void verifySizeEstimation(MsgList<WritableSizable> msgList) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutput dos = new DataOutputStream(bos);
        int accumulatedSize = 5;
        for (int i = 0; i < msgList.size(); i++) {
            bos.reset();
            WritableSizable value = msgList.get(i);
            value.write(dos);
            if (value.sizeInBytes() < bos.size()) {
                throw new Exception(value + " estimated size (" + value.sizeInBytes()
                        + ") is smaller than the actual size" + bos.size());
            }
            accumulatedSize += value.sizeInBytes();
        }
        bos.reset();
        msgList.write(dos);
        if (accumulatedSize < bos.size()) {
            throw new Exception("Estimated list size (" + accumulatedSize + ") is smaller than the actual size"
                    + bos.size());
        }
    }

    private void verifyExactSizeEstimation(MsgList<WritableSizable> msgList) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutput dos = new DataOutputStream(bos);
        int accumulatedSize = 5;
        for (int i = 0; i < msgList.size(); i++) {
            bos.reset();
            WritableSizable value = msgList.get(i);
            value.write(dos);
            if (value.sizeInBytes() != bos.size()) {
                throw new Exception(value + " estimated size (" + value.sizeInBytes()
                        + ") is smaller than the actual size" + bos.size());
            }
            accumulatedSize += value.sizeInBytes();
        }
        bos.reset();
        msgList.write(dos);
        if (accumulatedSize < bos.size()) {
            throw new Exception("Estimated list size (" + accumulatedSize + ") is smaller than the actual size"
                    + bos.size());
        }
    }

}
