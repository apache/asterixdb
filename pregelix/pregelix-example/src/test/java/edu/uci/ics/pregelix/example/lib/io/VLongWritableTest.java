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

/**
 * @author yingyib
 */
public class VLongWritableTest {

    @Test
    public void test() throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutput dos = new DataOutputStream(bos);
        Random rand = new Random(System.currentTimeMillis());
        MsgList<VLongWritable> msgList = new MsgList<VLongWritable>();
        int accumulatedSize = 4;
        for (int i = 0; i < 1000000; i++) {
            bos.reset();
            VLongWritable value = new VLongWritable(Math.abs(rand.nextLong()));
            value.write(dos);
            if (value.sizeInBytes() < bos.size()) {
                throw new Exception(value + " estimated size (" + value.sizeInBytes()
                        + ") is smaller than the actual size" + bos.size());
            }
            msgList.add(value);
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
