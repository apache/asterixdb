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
package org.apache.hyracks.storage.am.common.frames;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.storage.am.common.freepage.MutableArrayValueReference;
import org.apache.hyracks.storage.common.buffercache.VirtualPage;
import org.junit.Assert;
import org.junit.Test;

public class LIFOMetadataFrameTest {

    @Test
    public void test() throws HyracksDataException {
        LIFOMetaDataFrame frame = new LIFOMetaDataFrame();
        VirtualPage page = new VirtualPage(ByteBuffer.allocate(512), 512);
        MutableArrayValueReference testKey = new MutableArrayValueReference("TestLSNKey".getBytes());
        frame.setPage(page);
        frame.init();
        LongPointable longPointable = (LongPointable) LongPointable.FACTORY.createPointable();
        frame.get(testKey, longPointable);
        Assert.assertNull(longPointable.getByteArray());
        byte[] longBytes = new byte[Long.BYTES];
        MutableArrayValueReference value = new MutableArrayValueReference(longBytes);
        int space = frame.getSpace() - (value.getLength() + Integer.BYTES * 2 + testKey.getLength());
        for (long l = 1L; l < 52L; l++) {
            LongPointable.setLong(longBytes, 0, l);
            frame.put(testKey, value);
            Assert.assertEquals(space, frame.getSpace());
            frame.get(testKey, longPointable);
            Assert.assertEquals(l, longPointable.longValue());
        }
        Assert.assertEquals(frame.getFreePage(), -1);
        // add 10 pages and monitor space
        for (int i = 0; i < 10; i++) {
            frame.addFreePage(i);
            space -= Integer.BYTES;
            Assert.assertEquals(space, frame.getSpace());
        }
        for (int i = 9; i >= 0; i--) {
            int freePage = frame.getFreePage();
            Assert.assertEquals(freePage, i);
            space += Integer.BYTES;
            Assert.assertEquals(space, frame.getSpace());
        }
        Assert.assertTrue(frame.getFreePage() < 0);
    }
}
