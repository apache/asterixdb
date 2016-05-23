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
package org.apache.asterix.external.feed.test;

import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.asterix.external.feed.dataflow.FrameSpiller;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.test.support.TestUtils;
import org.apache.wicket.util.file.File;
import org.junit.Assert;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class FeedSpillerUnitTest extends TestCase {
    private static final int DEFAULT_FRAME_SIZE = 32768;
    private static final int NUM_FRAMES = 3096;
    private static final String TEST_DATAVERSE = "testverse";
    private static final String TEST_FEED = "testeed";
    private static final String TEST_DATASET = "testset";
    private static final FilenameFilter SPILL_FILE_FILTER = new FilenameFilter() {
        @Override
        public boolean accept(java.io.File dir, String name) {
            return name.startsWith(TEST_DATAVERSE);
        }
    };

    public FeedSpillerUnitTest(String testName) {
        super(testName);
    }

    public void removeSpillFiles() throws IOException {
        File cwd = new File("./");
        String[] spills = cwd.list(SPILL_FILE_FILTER);
        for (String fileName : spills) {
            Files.delete(Paths.get(fileName));
        }
    }

    public int countSpillFiles() throws IOException {
        File cwd = new File("./");
        return cwd.list(SPILL_FILE_FILTER).length;
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(FeedSpillerUnitTest.class);
    }
    /*
     * Spiller spills each 1024 frames to a file
     */

    /*
     * The following tests:
     * 1. Test writer only.
     * Write 1023 frames.
     * Check only 1 file exist.
     * Write 1 more frame
     * Check two files exist.
     * Insert 1023 more frames.
     * Check that we still have 2 files.
     * Write 1 more frame.
     * Check that we have 3 files.
     * Check that we have 2048 frames to read.
     * Close the spiller
     * Check files were deleted.
     */
    @org.junit.Test
    public void testWriteFixedSizeSpill() {
        try {
            removeSpillFiles();
            IHyracksTaskContext ctx = TestUtils.create(DEFAULT_FRAME_SIZE);
            FrameSpiller spiller = new FrameSpiller(ctx, TEST_DATAVERSE + "_" + TEST_FEED + "_" + TEST_DATASET,
                    new Long(NUM_FRAMES * DEFAULT_FRAME_SIZE));
            spiller.open();
            VSizeFrame frame = new VSizeFrame(ctx);
            spiller.spill(frame.getBuffer());
            Assert.assertEquals(1, spiller.remaining());
            Assert.assertEquals(1, countSpillFiles());
            for (int i = 0; i < 1022; i++) {
                spiller.spill(frame.getBuffer());
            }
            Assert.assertEquals(1023, spiller.remaining());
            Assert.assertEquals(1, countSpillFiles());
            spiller.spill(frame.getBuffer());
            Assert.assertEquals(1024, spiller.remaining());
            Assert.assertEquals(2, countSpillFiles());
            for (int i = 0; i < 1023; i++) {
                spiller.spill(frame.getBuffer());
            }
            Assert.assertEquals(2047, spiller.remaining());
            Assert.assertEquals(2, countSpillFiles());
            spiller.spill(frame.getBuffer());
            Assert.assertEquals(2048, spiller.remaining());
            Assert.assertEquals(3, countSpillFiles());
            spiller.close();
            Assert.assertEquals(0, spiller.remaining());
            Assert.assertEquals(0, countSpillFiles());
        } catch (Throwable th) {
            th.printStackTrace();
            Assert.fail(th.getMessage());
        }
    }

    /*
     * 2. Test writer and reader
     * Write 1047 and Read 1042 frames.
     * Check only 1 file exists.
     * Check switchToMemory() returns false.
     * Check remaining() returns 5.
     * Read the remaining 5 frames.
     * Check switchToMemory() returns true.
     * Check that the reader returns null
     * Close the spiller.
     * Check files were deleted.
     */
    @org.junit.Test
    public void testWriteReadFixedSizeSpill() {
        try {
            removeSpillFiles();
            IHyracksTaskContext ctx = TestUtils.create(DEFAULT_FRAME_SIZE);
            FrameSpiller spiller = new FrameSpiller(ctx, TEST_DATAVERSE + "_" + TEST_FEED + "_" + TEST_DATASET,
                    new Long(NUM_FRAMES * DEFAULT_FRAME_SIZE));
            spiller.open();
            VSizeFrame frame = new VSizeFrame(ctx);
            for (int i = 0; i < 1047; i++) {
                spiller.spill(frame.getBuffer());
            }
            for (int i = 0; i < 1042; i++) {
                spiller.next();
            }
            Assert.assertEquals(5, spiller.remaining());
            Assert.assertEquals(1, countSpillFiles());
            Assert.assertEquals(false, spiller.switchToMemory());
            for (int i = 0; i < 4; i++) {
                spiller.next();
            }
            Assert.assertEquals(false, spiller.next() == null);
            Assert.assertEquals(true, spiller.next() == null);
            Assert.assertEquals(true, spiller.switchToMemory());
            Assert.assertEquals(1, countSpillFiles());
            spiller.close();
            Assert.assertEquals(0, spiller.remaining());
            Assert.assertEquals(0, countSpillFiles());
            Assert.assertEquals(true, spiller.switchToMemory());
        } catch (Throwable th) {
            th.printStackTrace();
            Assert.fail(th.getMessage());
        }
    }
}
