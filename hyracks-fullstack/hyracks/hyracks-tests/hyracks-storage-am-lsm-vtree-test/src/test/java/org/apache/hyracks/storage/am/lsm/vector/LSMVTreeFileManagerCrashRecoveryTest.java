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

package org.apache.hyracks.storage.am.lsm.vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;

import org.apache.hyracks.api.compression.ICompressorDecompressorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import org.apache.hyracks.storage.am.lsm.common.impls.TreeIndexFactory;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVTreeFileManager;
import org.apache.hyracks.storage.am.lsm.vector.util.LSMVTreeTestHarness;
import org.apache.hyracks.storage.common.compression.NoOpCompressorDecompressorFactory;
import org.apache.hyracks.storage.common.compression.SnappyCompressorDecompressorFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Recovery test for {@link LSMVTreeFileManager#cleanupAndGetValidFiles()} (ASTERIXDB-3754).
 *
 * <p>A merge writes the merged component and then deletes its inputs as a <em>separate</em> step. A crash
 * in between leaves the pre-merge inputs on disk alongside the merged component. On the next
 * activation, {@code cleanupAndGetValidFiles()} must return only the merged component and delete the
 * superseded inputs — otherwise the index would load both, duplicating every record and resurfacing
 * deletes the merge reconciled away.
 *
 * <p>This is a file-manager-level test: it stages the exact post-crash on-disk file set (which is hard to
 * produce deterministically end-to-end, since a real merge deletes the inputs) and asserts the cleanup
 * reconciles it. Pre-fix the override returned all four components and deleted nothing.
 */
public class LSMVTreeFileManagerCrashRecoveryTest {

    private static final String VCT = "_vct";
    private static final String STATIC_STRUCTURE = ".staticstructure";
    private static final String LAF = ".dic";

    private final LSMVTreeTestHarness harness = new LSMVTreeTestHarness();

    @Before
    public void setUp() throws HyracksDataException {
        harness.setUp();
    }

    @After
    public void tearDown() throws HyracksDataException {
        harness.tearDown();
    }

    @Test
    public void crashAfterMergeDropsSupersededComponents() throws Exception {
        FileReference baseDir = harness.getFileReference();
        baseDir.getFile().mkdirs();

        // Post-crash on-disk state: the merged component [0_2] PLUS its un-deleted pre-merge inputs
        // [0_0], [1_1], [2_2], and the shared static structure.
        create(baseDir, "0_0" + VCT);
        create(baseDir, "1_1" + VCT);
        create(baseDir, "2_2" + VCT);
        create(baseDir, "0_2" + VCT); // merged component (range 0..2)
        create(baseDir, STATIC_STRUCTURE);

        LSMVTreeFileManager fileManager = fileManager(baseDir, NoOpCompressorDecompressorFactory.INSTANCE);
        List<LSMComponentFileReferences> valid = fileManager.cleanupAndGetValidFiles();

        // Only the merged component is valid.
        assertEquals("only the merged component should survive", 1, valid.size());
        assertTrue("survivor should be the merged [0_2] component",
                valid.get(0).getInsertIndexFileReference().getFile().getName().startsWith("0_2"));

        // The three superseded pre-merge inputs must have been deleted...
        assertFalse("pre-merge input 0_0 must be deleted", exists(baseDir, "0_0" + VCT));
        assertFalse("pre-merge input 1_1 must be deleted", exists(baseDir, "1_1" + VCT));
        assertFalse("pre-merge input 2_2 must be deleted", exists(baseDir, "2_2" + VCT));
        // ...while the merged component and the shared static structure are kept.
        assertTrue("merged component 0_2 must be kept", exists(baseDir, "0_2" + VCT));
        assertTrue("shared static structure must be kept", exists(baseDir, STATIC_STRUCTURE));
    }

    /** Sanity: with no overlap (plain flushes), all components survive, newest-first. */
    @Test
    public void nonOverlappingComponentsAllSurvive() throws Exception {
        FileReference baseDir = harness.getFileReference();
        baseDir.getFile().mkdirs();
        create(baseDir, "0_0" + VCT);
        create(baseDir, "1_1" + VCT);
        create(baseDir, "2_2" + VCT);
        create(baseDir, STATIC_STRUCTURE);

        LSMVTreeFileManager fileManager = fileManager(baseDir, NoOpCompressorDecompressorFactory.INSTANCE);
        List<LSMComponentFileReferences> valid = fileManager.cleanupAndGetValidFiles();

        assertEquals("all three flushed components survive", 3, valid.size());
        // LSM expects newest -> oldest.
        assertTrue(valid.get(0).getInsertIndexFileReference().getFile().getName().startsWith("2_2"));
        assertTrue(valid.get(2).getInsertIndexFileReference().getFile().getName().startsWith("0_0"));
        assertTrue(exists(baseDir, "0_0" + VCT));
        assertTrue(exists(baseDir, "2_2" + VCT));
    }

    /** A compressed merge leaves each input's look-aside file behind too; those go with their data files. */
    @Test
    public void compressedCrashAfterMergeDropsInputLookAsideFiles() throws Exception {
        FileReference baseDir = harness.getFileReference();
        baseDir.getFile().mkdirs();
        for (String seq : new String[] { "0_0", "1_1", "0_1" }) {
            create(baseDir, seq + VCT);
            create(baseDir, seq + VCT + LAF);
        }
        create(baseDir, STATIC_STRUCTURE);

        List<LSMComponentFileReferences> valid = compressedFileManager(baseDir).cleanupAndGetValidFiles();

        assertEquals(1, valid.size());
        assertTrue("survivor must open through its look-aside file",
                valid.get(0).getInsertIndexFileReference().isCompressed());
        assertTrue(exists(baseDir, "0_1" + VCT));
        assertTrue(exists(baseDir, "0_1" + VCT + LAF));
        for (String seq : new String[] { "0_0", "1_1" }) {
            assertFalse(seq + " data must be deleted", exists(baseDir, seq + VCT));
            assertFalse(seq + " look-aside file must be deleted", exists(baseDir, seq + VCT + LAF));
        }
        assertTrue("static structure is never compressed and must be kept", exists(baseDir, STATIC_STRUCTURE));
    }

    /**
     * A half-finished delete leaves a data file without its look-aside file, or the reverse. Neither is readable,
     * so both are finished off; an intact compressed component is kept.
     */
    @Test
    public void compressedComponentWithoutPartnerIsDropped() throws Exception {
        FileReference baseDir = harness.getFileReference();
        baseDir.getFile().mkdirs();
        create(baseDir, "0_0" + VCT);
        create(baseDir, "0_0" + VCT + LAF);
        create(baseDir, "1_1" + VCT); // look-aside file already deleted
        create(baseDir, "2_2" + VCT + LAF); // data file already deleted
        create(baseDir, STATIC_STRUCTURE);

        List<LSMComponentFileReferences> valid = compressedFileManager(baseDir).cleanupAndGetValidFiles();

        assertEquals(1, valid.size());
        assertTrue(valid.get(0).getInsertIndexFileReference().getFile().getName().startsWith("0_0"));
        assertFalse(exists(baseDir, "1_1" + VCT));
        assertFalse(exists(baseDir, "2_2" + VCT + LAF));
        assertTrue(exists(baseDir, "0_0" + VCT + LAF));
    }

    private LSMVTreeFileManager compressedFileManager(FileReference baseDir) {
        return fileManager(baseDir, new SnappyCompressorDecompressorFactory());
    }

    /** The file manager deletes through the buffer cache, so it needs a factory that carries the real one. */
    private LSMVTreeFileManager fileManager(FileReference baseDir, ICompressorDecompressorFactory compression) {
        TreeIndexFactory<ITreeIndex> factory = new TreeIndexFactory<>(harness.getIOManager(),
                harness.getDiskBufferCache(), null, null, null, null, 0) {
            @Override
            public ITreeIndex createIndexInstance(FileReference file) {
                throw new UnsupportedOperationException();
            }
        };
        return new LSMVTreeFileManager(harness.getIOManager(), baseDir, factory, compression);
    }

    private static void create(FileReference baseDir, String name) throws Exception {
        File f = baseDir.getChild(name).getFile();
        f.getParentFile().mkdirs();
        assertTrue("failed to stage " + name, f.createNewFile() || f.exists());
    }

    private static boolean exists(FileReference baseDir, String name) {
        return baseDir.getChild(name).getFile().exists();
    }
}
