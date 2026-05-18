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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVTreeFileManager;
import org.apache.hyracks.storage.am.lsm.vector.util.LSMVTreeTestHarness;
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

        LSMVTreeFileManager fileManager = new LSMVTreeFileManager(harness.getIOManager(), baseDir, null);
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

        LSMVTreeFileManager fileManager = new LSMVTreeFileManager(harness.getIOManager(), baseDir, null);
        List<LSMComponentFileReferences> valid = fileManager.cleanupAndGetValidFiles();

        assertEquals("all three flushed components survive", 3, valid.size());
        // LSM expects newest -> oldest.
        assertTrue(valid.get(0).getInsertIndexFileReference().getFile().getName().startsWith("2_2"));
        assertTrue(valid.get(2).getInsertIndexFileReference().getFile().getName().startsWith("0_0"));
        assertTrue(exists(baseDir, "0_0" + VCT));
        assertTrue(exists(baseDir, "2_2" + VCT));
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
