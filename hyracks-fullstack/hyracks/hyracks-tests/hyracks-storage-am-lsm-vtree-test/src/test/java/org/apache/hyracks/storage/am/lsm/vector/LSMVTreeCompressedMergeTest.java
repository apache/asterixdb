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
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.hyracks.api.compression.ICompressorDecompressorFactory;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.storage.common.compression.SnappyCompressorDecompressorFactory;

/**
 * {@link LSMVTreeMergeTest} with Snappy page compression on the data components: bulk load, two flushes and a
 * merge all write through the look-aside file, search reads back through it, and reactivation rediscovers the
 * merged component as compressed.
 */
public class LSMVTreeCompressedMergeTest extends LSMVTreeMergeTest {

    @Override
    protected ICompressorDecompressorFactory compressorDecompressorFactory() {
        return new SnappyCompressorDecompressorFactory();
    }

    @Override
    protected void verifyComponentFiles(FileReference indexDir) {
        String[] components = indexDir.getFile().list((dir, name) -> name.endsWith("_vct"));
        assertEquals("the merge leaves exactly one data component", 1, components.length);
        assertTrue("the data component has its look-aside file",
                new File(indexDir.getFile(), components[0] + ".dic").exists());
        String[] lafs = indexDir.getFile().list((dir, name) -> name.endsWith(".dic"));
        assertEquals("the merged-away inputs' look-aside files are gone", 1, lafs.length);
        String[] staticStructureLaf = indexDir.getFile().list((dir, name) -> name.startsWith(".staticstructure."));
        assertEquals("the static structure is never compressed", 0, staticStructureLaf.length);
    }
}
