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
package org.apache.asterix.cloud.lazy;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.List;

import org.apache.asterix.cloud.UncachedFileReference;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.junit.Test;

/**
 * The uncached count is polled for metrics, so it is read without the cacher's lock; it must still follow every way a
 * file enters or leaves the uncached set.
 */
public class ParallelCacherCountTest {

    private static final IODeviceHandle DEVICE = new IODeviceHandle(new File("/mnt/d0"), "."); // NOSONAR
    private static final String INDEX = "storage/partition_3/Default/dv/ds/0/ds/";

    @Test
    public void countsDataAndMetadataFiles() {
        ParallelCacher cacher = new ParallelCacher(null, List.of(file("2026-10-27-1_2026-10-27-1_b", 100),
                file("2026-10-27-1_2026-10-27-1_f", 10), file(".metadata", 1)), false);
        assertEquals(3, cacher.getUncachedFileCount());
        assertEquals(111L, cacher.getUncachedTotalSize());
    }

    @Test
    public void followsRemoveAndAdd() {
        FileReference data = file("2026-10-27-1_2026-10-27-1_b", 100);
        ParallelCacher cacher = new ParallelCacher(null, List.of(data, file(".metadata", 1)), false);
        cacher.remove(data);
        assertEquals(1, cacher.getUncachedFileCount());
        cacher.add(List.of(data));
        assertEquals(2, cacher.getUncachedFileCount());
    }

    private static FileReference file(String name, long size) {
        return new UncachedFileReference(DEVICE, INDEX + name, size);
    }
}
