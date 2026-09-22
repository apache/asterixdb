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
package org.apache.asterix.app.nc;

import java.io.File;
import java.nio.file.Files;
import java.util.Collections;

import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.control.nc.io.DefaultDeviceResolver;
import org.apache.hyracks.control.nc.io.IOManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class IndexCheckpointManagerTest {

    private File root;
    private IOManager ioManager;
    private IndexCheckpointManager checkpointManager;

    @Before
    public void setUp() throws Exception {
        root = Files.createTempDirectory("index_checkpoint_manager_test").toFile();
        ioManager = new IOManager(Collections.singletonList(new IODeviceHandle(root, "iodev")),
                new DefaultDeviceResolver(), 2, 10);
        FileReference indexPath = ioManager.resolve("index");
        ioManager.makeDirectories(indexPath);
        checkpointManager = new IndexCheckpointManager(indexPath, ioManager);
    }

    @After
    public void tearDown() throws Exception {
        ioManager.close();
        FileUtils.deleteDirectory(root);
    }

    /**
     * Rolling back an atomic statement deletes the checkpoint it wrote, which must not survive as the cached latest
     * checkpoint: the one before it is what the disk holds now.
     */
    @Test
    public void testLatestAfterDeleteLatest() throws Exception {
        checkpointManager.init(0, 10, 1, false, null);
        checkpointManager.flushed(1, 20, 2);
        checkpointManager.flushed(2, 30, 3);
        Assert.assertEquals(3, checkpointManager.getLatest().getLastComponentId());

        checkpointManager.deleteLatest(3);
        Assert.assertEquals(2, checkpointManager.getLatest().getLastComponentId());
        Assert.assertEquals(20, checkpointManager.getLowWatermark());
        Assert.assertEquals(1, checkpointManager.getValidComponentSequence());
    }

    /**
     * A write whose read-back succeeded is what the next read returns, without the disk being consulted again.
     */
    @Test
    public void testLatestFollowsWrites() throws Exception {
        checkpointManager.init(0, 10, 1, false, null);
        Assert.assertEquals(1, checkpointManager.getLatest().getLastComponentId());
        checkpointManager.flushed(1, 20, 2);
        Assert.assertEquals(2, checkpointManager.getLatest().getLastComponentId());
        checkpointManager.setLastComponentId(5);
        Assert.assertEquals(5, checkpointManager.getLatest().getLastComponentId());
        Assert.assertEquals(20, checkpointManager.getLowWatermark());

        checkpointManager.delete();
        Assert.assertFalse(checkpointManager.isValidIndex());
        checkpointManager.init(0, 40, 7, false, null);
        Assert.assertEquals(7, checkpointManager.getLatest().getLastComponentId());
        Assert.assertEquals(40, checkpointManager.getLowWatermark());
    }
}
