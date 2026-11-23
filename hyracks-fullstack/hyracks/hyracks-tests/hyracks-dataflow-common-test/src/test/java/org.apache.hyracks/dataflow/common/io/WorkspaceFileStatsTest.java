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
package org.apache.hyracks.dataflow.common.io;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IFileHandle;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.api.io.WorkspaceFileKind;
import org.apache.hyracks.control.nc.io.DefaultDeviceResolver;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.control.nc.io.WorkspaceFileStats;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Checks that the I/O on workspace files is counted by kind, and only on workspace files.
 */
public class WorkspaceFileStatsTest {

    private static final int FRAME_SIZE = 4096;

    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    private IOManager ioManager;
    private WorkspaceFileStats stats;

    @Before
    public void setUp() throws Exception {
        IODeviceHandle device = new IODeviceHandle(folder.newFolder("device"), "workspace");
        ioManager = new IOManager(List.of(device), new DefaultDeviceResolver(), 1, 10);
        stats = ioManager.getWorkspaceFileStats();
    }

    @After
    public void tearDown() throws Exception {
        ioManager.close();
    }

    @Test
    public void countsWorkspaceFileIoByKind() throws Exception {
        FileReference file = ioManager.createWorkspaceFile("sort-run", WorkspaceFileKind.SORT);
        Assert.assertEquals(WorkspaceFileKind.SORT, file.getWorkspaceKind());
        Assert.assertEquals(1, stats.getFilesCreated(WorkspaceFileKind.SORT));

        RunFileWriter writer = new RunFileWriter(file, ioManager);
        writer.open();
        writer.nextFrame(ByteBuffer.allocate(FRAME_SIZE));
        writer.nextFrame(ByteBuffer.allocate(FRAME_SIZE));
        writer.close();
        Assert.assertEquals(2L * FRAME_SIZE, stats.getBytesWritten(WorkspaceFileKind.SORT));

        IFileHandle handle = ioManager.open(file, IIOManager.FileReadWriteMode.READ_ONLY,
                IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);
        try {
            ByteBuffer buffer = ByteBuffer.allocate(FRAME_SIZE);
            Assert.assertEquals(FRAME_SIZE, ioManager.syncRead(handle, 0, buffer));
            buffer.clear();
            // reading past the end returns -1, which must not be subtracted
            Assert.assertEquals(-1, ioManager.syncRead(handle, 2L * FRAME_SIZE, buffer));
        } finally {
            ioManager.close(handle);
        }
        Assert.assertEquals(FRAME_SIZE, stats.getBytesRead(WorkspaceFileKind.SORT));

        for (WorkspaceFileKind other : WorkspaceFileKind.values()) {
            if (other != WorkspaceFileKind.SORT) {
                Assert.assertEquals(other.name(), 0, stats.getFilesCreated(other));
                Assert.assertEquals(other.name(), 0, stats.getBytesWritten(other));
                Assert.assertEquals(other.name(), 0, stats.getBytesRead(other));
            }
        }
    }

    @Test
    public void ignoresFilesOutsideTheWorkspace() throws Exception {
        FileReference file = ioManager.getIODevices().get(0).createFileRef("not-a-workspace-file");
        Assert.assertNull(file.getWorkspaceKind());
        Assert.assertTrue(file.getFile().createNewFile());
        RunFileWriter writer = new RunFileWriter(file, ioManager);
        writer.open();
        writer.nextFrame(ByteBuffer.allocate(FRAME_SIZE));
        writer.close();
        for (WorkspaceFileKind kind : WorkspaceFileKind.values()) {
            Assert.assertEquals(kind.name(), 0, stats.getBytesWritten(kind));
        }
        Assert.assertEquals(0, ioManager.getWorkspaceUsage().files());
    }

    @Test
    public void usageReflectsFilesPresentNow() throws Exception {
        Assert.assertEquals(new WorkspaceFileStats.Usage(0, 0), ioManager.getWorkspaceUsage());
        FileReference join = ioManager.createWorkspaceFile("join", WorkspaceFileKind.JOIN);
        FileReference group = ioManager.createWorkspaceFile("group", WorkspaceFileKind.GROUP);
        RunFileWriter writer = new RunFileWriter(join, ioManager);
        writer.open();
        writer.nextFrame(ByteBuffer.allocate(FRAME_SIZE));
        writer.close();
        Assert.assertEquals(new WorkspaceFileStats.Usage(2, FRAME_SIZE), ioManager.getWorkspaceUsage());

        Assert.assertTrue(join.delete());
        Assert.assertTrue(group.delete());
        Assert.assertEquals(new WorkspaceFileStats.Usage(0, 0), ioManager.getWorkspaceUsage());
        // the cumulative counts are unaffected by deletion
        Assert.assertEquals(1, stats.getFilesCreated(WorkspaceFileKind.JOIN));
        Assert.assertEquals(FRAME_SIZE, stats.getBytesWritten(WorkspaceFileKind.JOIN));
    }

    @Test
    public void countsEachKindAJobSpilledOnce() {
        stats.jobSpilled((1 << WorkspaceFileKind.SORT.ordinal()) | (1 << WorkspaceFileKind.JOIN.ordinal()));
        stats.jobSpilled(1 << WorkspaceFileKind.SORT.ordinal());
        Assert.assertEquals(2, stats.getJobsSpilled(WorkspaceFileKind.SORT));
        Assert.assertEquals(1, stats.getJobsSpilled(WorkspaceFileKind.JOIN));
        Assert.assertEquals(0, stats.getJobsSpilled(WorkspaceFileKind.GROUP));
    }
}
