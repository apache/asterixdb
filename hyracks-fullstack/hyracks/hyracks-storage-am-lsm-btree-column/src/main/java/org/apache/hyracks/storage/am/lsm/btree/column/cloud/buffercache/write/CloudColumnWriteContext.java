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
package org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.write;

import java.nio.ByteBuffer;
import java.util.BitSet;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IFileHandle;
import org.apache.hyracks.cloud.buffercache.context.DefaultCloudOnlyWriteContext;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.IColumnWriteContext;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.sweep.ColumnSweepPlanner;
import org.apache.hyracks.storage.common.buffercache.context.IBufferCacheWriteContext;
import org.apache.hyracks.storage.common.buffercache.context.write.DefaultBufferCacheWriteContext;
import org.apache.hyracks.storage.common.disk.IPhysicalDrive;

import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntSet;

public final class CloudColumnWriteContext implements IColumnWriteContext {
    private static final int INITIAL_NUMBER_OF_COLUMNS = 32;
    private final IPhysicalDrive drive;
    private final ColumnSweepPlanner planner;
    private final BitSet plan;
    private final IntSet indexedColumns;
    private int[] sizes;
    private int numberOfColumns;
    private IBufferCacheWriteContext currentContext;
    /**
     * writeAndSwap = true means the next call to write() will persist the page locally and swap to cloud-only
     * writer (i.e., the following pages will be written in the cloud but not locally).
     * <p>
     * 'writeAndSwap' is set to 'true' iff the previous column's last page should be persisted AND it is
     * 'overlapping' with this column's first page.
     */
    private boolean writeLocallyAndSwitchToCloudOnly;

    public CloudColumnWriteContext(IPhysicalDrive drive, ColumnSweepPlanner planner, int numberOfColumns) {
        this.drive = drive;
        this.planner = planner;
        this.plan = planner.getPlanCopy();
        this.indexedColumns = planner.getIndexedColumnsCopy();
        int initialLength =
                planner.getNumberOfPrimaryKeys() == numberOfColumns ? INITIAL_NUMBER_OF_COLUMNS : numberOfColumns;
        sizes = new int[initialLength];
        // Number of columns is not known during the flush operation
        this.numberOfColumns = 0;
        currentContext = DefaultBufferCacheWriteContext.INSTANCE;
    }

    @Override
    public void startWritingColumn(int columnIndex, boolean overlapping) {
        if (drive.isUnpressured() || indexedColumns.contains(columnIndex)) {
            // The current column will be persisted locally if free disk has space (drive.hasSpace() returns true)
            currentContext = DefaultBufferCacheWriteContext.INSTANCE;
        } else if (plan.get(columnIndex)) {
            // This column was planned for eviction, do not persist.
            if (overlapping && currentContext == DefaultBufferCacheWriteContext.INSTANCE) {
                // The previous column's last page should be persisted AND it is overlapping with the current column's
                // first page. Persist the first page locally and switch to cloud-only writer.
                writeLocallyAndSwitchToCloudOnly = true;
            } else {
                // The previous column's last page is not overlapping. Switch to cloud-only writer (if not already)
                currentContext = DefaultCloudOnlyWriteContext.INSTANCE;
            }
        } else {
            // Local drive is pressured. Write to cloud only.
            currentContext = DefaultCloudOnlyWriteContext.INSTANCE;
        }
    }

    @Override
    public void endWritingColumn(int columnIndex, int size) {
        ensureCapacity(columnIndex);
        sizes[columnIndex] = Math.max(sizes[columnIndex], size);
    }

    @Override
    public void columnsPersisted() {
        // Set the default writer context to persist pageZero and the interior nodes' pages locally
        currentContext = DefaultBufferCacheWriteContext.INSTANCE;
        writeLocallyAndSwitchToCloudOnly = false;
    }

    @Override
    public void close() {
        // Report the sizes of the written columns
        planner.adjustColumnSizes(sizes, numberOfColumns);
    }

    /*
     * ************************************************************************************************
     * WRITE methods
     * ************************************************************************************************
     */

    @Override
    public int write(IOManager ioManager, IFileHandle handle, long offset, ByteBuffer data)
            throws HyracksDataException {
        int writtenBytes = currentContext.write(ioManager, handle, offset, data);
        switchIfNeeded();
        return writtenBytes;
    }

    @Override
    public long write(IOManager ioManager, IFileHandle handle, long offset, ByteBuffer[] data)
            throws HyracksDataException {
        long writtenBytes = currentContext.write(ioManager, handle, offset, data);
        switchIfNeeded();
        return writtenBytes;
    }

    /*
     * ************************************************************************************************
     * helper methods
     * ************************************************************************************************
     */

    private void ensureCapacity(int columnIndex) {
        int length = sizes.length;
        if (columnIndex >= length) {
            sizes = IntArrays.grow(sizes, columnIndex + 1);
        }

        numberOfColumns = Math.max(numberOfColumns, columnIndex + 1);
    }

    private void switchIfNeeded() {
        if (writeLocallyAndSwitchToCloudOnly) {
            // Switch to cloud-only writer
            currentContext = DefaultCloudOnlyWriteContext.INSTANCE;
            writeLocallyAndSwitchToCloudOnly = false;
        }
    }
}
