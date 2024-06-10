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
package org.apache.hyracks.storage.am.lsm.btree.column.cloud;

import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.cloud.sweeper.SweepContext;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.ColumnProjectorType;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnProjectionInfo;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnTupleProjector;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.IColumnReadContext;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.IColumnWriteContext;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.read.CloudColumnReadContext;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.read.DefaultColumnReadContext;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.write.CloudColumnWriteContext;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.sweep.ColumnSweepPlanner;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.sweep.ColumnSweeper;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationType;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.disk.IPhysicalDrive;
import org.apache.hyracks.storage.common.disk.ISweepContext;

/**
 * The disk manager cannot be shared among different partitions as columns are local to each partition.
 * For example, column 9 in partition 0 corresponds to "salary" while column 9 in partition 1 corresponds to "age".
 */
public final class CloudColumnIndexDiskCacheManager implements IColumnIndexDiskCacheManager {
    private final IColumnTupleProjector sweepProjector;
    private final IPhysicalDrive drive;
    private final ColumnSweepPlanner planner;
    private final ColumnSweeper sweeper;

    public CloudColumnIndexDiskCacheManager(int numberOfPrimaryKeys, IColumnTupleProjector sweepProjector,
            IPhysicalDrive drive) {
        this.sweepProjector = sweepProjector;
        this.drive = drive;
        planner = new ColumnSweepPlanner(numberOfPrimaryKeys, System::nanoTime);
        sweeper = new ColumnSweeper(numberOfPrimaryKeys);
    }

    @Override
    public void activate(int numberOfColumns, List<ILSMDiskComponent> diskComponents, IBufferCache bufferCache)
            throws HyracksDataException {
        planner.onActivate(numberOfColumns, diskComponents, sweepProjector, bufferCache);
    }

    @Override
    public IColumnWriteContext createWriteContext(int numberOfColumns, LSMIOOperationType operationType) {
        return new CloudColumnWriteContext(drive, planner, numberOfColumns);
    }

    @Override
    public IColumnReadContext createReadContext(IColumnProjectionInfo projectionInfo) {
        ColumnProjectorType projectorType = projectionInfo.getProjectorType();
        if (projectorType == ColumnProjectorType.QUERY) {
            planner.access(projectionInfo);
        } else if (projectorType == ColumnProjectorType.MODIFY) {
            planner.setIndexedColumns(projectionInfo);
            // Requested (and indexed) columns will be persisted if space permits
            return DefaultColumnReadContext.INSTANCE;
        }
        return new CloudColumnReadContext(projectionInfo, drive, planner.getPlanCopy());
    }

    @Override
    public boolean isActive() {
        return planner.isActive();
    }

    @Override
    public boolean isSweepable() {
        return true;
    }

    @Override
    public boolean prepareSweepPlan() {
        return planner.plan();
    }

    @Override
    public long sweep(ISweepContext context) throws HyracksDataException {
        SweepContext sweepContext = (SweepContext) context;
        return sweeper.sweep(planner.getPlanCopy(), sweepContext, sweepProjector);
    }
}
