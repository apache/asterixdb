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
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnProjectionInfo;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.IColumnReadContext;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.IColumnWriteContext;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.read.DefaultColumnReadContext;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.write.DefaultColumnWriteContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationType;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.disk.ISweepContext;

public final class NoOpColumnIndexDiskCacheManager implements IColumnIndexDiskCacheManager {

    public static final IColumnIndexDiskCacheManager INSTANCE = new NoOpColumnIndexDiskCacheManager();

    private NoOpColumnIndexDiskCacheManager() {
    }

    @Override
    public void activate(int numberOfColumns, List<ILSMDiskComponent> diskComponents, IBufferCache bufferCache)
            throws HyracksDataException {
        // NoOp
    }

    @Override
    public IColumnWriteContext createWriteContext(int numberOfColumns, LSMIOOperationType operationType) {
        return DefaultColumnWriteContext.INSTANCE;
    }

    @Override
    public IColumnReadContext createReadContext(IColumnProjectionInfo projectionInfo) {
        return DefaultColumnReadContext.INSTANCE;
    }

    @Override
    public boolean isActive() {
        return false;
    }

    @Override
    public boolean isSweepable() {
        return false;
    }

    @Override
    public boolean prepareSweepPlan() {
        return false;
    }

    @Override
    public long sweep(ISweepContext context) {
        return 0;
    }

}
