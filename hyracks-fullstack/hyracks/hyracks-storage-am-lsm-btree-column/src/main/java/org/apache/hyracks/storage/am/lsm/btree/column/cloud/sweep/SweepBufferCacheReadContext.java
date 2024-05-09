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
package org.apache.hyracks.storage.am.lsm.btree.column.cloud.sweep;

import static org.apache.hyracks.cloud.buffercache.context.DefaultCloudReadContext.readAndPersistPage;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.storage.common.buffercache.BufferCacheHeaderHelper;
import org.apache.hyracks.storage.common.buffercache.CachedPage;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.context.IBufferCacheReadContext;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.hyracks.util.annotations.ThreadSafe;

@ThreadSafe
final class SweepBufferCacheReadContext implements IBufferCacheReadContext {
    static final IBufferCacheReadContext INSTANCE = new SweepBufferCacheReadContext();

    private SweepBufferCacheReadContext() {
    }

    @Override
    public void onPin(ICachedPage page) {
        // NoOp
    }

    @Override
    public void onUnpin(ICachedPage page) {
        // NoOp
    }

    @Override
    public boolean isNewPage() {
        return false;
    }

    @Override
    public boolean incrementStats() {
        // Do not increment the stats for the sweeper
        return false;
    }

    @Override
    public ByteBuffer processHeader(IOManager ioManager, BufferedFileHandle fileHandle, BufferCacheHeaderHelper header,
            CachedPage cPage) throws HyracksDataException {
        // Will not persist as the disk is pressured
        return readAndPersistPage(ioManager, fileHandle, header, cPage, false);
    }
}
