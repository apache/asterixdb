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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IFileHandle;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.IColumnWriteContext;
import org.apache.hyracks.storage.common.buffercache.context.write.DefaultBufferCacheWriteContext;

public final class DefaultColumnWriteContext implements IColumnWriteContext {
    public static final IColumnWriteContext INSTANCE = new DefaultColumnWriteContext();

    private DefaultColumnWriteContext() {
    }

    @Override
    public void startWritingColumn(int columnIndex, boolean overlapping) {
        // NoOp
    }

    @Override
    public void endWritingColumn(int columnIndex, int size) {
        // NoOp
    }

    @Override
    public void columnsPersisted() {
        // NoOp
    }

    @Override
    public void close() {
        // NoOp
    }

    @Override
    public int write(IOManager ioManager, IFileHandle handle, long offset, ByteBuffer data)
            throws HyracksDataException {
        return DefaultBufferCacheWriteContext.INSTANCE.write(ioManager, handle, offset, data);
    }

    @Override
    public long write(IOManager ioManager, IFileHandle handle, long offset, ByteBuffer[] data)
            throws HyracksDataException {
        return DefaultBufferCacheWriteContext.INSTANCE.write(ioManager, handle, offset, data);
    }
}
