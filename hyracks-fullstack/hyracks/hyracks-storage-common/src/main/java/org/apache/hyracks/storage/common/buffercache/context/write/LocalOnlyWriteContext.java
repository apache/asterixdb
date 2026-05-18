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
package org.apache.hyracks.storage.common.buffercache.context.write;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IFileHandle;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.storage.common.buffercache.context.IBufferCacheWriteContext;

/**
 * Writes pages to local storage only, deferring any long-term (cloud) upload. Off-cloud this is an
 * ordinary local write; in cloud mode it skips the upload (see {@code IOManager#localWriteOnly}).
 * <p>
 * Intended for the first pass of a multi-pass writer that publishes pages locally, mutates them, and
 * uploads them exactly once in a later pass — required because the cloud writer is append-only, so a
 * page must not be uploaded before its final bytes are known.
 */
public final class LocalOnlyWriteContext implements IBufferCacheWriteContext {
    public static final IBufferCacheWriteContext INSTANCE = new LocalOnlyWriteContext();

    private LocalOnlyWriteContext() {
    }

    @Override
    public int write(IOManager ioManager, IFileHandle handle, long offset, ByteBuffer data)
            throws HyracksDataException {
        return ioManager.localWriteOnly(handle, offset, data);
    }

    @Override
    public long write(IOManager ioManager, IFileHandle handle, long offset, ByteBuffer[] data)
            throws HyracksDataException {
        return ioManager.localWriteOnly(handle, offset, data);
    }
}
