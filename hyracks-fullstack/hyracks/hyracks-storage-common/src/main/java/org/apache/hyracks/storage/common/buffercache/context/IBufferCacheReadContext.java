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
package org.apache.hyracks.storage.common.buffercache.context;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.storage.common.buffercache.BufferCacheHeaderHelper;
import org.apache.hyracks.storage.common.buffercache.CachedPage;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

/**
 * Provide a context to {@link IBufferCache} pin/unpin operations as well as processing the header of the first
 * pages when the {@link IBufferCache} requires to read from disk
 */
public interface IBufferCacheReadContext {
    /**
     * Signals a page was found and {@link IBufferCache} is about to pin the requested page
     *
     * @param page that will be pinned
     */
    void onPin(ICachedPage page) throws HyracksDataException;

    /**
     * Signals that a page will be unpinned
     *
     * @param page that will be unpinned
     */
    void onUnpin(ICachedPage page);

    /**
     * @return true if pinning a new page, false otherwise
     */
    boolean isNewPage();

    /**
     * @return true to increment {@link IBufferCache} stats, false otherwise
     */
    boolean incrementStats();

    /**
     * Processing the header of a read page during a pin operation
     * Note: This operation modifies the position and limit of the header
     *
     * @param ioManager  I/O manager
     * @param fileHandle file the page was read from
     * @param header     header of the page
     * @param cPage      the pinned page
     * @return the byte buffer of the header after processing it
     */
    ByteBuffer processHeader(IOManager ioManager, BufferedFileHandle fileHandle, BufferCacheHeaderHelper header,
            CachedPage cPage) throws HyracksDataException;
}
