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
package org.apache.hyracks.storage.common.disk;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.context.IBufferCacheReadContext;

/**
 * Provides the necessary {@link IBufferCache} functionalities for a sweep operation
 */
public interface ISweepContext {

    /**
     * Open a file for sweeping
     *
     * @param fileId to open
     */
    void open(int fileId) throws HyracksDataException;

    /**
     * Close the opened file
     */
    void close() throws HyracksDataException;

    /**
     * Pin a page
     *
     * @param dpid    page unique ID
     * @param bcOpCtx read context
     * @return pinned page
     */
    ICachedPage pin(long dpid, IBufferCacheReadContext bcOpCtx) throws HyracksDataException;

    /**
     * Unpin a page
     *
     * @param page    to unpin
     * @param bcOpCtx read context
     */
    void unpin(ICachedPage page, IBufferCacheReadContext bcOpCtx) throws HyracksDataException;
}
