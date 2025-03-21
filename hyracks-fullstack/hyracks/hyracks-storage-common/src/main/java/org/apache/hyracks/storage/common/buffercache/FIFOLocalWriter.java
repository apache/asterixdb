/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hyracks.storage.common.buffercache;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.common.buffercache.context.IBufferCacheWriteContext;
import org.apache.hyracks.util.ExitUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FIFOLocalWriter implements IFIFOPageWriter {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final boolean DEBUG = false;

    private final BufferCache bufferCache;

    private final IPageWriteCallback callback;
    private final IPageWriteFailureCallback failureCallback;
    private final IBufferCacheWriteContext context;

    public FIFOLocalWriter(BufferCache bufferCache, IPageWriteCallback callback,
            IPageWriteFailureCallback failureCallback, IBufferCacheWriteContext context) {
        this.bufferCache = bufferCache;
        this.callback = callback;
        this.failureCallback = failureCallback;
        this.context = context;
    }

    @SuppressWarnings("squid:S1181")
    @Override
    public void write(ICachedPage page) throws HyracksDataException {
        CachedPage cPage = (CachedPage) page;
        try {
            callback.beforeWrite(cPage);
            bufferCache.write(cPage, context);
            callback.afterWrite(cPage);
        } catch (HyracksDataException e) {
            LOGGER.warn("Failed to write page {}", cPage, e);
            throw HyracksDataException.create(ErrorCode.FAILED_IO_OPERATION, e);
        } catch (Throwable th) {
            // Halt
            LOGGER.error("FIFOLocalWriter has encountered a fatal error", th);
            ExitUtil.halt(ExitUtil.EC_ABNORMAL_TERMINATION);
        } finally {
            bufferCache.returnPage(cPage);
            if (DEBUG) {
                LOGGER.error("[FIFO] Return page: {}, {}", cPage.cpid, cPage.dpid);
            }
        }
    }
}
