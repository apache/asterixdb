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

import org.apache.hyracks.util.ExitUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FIFOLocalWriter implements IFIFOPageWriter {
    private static final Logger LOGGER = LogManager.getLogger();
    public static final FIFOLocalWriter INSTANCE = new FIFOLocalWriter();
    private static final boolean DEBUG = false;

    private FIFOLocalWriter() {
    }

    @SuppressWarnings("squid:S1181") // System must halt on all IO errors
    @Override
    public void write(ICachedPage page, BufferCache bufferCache) {
        CachedPage cPage = (CachedPage) page;
        try {
            bufferCache.write(cPage);
        } catch (Exception e) {
            page.writeFailed(e);
            LOGGER.warn("Failed to write page {}", cPage, e);
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
