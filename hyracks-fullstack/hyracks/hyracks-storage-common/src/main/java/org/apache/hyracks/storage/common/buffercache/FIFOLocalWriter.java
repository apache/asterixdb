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

import org.apache.hyracks.api.exceptions.HyracksDataException;

public class FIFOLocalWriter implements IFIFOPageWriter {
    private static FIFOLocalWriter instance;
    private static boolean DEBUG = false;

    public static FIFOLocalWriter instance() {
        if(instance == null) {
            instance = new FIFOLocalWriter();
        }
        return instance;
    }

    @Override
    public void write(ICachedPage page, BufferCache bufferCache) throws HyracksDataException {
        CachedPage cPage = (CachedPage)page;
        try {
            bufferCache.write(cPage);
        } finally {
            bufferCache.returnPage(cPage);
            if (DEBUG) {
                System.out.println("[FIFO] Return page: " + cPage.cpid + "," + cPage.dpid);
            }
        }
    }

    @Override
    public void sync(int fileId, BufferCache bufferCache) throws HyracksDataException {
        bufferCache.force(fileId,true);
    }
}
