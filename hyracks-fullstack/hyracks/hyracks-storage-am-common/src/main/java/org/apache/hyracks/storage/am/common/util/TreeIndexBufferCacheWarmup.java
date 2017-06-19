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
package org.apache.hyracks.storage.am.common.util;

import java.util.ArrayList;
import java.util.Random;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetadataFrame;
import org.apache.hyracks.storage.common.arraylist.IntArrayList;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.hyracks.util.MathUtil;

public class TreeIndexBufferCacheWarmup {
    private final IBufferCache bufferCache;
    private final IMetadataPageManager freePageManager;
    private final FileReference fileRef;
    private final ArrayList<IntArrayList> pagesByLevel = new ArrayList<>();
    private final Random rnd = new Random();

    public TreeIndexBufferCacheWarmup(IBufferCache bufferCache, IMetadataPageManager freePageManager,
            FileReference fileRef) {
        this.bufferCache = bufferCache;
        this.freePageManager = freePageManager;
        this.fileRef = fileRef;
    }

    public void warmup(ITreeIndexFrame frame, ITreeIndexMetadataFrame metaFrame, int[] warmupTreeLevels,
            int[] warmupRepeats) throws HyracksDataException {
        int fileId = bufferCache.openFile(fileRef);

        // scan entire file to determine pages in each level
        int maxPageId = freePageManager.getMaxPageId(metaFrame);
        for (int pageId = 0; pageId <= maxPageId; pageId++) {
            ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), false);
            page.acquireReadLatch();
            try {
                frame.setPage(page);
                byte level = frame.getLevel();
                while (level >= pagesByLevel.size()) {
                    pagesByLevel.add(new IntArrayList(100, 100));
                }
                if (level >= 0) {
                    pagesByLevel.get(level).add(pageId);
                }
            } finally {
                page.releaseReadLatch();
                bufferCache.unpin(page);
            }
        }

        // pin certain pages again to simulate frequent access
        for (int i = 0; i < warmupTreeLevels.length; i++) {
            if (warmupTreeLevels[i] < pagesByLevel.size()) {
                int repeats = warmupRepeats[i];
                IntArrayList pageIds = pagesByLevel.get(warmupTreeLevels[i]);
                int[] remainingPageIds = new int[pageIds.size()];
                for (int r = 0; r < repeats; r++) {
                    for (int j = 0; j < pageIds.size(); j++) {
                        remainingPageIds[j] = pageIds.get(j);
                    }

                    int remainingLength = pageIds.size();
                    for (int j = 0; j < pageIds.size(); j++) {
                        int index = MathUtil.stripSignBit(rnd.nextInt()) % remainingLength;
                        int pageId = remainingPageIds[index];

                        // pin & latch then immediately unlatch & unpin
                        ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), false);
                        page.acquireReadLatch();
                        page.releaseReadLatch();
                        bufferCache.unpin(page);

                        remainingPageIds[index] = remainingPageIds[remainingLength - 1];
                        remainingLength--;
                    }
                }
            }
        }

        bufferCache.closeFile(fileId);
    }
}
