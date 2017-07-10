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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.storage.am.common.api.IPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetadataFrame;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

public class TreeIndexStatsGatherer {

    private final TreeIndexStats treeIndexStats = new TreeIndexStats();
    private final IBufferCache bufferCache;
    private final IPageManager freePageManager;
    private final FileReference fileRef;
    private final int rootPage;

    public TreeIndexStatsGatherer(IBufferCache bufferCache, IPageManager freePageManager, FileReference fileRef,
            int rootPage) {
        this.bufferCache = bufferCache;
        this.freePageManager = freePageManager;
        this.fileRef = fileRef;
        this.rootPage = rootPage;
    }

    public TreeIndexStats gatherStats(ITreeIndexFrame leafFrame, ITreeIndexFrame interiorFrame,
            ITreeIndexMetadataFrame metaFrame) throws HyracksDataException {
        int fileId = bufferCache.openFile(fileRef);
        treeIndexStats.begin();
        int maxPageId = freePageManager.getMaxPageId(metaFrame);
        for (int pageId = 0; pageId <= maxPageId; pageId++) {
            ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), false);
            page.acquireReadLatch();
            try {
                metaFrame.setPage(page);
                leafFrame.setPage(page);
                interiorFrame.setPage(page);

                if (leafFrame.isLeaf()) {
                    if (pageId == rootPage) {
                        treeIndexStats.addRoot(leafFrame);
                    } else {
                        treeIndexStats.add(leafFrame);
                    }
                } else if (interiorFrame.isInterior()) {
                    if (pageId == rootPage) {
                        treeIndexStats.addRoot(interiorFrame);
                    } else {
                        treeIndexStats.add(interiorFrame);
                    }
                } else {
                    treeIndexStats.add(metaFrame);
                }

            } finally {
                page.releaseReadLatch();
                bufferCache.unpin(page);
            }
        }

        treeIndexStats.end();

        bufferCache.closeFile(fileId);

        return treeIndexStats;
    }
}
