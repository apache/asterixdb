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

package org.apache.hyracks.storage.am.btree.impls;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import org.apache.hyracks.storage.common.IIndexCursorStats;
import org.apache.hyracks.storage.common.NoOpIndexCursorStats;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

public class DiskBTreeRangeSearchCursor extends BTreeRangeSearchCursor {

    public DiskBTreeRangeSearchCursor(IBTreeLeafFrame frame, boolean exclusiveLatchNodes) {
        this(frame, exclusiveLatchNodes, NoOpIndexCursorStats.INSTANCE);
    }

    protected DiskBTreeRangeSearchCursor(IBTreeLeafFrame frame, boolean exclusiveLatchNodes, IIndexCursorStats stats) {
        super(frame, exclusiveLatchNodes, stats);
    }

    @Override
    public boolean doHasNext() throws HyracksDataException {
        int nextLeafPage;
        if (tupleIndex >= frame.getTupleCount()) {
            nextLeafPage = frame.getNextLeaf();
            if (nextLeafPage >= 0) {
                fetchNextLeafPage(nextLeafPage);
                tupleIndex = 0;
                // update page ids and positions
                stopTupleIndex = getHighKeyIndex();
                if (stopTupleIndex < 0) {
                    return false;
                }
            } else {
                return false;
            }
        }
        if (tupleIndex > stopTupleIndex) {
            return false;
        }

        frameTuple.resetByTupleIndex(frame, tupleIndex);
        return true;
    }

    public ICachedPage getPage() {
        return page;
    }

    @Override
    protected void releasePage() throws HyracksDataException {
        bufferCache.unpin(page);
    }

    @Override
    protected ICachedPage acquirePage(int pageId) throws HyracksDataException {
        stats.getPageCounter().update(1);
        return bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), false);
    }

}
