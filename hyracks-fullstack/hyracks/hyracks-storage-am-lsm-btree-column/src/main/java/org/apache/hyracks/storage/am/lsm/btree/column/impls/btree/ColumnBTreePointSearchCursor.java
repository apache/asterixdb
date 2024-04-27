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
package org.apache.hyracks.storage.am.lsm.btree.column.impls.btree;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.btree.api.IDiskBTreeStatefulPointSearchCursor;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.ophelpers.FindTupleMode;
import org.apache.hyracks.storage.am.common.ophelpers.FindTupleNoExactMatchPolicy;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.IColumnReadContext;
import org.apache.hyracks.storage.common.IIndexCursorStats;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;

public class ColumnBTreePointSearchCursor extends ColumnBTreeRangeSearchCursor
        implements IDiskBTreeStatefulPointSearchCursor {

    public ColumnBTreePointSearchCursor(ColumnBTreeReadLeafFrame frame, IIndexCursorStats stats, int index,
            IColumnReadContext context) {
        super(frame, stats, index, context);
    }

    @Override
    public boolean doHasNext() {
        // If we found the exact key, return true
        return yieldFirstCall;
    }

    @Override
    protected boolean shouldYieldFirstCall() throws HyracksDataException {
        return pred.getLowKeyComparator().compare(lowKey, frameTuple) == 0;
    }

    @Override
    public void doClose() throws HyracksDataException {
        pageId = IBufferCache.INVALID_PAGEID;
        super.doClose();
    }

    @Override
    public int getLastPageId() {
        return pageId;
    }

    @Override
    public void setCursorToNextKey(ISearchPredicate searchPred) throws HyracksDataException {
        int index = getLowKeyIndex();
        if (index == frame.getTupleCount()) {
            frameTuple.consume();
            yieldFirstCall = false;
            return;
        }
        frameTuple.setAt(index);
        yieldFirstCall = true;
    }

    @Override
    protected void setSearchPredicate(ISearchPredicate searchPred) {
        pred = (RangePredicate) searchPred;
        lowKey = pred.getLowKey();
        lowKeyFtm = FindTupleMode.EXACT;
        lowKeyFtp = FindTupleNoExactMatchPolicy.NONE;
        reusablePredicate.setLowKeyComparator(originalKeyCmp);
    }

    @Override
    protected int getLowKeyIndex() throws HyracksDataException {
        int index = frameTuple.findTupleIndex(pred.getLowKey(), pred.getLowKeyComparator(), lowKeyFtm, lowKeyFtp);
        return index < 0 ? frame.getTupleCount() : index;
    }

    @Override
    public ITreeIndexFrame getFrame() {
        return frame;
    }
}
