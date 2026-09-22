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
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.IColumnReadContext;
import org.apache.hyracks.storage.common.IIndexCursorStats;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;

/**
 * Point-search cursor answering <b>existence only</b>: {@code hasNext()} says whether the key is physically on
 * the leaf page, antimatter included, and nothing is positioned for reading. Its one consumer is the sample
 * cursor's liveness probe ({@code LSMBTreePointSearchCursor#keyExistsIncludingAntimatter}); wired only from
 * {@code LSMColumnBTree#createSampleLivenessSearchCursor}. Never hand it to a query path.
 * <p>
 * <b>Same verdict as {@link ColumnBTreePointSearchCursor}, key for key.</b> The base cursor's verdict is a pure
 * function of {@code getLowKeyIndex()}: with no high key, {@code end} is always the last slot, so a match yields
 * {@code start <= end} and {@code shouldYieldFirstCall()} then re-compares the key against the very index the
 * exact match was found at — {@code 0} by construction. The {@code frameTuple.reset(...)} in between pins and
 * resets every projected column and cannot change the answer, only its cost. So this cursor computes the index
 * and stops. {@code findTupleIndex} random-accesses PK values ({@code setKeyAt} → {@code reader.getValue}),
 * touching no definition levels and pinning no column page, so with a key-only projection the probe reads page
 * zero and nothing else.
 * <p>
 * <b>Antimatter counts as present</b>, and must: a newer delete shadows the older live tuple, so reporting it
 * absent would leak logically-deleted rows into the sample. PK values are written for antimatter too, so
 * {@code findTupleIndex} matches it like any insert.
 * <p>
 * <b>Same-page search reuse is disabled</b> ({@link #getLastPageId()} returns
 * {@link IBufferCache#INVALID_PAGEID}, so {@code DiskBTree#search} always descends from the root): without a
 * positioned {@code frameTuple} there is no monotonic {@code tupleIndex} for it to build on, and the sampler's
 * probe keys are not ascending anyway.
 */
public final class ColumnBTreeExistencePointSearchCursor extends ColumnBTreePointSearchCursor {

    public ColumnBTreeExistencePointSearchCursor(ColumnBTreeReadLeafFrame frame, IIndexCursorStats stats, int index,
            IColumnReadContext context) {
        super(frame, stats, index, context);
    }

    @Override
    protected void initCursorPosition(ISearchPredicate searchPred) throws HyracksDataException {
        setSearchPredicate(searchPred);
        yieldFirstCall = false;
        // Settled here and nowhere else: no setCursorPosition(), so no frameTuple.reset() and no column
        // materialization.
        if (getExactTupleIndex() < 0) {
            // Leaves the frame tuple consumed, as the base class's "start > end" branch does.
            frameTuple.consume();
        } else {
            yieldFirstCall = true;
        }
    }

    /**
     * @return the exactly-matched index, or a negative indicator. Unlike
     *         {@link ColumnBTreePointSearchCursor#getLowKeyIndex()} the negative is returned as-is rather than
     *         folded into {@code getTupleCount()}: here "absent" is the answer.
     */
    private int getExactTupleIndex() throws HyracksDataException {
        return frameTuple.findTupleIndex(pred.getLowKey(), pred.getLowKeyComparator(), lowKeyFtm, lowKeyFtp);
    }

    @Override
    public int getLastPageId() {
        return IBufferCache.INVALID_PAGEID;
    }

    @Override
    public void setCursorToNextKey(ISearchPredicate searchPred) throws HyracksDataException {
        throw new UnsupportedOperationException(
                "same-page search reuse is disabled for " + getClass().getSimpleName() + "; getLastPageId() is "
                        + "always INVALID_PAGEID so DiskBTree#search must always descend from the root");
    }

    /**
     * @throws UnsupportedOperationException always: the frame tuple is never positioned, so returning it would
     *         hand out unpositioned column state. Better a crash than a silently wrong tuple.
     */
    @Override
    public ITupleReference doGetTuple() {
        throw new UnsupportedOperationException(
                getClass().getSimpleName() + " answers existence only; its tuple is never positioned");
    }
}
