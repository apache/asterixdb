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
package org.apache.hyracks.storage.am.lsm.invertedindex.inmemory;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.impls.BTree.BTreeAccessor;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
import org.apache.hyracks.storage.am.common.api.IPageManager;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexSearcher;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IPartitionedInvertedIndex;
import org.apache.hyracks.storage.am.lsm.invertedindex.search.InvertedListPartitions;
import org.apache.hyracks.storage.am.lsm.invertedindex.search.PartitionedTOccurrenceSearcher;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.util.PartitionedInvertedIndexTokenizingTupleIterator;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;

public class PartitionedInMemoryInvertedIndex extends InMemoryInvertedIndex implements IPartitionedInvertedIndex {

    protected final ReentrantReadWriteLock partitionIndexLock = new ReentrantReadWriteLock(true);
    protected short minPartitionIndex = Short.MAX_VALUE;
    protected short maxPartitionIndex = Short.MIN_VALUE;

    public PartitionedInMemoryInvertedIndex(IBufferCache memBufferCache, IPageManager memFreePageManager,
            ITypeTraits[] invListTypeTraits, IBinaryComparatorFactory[] invListCmpFactories,
            ITypeTraits[] tokenTypeTraits, IBinaryComparatorFactory[] tokenCmpFactories,
            IBinaryTokenizerFactory tokenizerFactory, FileReference btreeFileRef) throws HyracksDataException {
        super(memBufferCache, memFreePageManager, invListTypeTraits, invListCmpFactories, tokenTypeTraits,
                tokenCmpFactories, tokenizerFactory, btreeFileRef);
    }

    @Override
    public void insert(ITupleReference tuple, BTreeAccessor btreeAccessor, IIndexOperationContext ictx)
            throws HyracksDataException {
        super.insert(tuple, btreeAccessor, ictx);
        PartitionedInMemoryInvertedIndexOpContext ctx = (PartitionedInMemoryInvertedIndexOpContext) ictx;
        PartitionedInvertedIndexTokenizingTupleIterator tupleIter =
                (PartitionedInvertedIndexTokenizingTupleIterator) ctx.getTupleIter();
        updatePartitionIndexes(tupleIter.getNumTokens());
    }

    @Override
    public void clear() throws HyracksDataException {
        super.clear();
        minPartitionIndex = Short.MAX_VALUE;
        maxPartitionIndex = Short.MIN_VALUE;
    }

    public void updatePartitionIndexes(short numTokens) {
        partitionIndexLock.writeLock().lock();
        try {
            if (numTokens < minPartitionIndex) {
                minPartitionIndex = numTokens;
            }
            if (numTokens > maxPartitionIndex) {
                maxPartitionIndex = numTokens;
            }
        } finally {
            partitionIndexLock.writeLock().unlock();
        }
    }

    @Override
    public PartitionedInMemoryInvertedIndexAccessor createAccessor(IIndexAccessParameters iap)
            throws HyracksDataException {
        return new PartitionedInMemoryInvertedIndexAccessor(this,
                new PartitionedInMemoryInvertedIndexOpContext(btree, tokenCmpFactories, tokenizerFactory), iap);
    }

    @Override
    public boolean openInvertedListPartitionCursors(IInvertedIndexSearcher searcher, IIndexOperationContext ictx,
            short numTokensLowerBound, short numTokensUpperBound, InvertedListPartitions invListPartitions)
            throws HyracksDataException {
        short minPartitionIndex;
        short maxPartitionIndex;
        partitionIndexLock.readLock().lock();
        minPartitionIndex = this.minPartitionIndex;
        maxPartitionIndex = this.maxPartitionIndex;
        partitionIndexLock.readLock().unlock();

        if (minPartitionIndex == Short.MAX_VALUE && maxPartitionIndex == Short.MIN_VALUE) {
            // Index must be empty.
            return false;
        }
        short partitionStartIndex = minPartitionIndex;
        short partitionEndIndex = maxPartitionIndex;
        if (numTokensLowerBound >= 0) {
            partitionStartIndex = (short) Math.max(minPartitionIndex, numTokensLowerBound);
        }
        if (numTokensUpperBound >= 0) {
            partitionEndIndex = (short) Math.min(maxPartitionIndex, numTokensUpperBound);
        }

        PartitionedTOccurrenceSearcher partSearcher = (PartitionedTOccurrenceSearcher) searcher;
        PartitionedInMemoryInvertedIndexOpContext ctx = (PartitionedInMemoryInvertedIndexOpContext) ictx;
        ctx.setOperation(IndexOperation.SEARCH);
        // We can pick either of the full low or high search key, since they should be identical here.
        ITupleReference searchKey = partSearcher.getFullLowSearchKey();
        ctx.getBtreePred().setLowKey(searchKey, true);
        ctx.getBtreePred().setHighKey(searchKey, true);
        // Go through all possibly partitions and see if the token matches.
        // TODO: This procedure could be made more efficient by determining the next partition to search
        // using the last existing partition and re-searching the BTree with an open interval as low key.
        for (short i = partitionStartIndex; i <= partitionEndIndex; i++) {
            partSearcher.setNumTokensBoundsInSearchKeys(i, i);
            InMemoryInvertedListCursor inMemListCursor =
                    (InMemoryInvertedListCursor) partSearcher.getCachedInvertedListCursor();
            inMemListCursor.prepare(ctx.getBtreeAccessor(), ctx.getBtreePred(), ctx.getTokenFieldsCmp(),
                    ctx.getBtreeCmp());
            inMemListCursor.reset(searchKey);
            // Makes the cursor state to OPENED
            inMemListCursor.open(null, null);
            invListPartitions.addInvertedListCursor(inMemListCursor, i);
        }
        return true;
    }

    @Override
    public boolean isEmpty() {
        partitionIndexLock.readLock().lock();
        if (minPartitionIndex == Short.MAX_VALUE && maxPartitionIndex == Short.MIN_VALUE) {
            // Index must be empty.
            partitionIndexLock.readLock().unlock();
            return true;
        }
        partitionIndexLock.readLock().unlock();
        return false;
    }
}
