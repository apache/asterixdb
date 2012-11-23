/*
 * Copyright 2009-2012 by The Regents of the University of California
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
package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.inmemory;

import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeException;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree.BTreeAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexSearcher;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedListCursor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IPartitionedInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.search.InvertedListPartitions;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.search.PartitionedTOccurrenceSearcher;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util.PartitionedInvertedIndexTokenizingTupleIterator;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;

public class PartitionedInMemoryInvertedIndex extends InMemoryInvertedIndex implements IPartitionedInvertedIndex {

    protected final ReentrantReadWriteLock partitionIndexLock = new ReentrantReadWriteLock(true);
    protected int minPartitionIndex = Integer.MAX_VALUE;
    protected int maxPartitionIndex = Integer.MIN_VALUE;

    public PartitionedInMemoryInvertedIndex(IBufferCache memBufferCache, IFreePageManager memFreePageManager,
            ITypeTraits[] invListTypeTraits, IBinaryComparatorFactory[] invListCmpFactories,
            ITypeTraits[] tokenTypeTraits, IBinaryComparatorFactory[] tokenCmpFactories,
            IBinaryTokenizerFactory tokenizerFactory) throws BTreeException {
        super(memBufferCache, memFreePageManager, invListTypeTraits, invListCmpFactories, tokenTypeTraits,
                tokenCmpFactories, tokenizerFactory);
    }

    @Override
    public void insert(ITupleReference tuple, BTreeAccessor btreeAccessor, IIndexOperationContext ictx)
            throws HyracksDataException, IndexException {
        super.insert(tuple, btreeAccessor, ictx);
        PartitionedInMemoryInvertedIndexOpContext ctx = (PartitionedInMemoryInvertedIndexOpContext) ictx;
        PartitionedInvertedIndexTokenizingTupleIterator tupleIter = (PartitionedInvertedIndexTokenizingTupleIterator) ctx.tupleIter;
        updatePartitionIndexes(tupleIter.getNumTokens());
    }

    @Override
    public void clear() throws HyracksDataException {
        super.clear();
        minPartitionIndex = Integer.MAX_VALUE;
        maxPartitionIndex = Integer.MIN_VALUE;
    }

    public void updatePartitionIndexes(int numTokens) {
        partitionIndexLock.writeLock().lock();
        if (numTokens < minPartitionIndex) {
            minPartitionIndex = numTokens;
        }
        if (numTokens > maxPartitionIndex) {
            maxPartitionIndex = numTokens;
        }
        partitionIndexLock.writeLock().unlock();
    }

    @Override
    public IIndexAccessor createAccessor(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) {
        return new PartitionedInMemoryInvertedIndexAccessor(this, new PartitionedInMemoryInvertedIndexOpContext(btree,
                tokenCmpFactories, tokenizerFactory));
    }

    @Override
    public void openInvertedListPartitionCursors(IInvertedIndexSearcher searcher, IIndexOperationContext ictx,
            int numTokensLowerBound, int numTokensUpperBound, InvertedListPartitions invListPartitions)
            throws HyracksDataException, IndexException {
        int minPartitionIndex;
        int maxPartitionIndex;
        partitionIndexLock.readLock().lock();
        minPartitionIndex = this.minPartitionIndex;
        maxPartitionIndex = this.maxPartitionIndex;
        partitionIndexLock.readLock().unlock();

        if (minPartitionIndex == Integer.MAX_VALUE || maxPartitionIndex == Integer.MIN_VALUE) {
            // Index must be empty.
            return;
        }
        int partitionStartIndex = minPartitionIndex;
        int partitionEndIndex = maxPartitionIndex;
        if (numTokensLowerBound >= 0) {
            partitionStartIndex = Math.max(minPartitionIndex, numTokensLowerBound);
        }
        if (numTokensUpperBound >= 0) {
            partitionEndIndex = Math.min(maxPartitionIndex, numTokensUpperBound);
        }

        PartitionedTOccurrenceSearcher partSearcher = (PartitionedTOccurrenceSearcher) searcher;
        PartitionedInMemoryInvertedIndexOpContext ctx = (PartitionedInMemoryInvertedIndexOpContext) ictx;
        ctx.setOperation(IndexOperation.SEARCH);
        // We can pick either of the full low or high search key, since they should be identical here.
        ITupleReference searchKey = partSearcher.getFullLowSearchKey();
        ctx.btreePred.setLowKey(searchKey, true);
        ctx.btreePred.setHighKey(searchKey, true);
        // Go through all possibly partitions and see if the token matches.
        // TODO: This procedure could be made more efficient by determining the next partition to search
        // using the last existing partition and re-searching the BTree with an open interval as low key.
        for (int i = partitionStartIndex; i <= partitionEndIndex; i++) {
            partSearcher.setNumTokensBoundsInSearchKeys(i, i);
            InMemoryInvertedListCursor inMemListCursor = (InMemoryInvertedListCursor) partSearcher
                    .getCachedInvertedListCursor();
            inMemListCursor.prepare(ctx.btreeAccessor, ctx.btreePred, ctx.tokenFieldsCmp, ctx.btreeCmp);
            inMemListCursor.reset(searchKey);
            invListPartitions.addInvertedListCursor(inMemListCursor, i);
        }
    }

    @Override
    public void cleanupPartitionState(ArrayList<IInvertedListCursor> invListCursors) throws HyracksDataException {
        // Make sure to unpin/close cursors if the entire partition is pruned.
        //for (IInvertedListCursor cursor : invListCursors) {
        //    cursor.unpinPages();
        //}
    }
}
