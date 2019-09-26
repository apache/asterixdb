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

package org.apache.hyracks.storage.am.lsm.invertedindex.ondisk;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.data.std.primitive.ShortPointable;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
import org.apache.hyracks.storage.am.common.api.IPageManagerFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexSearcher;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedListBuilder;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IPartitionedInvertedIndex;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.InvertedListCursor;
import org.apache.hyracks.storage.am.lsm.invertedindex.search.InvertedIndexSearchPredicate;
import org.apache.hyracks.storage.am.lsm.invertedindex.search.InvertedListPartitions;
import org.apache.hyracks.storage.am.lsm.invertedindex.search.PartitionedTOccurrenceSearcher;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;

public class PartitionedOnDiskInvertedIndex extends OnDiskInvertedIndex implements IPartitionedInvertedIndex {

    protected final int PARTITIONING_NUM_TOKENS_FIELD = 1;

    public PartitionedOnDiskInvertedIndex(IBufferCache bufferCache, IInvertedListBuilder invListBuilder,
            ITypeTraits[] invListTypeTraits, IBinaryComparatorFactory[] invListCmpFactories,
            ITypeTraits[] tokenTypeTraits, IBinaryComparatorFactory[] tokenCmpFactories, FileReference btreeFile,
            FileReference invListsFile, IPageManagerFactory pageManagerFactory) throws HyracksDataException {
        super(bufferCache, invListBuilder, invListTypeTraits, invListCmpFactories, tokenTypeTraits, tokenCmpFactories,
                btreeFile, invListsFile, pageManagerFactory);
    }

    public class PartitionedOnDiskInvertedIndexAccessor extends OnDiskInvertedIndexAccessor {
        public PartitionedOnDiskInvertedIndexAccessor(OnDiskInvertedIndex index, IIndexAccessParameters iap)
                throws HyracksDataException {
            super(index, iap);
        }

        @Override
        public IIndexCursor createSearchCursor(boolean exclusive) throws HyracksDataException {
            if (searcher == null) {
                searcher = new PartitionedTOccurrenceSearcher(index, ctx);
            }
            return new OnDiskInvertedIndexSearchCursor(searcher);
        }

        @Override
        public void search(IIndexCursor cursor, ISearchPredicate searchPred) throws HyracksDataException {
            if (searcher == null) {
                searcher = new PartitionedTOccurrenceSearcher(index, ctx);
            }
            searcher.search(cursor, (InvertedIndexSearchPredicate) searchPred, opCtx);
        }
    }

    @Override
    public PartitionedOnDiskInvertedIndexAccessor createAccessor(IIndexAccessParameters iap)
            throws HyracksDataException {
        return new PartitionedOnDiskInvertedIndexAccessor(this, iap);
    }

    @Override
    public boolean openInvertedListPartitionCursors(IInvertedIndexSearcher searcher, IIndexOperationContext ictx,
            short numTokensLowerBound, short numTokensUpperBound, InvertedListPartitions invListPartitions)
            throws HyracksDataException {
        PartitionedTOccurrenceSearcher partSearcher = (PartitionedTOccurrenceSearcher) searcher;
        OnDiskInvertedIndexOpContext ctx = (OnDiskInvertedIndexOpContext) ictx;
        ITupleReference lowSearchKey = null;
        ITupleReference highSearchKey = null;
        partSearcher.setNumTokensBoundsInSearchKeys(numTokensLowerBound, numTokensUpperBound);
        if (numTokensLowerBound < 0) {
            ctx.getBtreePred().setLowKeyComparator(ctx.getPrefixSearchCmp());
            lowSearchKey = partSearcher.getPrefixSearchKey();
        } else {
            ctx.getBtreePred().setLowKeyComparator(ctx.getSearchCmp());
            lowSearchKey = partSearcher.getFullLowSearchKey();
        }
        if (numTokensUpperBound < 0) {
            ctx.getBtreePred().setHighKeyComparator(ctx.getPrefixSearchCmp());
            highSearchKey = partSearcher.getPrefixSearchKey();
        } else {
            ctx.getBtreePred().setHighKeyComparator(ctx.getSearchCmp());
            highSearchKey = partSearcher.getFullHighSearchKey();
        }
        ctx.getBtreePred().setLowKey(lowSearchKey, true);
        ctx.getBtreePred().setHighKey(highSearchKey, true);
        boolean tokenExists = false;
        ctx.getBtreeAccessor().search(ctx.getBtreeCursor(), ctx.getBtreePred());
        try {
            while (ctx.getBtreeCursor().hasNext()) {
                ctx.getBtreeCursor().next();
                ITupleReference btreeTuple = ctx.getBtreeCursor().getTuple();
                short numTokens = ShortPointable.getShort(btreeTuple.getFieldData(PARTITIONING_NUM_TOKENS_FIELD),
                        btreeTuple.getFieldStart(PARTITIONING_NUM_TOKENS_FIELD));
                InvertedListCursor invListCursor = partSearcher.getCachedInvertedListCursor();
                openInvertedListCursor(btreeTuple, invListCursor, ctx);
                invListPartitions.addInvertedListCursor(invListCursor, numTokens);
                tokenExists = true;
            }
        } finally {
            ctx.getBtreeCursor().close();
        }
        return tokenExists;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }
}
