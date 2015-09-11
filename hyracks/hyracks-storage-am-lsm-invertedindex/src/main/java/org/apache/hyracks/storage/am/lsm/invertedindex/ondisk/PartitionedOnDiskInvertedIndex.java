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

import java.util.ArrayList;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.data.std.primitive.ShortPointable;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.ShortSerializerDeserializer;
import org.apache.hyracks.storage.am.common.api.IIndexAccessor;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallback;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallback;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexSearcher;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedListBuilder;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedListCursor;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IPartitionedInvertedIndex;
import org.apache.hyracks.storage.am.lsm.invertedindex.search.InvertedListPartitions;
import org.apache.hyracks.storage.am.lsm.invertedindex.search.PartitionedTOccurrenceSearcher;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.file.IFileMapProvider;

public class PartitionedOnDiskInvertedIndex extends OnDiskInvertedIndex implements IPartitionedInvertedIndex {

    protected final int PARTITIONING_NUM_TOKENS_FIELD = 1;

    public PartitionedOnDiskInvertedIndex(IBufferCache bufferCache, IFileMapProvider fileMapProvider,
            IInvertedListBuilder invListBuilder, ITypeTraits[] invListTypeTraits,
            IBinaryComparatorFactory[] invListCmpFactories, ITypeTraits[] tokenTypeTraits,
            IBinaryComparatorFactory[] tokenCmpFactories, FileReference btreeFile, FileReference invListsFile)
            throws IndexException {
        super(bufferCache, fileMapProvider, invListBuilder, invListTypeTraits, invListCmpFactories, tokenTypeTraits,
                tokenCmpFactories, btreeFile, invListsFile);
    }

    public class PartitionedOnDiskInvertedIndexAccessor extends OnDiskInvertedIndexAccessor {
        public PartitionedOnDiskInvertedIndexAccessor(OnDiskInvertedIndex index) throws HyracksDataException {
            super(index, new PartitionedTOccurrenceSearcher(ctx, index));
        }
    }

    @Override
    public IIndexAccessor createAccessor(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) throws HyracksDataException {
        return new PartitionedOnDiskInvertedIndexAccessor(this);
    }

    @Override
    public boolean openInvertedListPartitionCursors(IInvertedIndexSearcher searcher, IIndexOperationContext ictx,
            short numTokensLowerBound, short numTokensUpperBound, InvertedListPartitions invListPartitions,
            ArrayList<IInvertedListCursor> cursorsOrderedByTokens) throws HyracksDataException, IndexException {
        PartitionedTOccurrenceSearcher partSearcher = (PartitionedTOccurrenceSearcher) searcher;
        OnDiskInvertedIndexOpContext ctx = (OnDiskInvertedIndexOpContext) ictx;
        ITupleReference lowSearchKey = null;
        ITupleReference highSearchKey = null;
        partSearcher.setNumTokensBoundsInSearchKeys(numTokensLowerBound, numTokensUpperBound);
        if (numTokensLowerBound < 0) {
            ctx.btreePred.setLowKeyComparator(ctx.prefixSearchCmp);
            lowSearchKey = partSearcher.getPrefixSearchKey();
        } else {
            ctx.btreePred.setLowKeyComparator(ctx.searchCmp);
            lowSearchKey = partSearcher.getFullLowSearchKey();
        }
        if (numTokensUpperBound < 0) {
            ctx.btreePred.setHighKeyComparator(ctx.prefixSearchCmp);
            highSearchKey = partSearcher.getPrefixSearchKey();
        } else {
            ctx.btreePred.setHighKeyComparator(ctx.searchCmp);
            highSearchKey = partSearcher.getFullHighSearchKey();
        }
        ctx.btreePred.setLowKey(lowSearchKey, true);
        ctx.btreePred.setHighKey(highSearchKey, true);
        ctx.btreeAccessor.search(ctx.btreeCursor, ctx.btreePred);
        boolean tokenExists = false;
        try {
            while (ctx.btreeCursor.hasNext()) {
                ctx.btreeCursor.next();
                ITupleReference btreeTuple = ctx.btreeCursor.getTuple();
                short numTokens = ShortPointable.getShort(btreeTuple.getFieldData(PARTITIONING_NUM_TOKENS_FIELD), btreeTuple.getFieldStart(PARTITIONING_NUM_TOKENS_FIELD));
                IInvertedListCursor invListCursor = partSearcher.getCachedInvertedListCursor();
                resetInvertedListCursor(btreeTuple, invListCursor);
                cursorsOrderedByTokens.add(invListCursor);
                invListPartitions.addInvertedListCursor(invListCursor, numTokens);
                tokenExists = true;
            }
        } finally {
            ctx.btreeCursor.close();
            ctx.btreeCursor.reset();
        }
        return tokenExists;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }
}
