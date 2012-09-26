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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls;

import edu.uci.ics.hyracks.storage.am.common.api.IIndex;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;
import edu.uci.ics.hyracks.storage.am.common.tuples.PermutingTupleReference;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexAccessor;

public class LSMInvertedIndexOpContext implements IIndexOperationContext {

    private IndexOperation op;
    private final IInvertedIndex memInvIndex;
    private final IIndex memDeletedKeysBTree;

    public final IModificationOperationCallback modificationCallback;
    public final ISearchOperationCallback searchCallback;
    
    // Tuple that only has the inverted-index elements (aka keys), projecting away the document fields.
    public PermutingTupleReference keysOnlyTuple;
    
    // Accessor to the in-memory inverted index.
    public IInvertedIndexAccessor memInvIndexAccessor;
    // Accessor to the deleted-keys BTree.
    public IIndexAccessor deletedKeysBTreeAccessor;

    public LSMInvertedIndexOpContext(IInvertedIndex memInvIndex, IIndex memDeletedKeysBTree,
            IModificationOperationCallback modificationCallback, ISearchOperationCallback searchCallback) {
        this.memInvIndex = memInvIndex;
        this.memDeletedKeysBTree = memDeletedKeysBTree;
        this.modificationCallback = modificationCallback;
        this.searchCallback = searchCallback;
    }

    @Override
    public void reset() {
    }

    @Override
    // TODO: Ignore opcallback for now.
    public void startOperation(IndexOperation newOp) {
        switch (newOp) {
            case INSERT:
            case DELETE:
            case PHYSICALDELETE: {
                if (deletedKeysBTreeAccessor == null) {
                    memInvIndexAccessor = (IInvertedIndexAccessor) memInvIndex.createAccessor(
                            NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
                    deletedKeysBTreeAccessor = memDeletedKeysBTree.createAccessor(NoOpOperationCallback.INSTANCE,
                            NoOpOperationCallback.INSTANCE);
                    // Project away the document fields, leaving only the key fields.
                    int numTokenFields = memInvIndex.getTokenTypeTraits().length;
                    int numKeyFields = memInvIndex.getInvListTypeTraits().length;
                    int[] keyFieldPermutation = new int[memInvIndex.getInvListTypeTraits().length];
                    for (int i = 0; i < numKeyFields; i++) {
                        keyFieldPermutation[i] = numTokenFields + i;
                    }
                    keysOnlyTuple = new PermutingTupleReference(keyFieldPermutation);
                }
                break;
            }
        }
        op = newOp;
    }
    
    public IndexOperation getIndexOp() {
        return op;
    }
}
