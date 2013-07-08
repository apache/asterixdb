/*
 * Copyright 2009-2013 by The Regents of the University of California
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

import java.util.LinkedList;
import java.util.List;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.IIndex;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;
import edu.uci.ics.hyracks.storage.am.common.tuples.PermutingTupleReference;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexAccessor;

public class LSMInvertedIndexOpContext implements ILSMIndexOperationContext {

    private static final int NUM_DOCUMENT_FIELDS = 1;

    private IndexOperation op;
    private final IInvertedIndex memInvIndex;
    private final IIndex memDeletedKeysBTree;
    private final List<ILSMComponent> componentHolder;

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
        this.componentHolder = new LinkedList<ILSMComponent>();
        this.modificationCallback = modificationCallback;
        this.searchCallback = searchCallback;
    }

    @Override
    public void reset() {
        componentHolder.clear();
    }

    @Override
    // TODO: Ignore opcallback for now.
    public void setOperation(IndexOperation newOp) throws HyracksDataException {
        reset();
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
                    int numKeyFields = memInvIndex.getInvListTypeTraits().length;
                    int[] keyFieldPermutation = new int[numKeyFields];
                    for (int i = 0; i < numKeyFields; i++) {
                        keyFieldPermutation[i] = NUM_DOCUMENT_FIELDS + i;
                    }
                    keysOnlyTuple = new PermutingTupleReference(keyFieldPermutation);
                }
                break;
            }
        }
        op = newOp;
    }

    @Override
    public IndexOperation getOperation() {
        return op;
    }

    @Override
    public List<ILSMComponent> getComponentHolder() {
        return componentHolder;
    }

    @Override
    public ISearchOperationCallback getSearchOperationCallback() {
        return searchCallback;
    }

    @Override
    public IModificationOperationCallback getModificationCallback() {
        return modificationCallback;
    }
}
