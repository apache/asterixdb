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
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;
import edu.uci.ics.hyracks.storage.am.common.tuples.PermutingTupleReference;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexAccessor;

public class LSMInvertedIndexOpContext implements ILSMIndexOperationContext {

    private static final int NUM_DOCUMENT_FIELDS = 1;

    private IndexOperation op;
    private final List<ILSMComponent> componentHolder;
    private final List<ILSMComponent> componentsToBeMerged;
    
    public final IModificationOperationCallback modificationCallback;
    public final ISearchOperationCallback searchCallback;

    // Tuple that only has the inverted-index elements (aka keys), projecting away the document fields.
    public PermutingTupleReference keysOnlyTuple;

    // Accessor to the in-memory inverted indexes.
    public IInvertedIndexAccessor[] mutableInvIndexAccessors;
    // Accessor to the deleted-keys BTrees.
    public IIndexAccessor[] deletedKeysBTreeAccessors;

    public IInvertedIndexAccessor currentMutableInvIndexAccessors;
    public IIndexAccessor currentDeletedKeysBTreeAccessors;

    public LSMInvertedIndexOpContext(List<ILSMComponent> mutableComponents,
            IModificationOperationCallback modificationCallback, ISearchOperationCallback searchCallback)
            throws HyracksDataException {
        this.componentHolder = new LinkedList<ILSMComponent>();
        this.componentsToBeMerged = new LinkedList<ILSMComponent>();
        this.modificationCallback = modificationCallback;
        this.searchCallback = searchCallback;

        mutableInvIndexAccessors = new IInvertedIndexAccessor[mutableComponents.size()];
        deletedKeysBTreeAccessors = new IIndexAccessor[mutableComponents.size()];

        for (int i = 0; i < mutableComponents.size(); i++) {
            LSMInvertedIndexMemoryComponent mutableComponent = (LSMInvertedIndexMemoryComponent) mutableComponents
                    .get(i);
            mutableInvIndexAccessors[i] = (IInvertedIndexAccessor) mutableComponent.getInvIndex().createAccessor(
                    NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
            deletedKeysBTreeAccessors[i] = mutableComponent.getDeletedKeysBTree().createAccessor(
                    NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        }

        assert mutableComponents.size() > 0;

        // Project away the document fields, leaving only the key fields.
        LSMInvertedIndexMemoryComponent c = (LSMInvertedIndexMemoryComponent) mutableComponents.get(0);
        int numKeyFields = c.getInvIndex().getInvListTypeTraits().length;
        int[] keyFieldPermutation = new int[numKeyFields];
        for (int i = 0; i < numKeyFields; i++) {
            keyFieldPermutation[i] = NUM_DOCUMENT_FIELDS + i;
        }
        keysOnlyTuple = new PermutingTupleReference(keyFieldPermutation);
    }

    @Override
    public void reset() {
        componentHolder.clear();
        componentsToBeMerged.clear();
    }

    @Override
    // TODO: Ignore opcallback for now.
    public void setOperation(IndexOperation newOp) throws HyracksDataException {
        reset();
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

    @Override
    public void setCurrentMutableComponentId(int currentMutableComponentId) {
        currentMutableInvIndexAccessors = mutableInvIndexAccessors[currentMutableComponentId];
        currentDeletedKeysBTreeAccessors = deletedKeysBTreeAccessors[currentMutableComponentId];
    }
    
    @Override
    public List<ILSMComponent> getComponentsToBeMerged() {
        return componentsToBeMerged;
    }
}
