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

package org.apache.hyracks.storage.am.lsm.invertedindex.impls;

import java.util.List;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.hyracks.dataflow.common.data.accessors.PermutingTupleReference;
import org.apache.hyracks.storage.am.common.api.IExtendedModificationOperationCallback;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexAccessor;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.util.trace.ITracer;

public class LSMInvertedIndexOpContext extends AbstractLSMIndexOperationContext {

    private static final int NUM_DOCUMENT_FIELDS = 1;

    // Tuple that only has the inverted-index elements (aka keys), projecting away the document fields.
    private PermutingTupleReference keysOnlyTuple;
    // Accessor to the in-memory inverted indexes.
    private IInvertedIndexAccessor[] mutableInvIndexAccessors;
    // Accessor to the deleted-keys BTrees.
    private IIndexAccessor[] deletedKeysBTreeAccessors;
    private IInvertedIndexAccessor currentMutableInvIndexAccessors;
    private IIndexAccessor currentDeletedKeysBTreeAccessors;
    // To keep the buffer frame manager in case of a search
    private IIndexAccessParameters iap;
    private boolean destroyed = false;

    public LSMInvertedIndexOpContext(ILSMIndex index, List<ILSMMemoryComponent> mutableComponents,
            IIndexAccessParameters iap, int[] invertedIndexFields, int[] filterFields,
            IBinaryComparatorFactory[] filterComparatorFactories, ITracer tracer) throws HyracksDataException {
        super(index, invertedIndexFields, filterFields, filterComparatorFactories, iap.getSearchOperationCallback(),
                (IExtendedModificationOperationCallback) iap.getModificationCallback(), tracer);
        mutableInvIndexAccessors = new IInvertedIndexAccessor[mutableComponents.size()];
        deletedKeysBTreeAccessors = new IIndexAccessor[mutableComponents.size()];
        for (int i = 0; i < mutableComponents.size(); i++) {
            LSMInvertedIndexMemoryComponent mutableComponent =
                    (LSMInvertedIndexMemoryComponent) mutableComponents.get(i);
            mutableInvIndexAccessors[i] =
                    mutableComponent.getIndex().createAccessor(NoOpIndexAccessParameters.INSTANCE);
            deletedKeysBTreeAccessors[i] =
                    mutableComponent.getBuddyIndex().createAccessor(NoOpIndexAccessParameters.INSTANCE);
        }
        // Project away the document fields, leaving only the key fields.
        LSMInvertedIndexMemoryComponent c = (LSMInvertedIndexMemoryComponent) mutableComponents.get(0);
        int numKeyFields = c.getIndex().getInvListTypeTraits().length;
        int[] keyFieldPermutation = new int[numKeyFields];
        for (int i = 0; i < numKeyFields; i++) {
            keyFieldPermutation[i] = NUM_DOCUMENT_FIELDS + i;
        }
        keysOnlyTuple = new PermutingTupleReference(keyFieldPermutation);
        this.iap = iap;
    }

    @Override
    public void setCurrentMutableComponentId(int currentMutableComponentId) {
        currentMutableInvIndexAccessors = mutableInvIndexAccessors[currentMutableComponentId];
        currentDeletedKeysBTreeAccessors = deletedKeysBTreeAccessors[currentMutableComponentId];
    }

    public IInvertedIndexAccessor getCurrentMutableInvIndexAccessors() {
        return currentMutableInvIndexAccessors;
    }

    public PermutingTupleReference getKeysOnlyTuple() {
        return keysOnlyTuple;
    }

    public IIndexAccessor getCurrentDeletedKeysBTreeAccessors() {
        return currentDeletedKeysBTreeAccessors;
    }

    public IIndexAccessParameters getIndexAccessParameters() {
        return iap;
    }

    @Override
    public void destroy() throws HyracksDataException {
        if (destroyed) {
            return;
        }
        destroyed = true;
        Throwable failure = CleanupUtils.destroy(null, mutableInvIndexAccessors);
        failure = CleanupUtils.destroy(failure, deletedKeysBTreeAccessors);
        if (failure != null) {
            throw HyracksDataException.create(failure);
        }
    }
}
