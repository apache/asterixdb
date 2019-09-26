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

import org.apache.hyracks.dataflow.common.data.accessors.PermutingTupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMHarness;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;

public class LSMInvertedIndexRangeSearchCursorInitialState implements ICursorInitialState {

    private final MultiComparator tokensAndKeysCmp;
    private final MultiComparator keyCmp;
    private final ILSMHarness lsmHarness;

    private final ISearchPredicate predicate;
    private final PermutingTupleReference keysOnlyTuple;
    private final ITreeIndexFrameFactory deletedKeysBtreeLeafFrameFactory;

    private final boolean includeMemComponent;
    private final List<ILSMComponent> operationalComponents;

    public LSMInvertedIndexRangeSearchCursorInitialState(MultiComparator tokensAndKeysCmp, MultiComparator keyCmp,
            PermutingTupleReference keysOnlyTuple, ITreeIndexFrameFactory deletedKeysBtreeLeafFrameFactory,
            boolean includeMemComponent, ILSMHarness lsmHarness, ISearchPredicate predicate,
            List<ILSMComponent> operationalComponents) {
        this.tokensAndKeysCmp = tokensAndKeysCmp;
        this.keyCmp = keyCmp;
        this.keysOnlyTuple = keysOnlyTuple;
        this.deletedKeysBtreeLeafFrameFactory = deletedKeysBtreeLeafFrameFactory;
        this.lsmHarness = lsmHarness;
        this.predicate = predicate;
        this.includeMemComponent = includeMemComponent;
        this.operationalComponents = operationalComponents;
    }

    public int getNumComponents() {
        return operationalComponents.size();
    }

    @Override
    public ICachedPage getPage() {
        return null;
    }

    @Override
    public void setPage(ICachedPage page) {
    }

    public List<ILSMComponent> getOperationalComponents() {
        return operationalComponents;
    }

    public ILSMHarness getLSMHarness() {
        return lsmHarness;
    }

    @Override
    public ISearchOperationCallback getSearchOperationCallback() {
        return null;
    }

    @Override
    public void setSearchOperationCallback(ISearchOperationCallback searchCallback) {
        // Do nothing.
    }

    public ISearchPredicate getSearchPredicate() {
        return predicate;
    }

    @Override
    public void setOriginialKeyComparator(MultiComparator originalCmp) {
        // Do nothing.
    }

    @Override
    public MultiComparator getOriginalKeyComparator() {
        return tokensAndKeysCmp;
    }

    public MultiComparator getKeyComparator() {
        return keyCmp;
    }

    public ITreeIndexFrameFactory getgetDeletedKeysBTreeLeafFrameFactory() {
        return deletedKeysBtreeLeafFrameFactory;
    }

    public boolean getIncludeMemComponent() {
        return includeMemComponent;
    }

    public PermutingTupleReference getKeysOnlyTuple() {
        return keysOnlyTuple;
    }
}
