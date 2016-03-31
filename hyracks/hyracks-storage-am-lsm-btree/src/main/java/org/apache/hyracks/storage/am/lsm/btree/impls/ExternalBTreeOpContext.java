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
package org.apache.hyracks.storage.am.lsm.btree.impls;

import java.util.LinkedList;
import java.util.List;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallback;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallback;
import org.apache.hyracks.storage.am.common.api.ISearchPredicate;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.common.ophelpers.MultiComparator;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;

public class ExternalBTreeOpContext implements ILSMIndexOperationContext {
    public ITreeIndexFrameFactory insertLeafFrameFactory;
    public ITreeIndexFrameFactory deleteLeafFrameFactory;
    public IBTreeLeafFrame insertLeafFrame;
    public IBTreeLeafFrame deleteLeafFrame;
    public IndexOperation op;
    public final MultiComparator cmp;
    public final MultiComparator bloomFilterCmp;
    public final ISearchOperationCallback searchCallback;
    private final List<ILSMComponent> componentHolder;
    private final List<ILSMComponent> componentsToBeMerged;
    private final List<ILSMComponent> componentsToBeReplicated;
    private final int targetIndexVersion;
    public ISearchPredicate searchPredicate;
    public LSMBTreeCursorInitialState searchInitialState;

    public ExternalBTreeOpContext(ITreeIndexFrameFactory insertLeafFrameFactory,
            ITreeIndexFrameFactory deleteLeafFrameFactory, ISearchOperationCallback searchCallback,
            int numBloomFilterKeyFields, IBinaryComparatorFactory[] cmpFactories, int targetIndexVersion,
            ILSMHarness lsmHarness) {
        if (cmpFactories != null) {
            this.cmp = MultiComparator.create(cmpFactories);
        } else {
            this.cmp = null;
        }
        bloomFilterCmp = MultiComparator.create(cmpFactories, 0, numBloomFilterKeyFields);
        this.insertLeafFrameFactory = insertLeafFrameFactory;
        this.deleteLeafFrameFactory = deleteLeafFrameFactory;
        this.insertLeafFrame = (IBTreeLeafFrame) insertLeafFrameFactory.createFrame();
        this.deleteLeafFrame = (IBTreeLeafFrame) deleteLeafFrameFactory.createFrame();
        if (insertLeafFrame != null && this.cmp != null) {
            insertLeafFrame.setMultiComparator(cmp);
        }
        if (deleteLeafFrame != null && this.cmp != null) {
            deleteLeafFrame.setMultiComparator(cmp);
        }
        this.componentHolder = new LinkedList<ILSMComponent>();
        this.componentsToBeMerged = new LinkedList<ILSMComponent>();
        this.componentsToBeReplicated = new LinkedList<ILSMComponent>();
        this.searchCallback = searchCallback;
        this.targetIndexVersion = targetIndexVersion;
        searchInitialState = new LSMBTreeCursorInitialState(insertLeafFrameFactory, cmp, bloomFilterCmp, lsmHarness,
                null, searchCallback, null);
    }

    @Override
    public void setOperation(IndexOperation newOp) {
        reset();
        this.op = newOp;
    }

    @Override
    public void reset() {
        componentHolder.clear();
        componentsToBeMerged.clear();
        componentsToBeReplicated.clear();
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

    // Disk only index should never needs a modification callback
    @Override
    public IModificationOperationCallback getModificationCallback() {
        return null;
    }

    @Override
    public void setCurrentMutableComponentId(int currentMutableComponentId) {
        // Do nothing: this method should never be called for this class
    }

    @Override
    public List<ILSMComponent> getComponentsToBeMerged() {
        return componentsToBeMerged;
    }

    // Used by indexes with global transaction
    public int getTargetIndexVersion() {
        return targetIndexVersion;
    }

    @Override
    public void setSearchPredicate(ISearchPredicate searchPredicate) {
        this.searchPredicate = searchPredicate;
    }

    @Override
    public ISearchPredicate getSearchPredicate() {
        return searchPredicate;
    }

    @Override
    public List<ILSMComponent> getComponentsToBeReplicated() {
        return componentsToBeReplicated;
    }

}
