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
package edu.uci.ics.hyracks.storage.am.lsm.rtree.impls;

import java.util.LinkedList;
import java.util.List;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;

public class ExternalRTreeOpContext implements ILSMIndexOperationContext {
    private IndexOperation op;
    private MultiComparator bTreeCmp;
    private MultiComparator rTreeCmp;
    public final List<ILSMComponent> componentHolder;
    private final List<ILSMComponent> componentsToBeMerged;
    public final ISearchOperationCallback searchCallback;
    private final int targetIndexVersion;
    public ISearchPredicate searchPredicate;

    public ExternalRTreeOpContext(IBinaryComparatorFactory[] rtreeCmpFactories,
            IBinaryComparatorFactory[] btreeCmpFactories, ISearchOperationCallback searchCallback,
            int targetIndexVersion) {

        this.componentHolder = new LinkedList<ILSMComponent>();
        this.componentsToBeMerged = new LinkedList<ILSMComponent>();
        this.searchCallback = searchCallback;
        this.targetIndexVersion = targetIndexVersion;
        this.bTreeCmp = MultiComparator.create(btreeCmpFactories);
        this.rTreeCmp = MultiComparator.create(rtreeCmpFactories);
    }

    public void setOperation(IndexOperation newOp) {
        reset();
        this.op = newOp;
    }

    @Override
    public void setCurrentMutableComponentId(int currentMutableComponentId) {
        // Do nothing. this should never be called for disk only indexes
    }

    @Override
    public void reset() {
        componentHolder.clear();
        componentsToBeMerged.clear();
    }

    @Override
    public IndexOperation getOperation() {
        return op;
    }

    public MultiComparator getBTreeMultiComparator() {
        return bTreeCmp;
    }

    public MultiComparator getRTreeMultiComparator() {
        return rTreeCmp;
    }

    @Override
    public List<ILSMComponent> getComponentHolder() {
        return componentHolder;
    }

    @Override
    public ISearchOperationCallback getSearchOperationCallback() {
        return searchCallback;
    }

    // This should never be needed for disk only indexes
    @Override
    public IModificationOperationCallback getModificationCallback() {
        return null;
    }

    @Override
    public List<ILSMComponent> getComponentsToBeMerged() {
        return componentsToBeMerged;
    }

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
}