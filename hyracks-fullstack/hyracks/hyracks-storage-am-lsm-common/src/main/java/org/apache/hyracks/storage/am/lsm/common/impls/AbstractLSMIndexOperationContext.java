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
package org.apache.hyracks.storage.am.lsm.common.impls;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.PermutingTupleReference;
import org.apache.hyracks.storage.am.common.api.IExtendedModificationOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.util.trace.ITracer;
import org.apache.hyracks.util.trace.ITracer.Scope;
import org.apache.hyracks.util.trace.TraceUtils;

public abstract class AbstractLSMIndexOperationContext implements ILSMIndexOperationContext {

    protected final ILSMIndex index;
    protected final PermutingTupleReference indexTuple;
    protected final MultiComparator filterCmp;
    protected final PermutingTupleReference filterTuple;
    protected final List<ILSMComponent> componentHolder;
    protected final List<ILSMDiskComponent> componentsToBeMerged;
    protected final List<ILSMDiskComponent> componentsToBeReplicated;
    protected final ISearchOperationCallback searchCallback;
    protected final IExtendedModificationOperationCallback modificationCallback;
    protected IndexOperation op;
    protected boolean accessingComponents = false;
    protected ISearchPredicate searchPredicate;
    protected final ITracer tracer;
    protected final long traceCategory;
    private long enterExitTime = 0L;
    protected boolean skipFilter = false;
    protected boolean recovery = false;
    private ILSMIOOperation ioOperation;
    private Map<String, Object> parametersMap;

    public AbstractLSMIndexOperationContext(ILSMIndex index, int[] treeFields, int[] filterFields,
            IBinaryComparatorFactory[] filterCmpFactories, ISearchOperationCallback searchCallback,
            IExtendedModificationOperationCallback modificationCallback, ITracer tracer) {
        this.index = index;
        this.searchCallback = searchCallback;
        this.modificationCallback = modificationCallback;
        this.componentHolder = new ArrayList<>();
        this.componentsToBeMerged = new ArrayList<>();
        this.componentsToBeReplicated = new ArrayList<>();
        if (filterFields != null) {
            indexTuple = new PermutingTupleReference(treeFields);
            filterCmp = MultiComparator.create(filterCmpFactories);
            filterTuple = new PermutingTupleReference(filterFields);
        } else {
            indexTuple = null;
            filterCmp = null;
            filterTuple = null;
        }
        this.tracer = tracer;
        this.traceCategory = tracer.getRegistry().get(TraceUtils.INDEX_OPERATIONS);
    }

    @Override
    public boolean isAccessingComponents() {
        return accessingComponents;
    }

    @Override
    public void setAccessingComponents(boolean accessingComponents) {
        this.accessingComponents = accessingComponents;
    }

    @Override
    public final PermutingTupleReference getIndexTuple() {
        return indexTuple;
    }

    @Override
    public final PermutingTupleReference getFilterTuple() {
        return filterTuple;
    }

    @Override
    public final MultiComparator getFilterCmp() {
        return filterCmp;
    }

    @Override
    public void reset() {
        accessingComponents = false;
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

    @Override
    public IExtendedModificationOperationCallback getModificationCallback() {
        return modificationCallback;
    }

    @Override
    public List<ILSMDiskComponent> getComponentsToBeMerged() {
        return componentsToBeMerged;
    }

    @Override
    public List<ILSMDiskComponent> getComponentsToBeReplicated() {
        return componentsToBeReplicated;
    }

    @Override
    public void setOperation(IndexOperation newOp) throws HyracksDataException {
        reset();
        op = newOp;
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
    public final boolean isTracingEnabled() {
        return tracer.isEnabled(traceCategory);
    }

    @Override
    public void logPerformanceCounters(int tupleCount) {
        if (isTracingEnabled()) {
            tracer.instant("store-counters", traceCategory, Scope.t,
                    "{\"count\":" + tupleCount + ",\"enter-exit-duration-ns\":" + enterExitTime + "}");
            resetCounters();
        }
    }

    public void resetCounters() {
        enterExitTime = 0L;
    }

    @Override
    public void incrementEnterExitTime(long increment) {
        enterExitTime += increment;
    }

    @Override
    public ILSMIndex getIndex() {
        return index;
    }

    @Override
    public boolean isFilterSkipped() {
        return skipFilter;
    }

    @Override
    public void setFilterSkip(boolean skip) {
        this.skipFilter = skip;
    }

    @Override
    public boolean isRecovery() {
        return recovery;
    }

    @Override
    public void setRecovery(boolean recovery) {
        this.recovery = recovery;
    }

    @Override
    public ILSMIOOperation getIoOperation() {
        return ioOperation;
    }

    @Override
    public void setIoOperation(ILSMIOOperation ioOperation) {
        this.ioOperation = ioOperation;
    }

    @Override
    public void setParameters(Map<String, Object> map) {
        this.parametersMap = map;
    }

    @Override
    public Map<String, Object> getParameters() {
        return parametersMap;
    }

}
