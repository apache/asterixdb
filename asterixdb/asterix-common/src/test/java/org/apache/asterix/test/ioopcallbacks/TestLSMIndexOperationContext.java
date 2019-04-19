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
package org.apache.asterix.test.ioopcallbacks;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.PermutingTupleReference;
import org.apache.hyracks.storage.am.common.api.IExtendedModificationOperationCallback;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;

public class TestLSMIndexOperationContext implements ILSMIndexOperationContext {

    private final ILSMIndex index;
    private final List<ILSMComponent> componentHolder = new ArrayList<>();
    private final List<ILSMDiskComponent> componentsToBeMerged = new ArrayList<>();
    private final List<ILSMDiskComponent> componentsToBeReplicated = new ArrayList<>();
    private boolean isAccessingComponents;
    private IndexOperation op;
    private ILSMIOOperation ioOperation;
    private Map<String, Object> map;
    private boolean filterSkip = false;
    private boolean isRecovery = false;

    public TestLSMIndexOperationContext(ILSMIndex index) {
        this.index = index;
    }

    @Override
    public void setOperation(IndexOperation newOp) throws HyracksDataException {
        this.op = newOp;
    }

    @Override
    public IndexOperation getOperation() {
        return op;
    }

    @Override
    public void reset() {
        op = null;
        componentHolder.clear();
        componentsToBeMerged.clear();
        componentsToBeReplicated.clear();
        isAccessingComponents = false;
    }

    @Override
    public void destroy() throws HyracksDataException {
    }

    @Override
    public List<ILSMComponent> getComponentHolder() {
        return componentHolder;
    }

    @Override
    public List<ILSMDiskComponent> getComponentsToBeMerged() {
        return componentsToBeMerged;
    }

    @Override
    public ISearchOperationCallback getSearchOperationCallback() {
        return NoOpOperationCallback.INSTANCE;
    }

    @Override
    public IExtendedModificationOperationCallback getModificationCallback() {
        return NoOpOperationCallback.INSTANCE;
    }

    @Override
    public void setCurrentMutableComponentId(int currentMutableComponentId) {
    }

    @Override
    public void setSearchPredicate(ISearchPredicate searchPredicate) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ISearchPredicate getSearchPredicate() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<ILSMDiskComponent> getComponentsToBeReplicated() {
        return componentsToBeReplicated;
    }

    @Override
    public boolean isAccessingComponents() {
        return isAccessingComponents;
    }

    @Override
    public void setAccessingComponents(boolean accessingComponents) {
        this.isAccessingComponents = accessingComponents;
    }

    @Override
    public PermutingTupleReference getIndexTuple() {
        throw new UnsupportedOperationException();
    }

    @Override
    public PermutingTupleReference getFilterTuple() {
        return null;
    }

    @Override
    public MultiComparator getFilterCmp() {
        return null;
    }

    @Override
    public ILSMIndex getIndex() {
        return index;
    }

    @Override
    public void logPerformanceCounters(int tupleCount) {
    }

    @Override
    public void incrementEnterExitTime(long increment) {
    }

    @Override
    public boolean isTracingEnabled() {
        return false;
    }

    @Override
    public boolean isFilterSkipped() {
        return filterSkip;
    }

    @Override
    public void setFilterSkip(boolean skip) {
        this.filterSkip = skip;
    }

    @Override
    public boolean isRecovery() {
        return isRecovery;
    }

    @Override
    public void setRecovery(boolean recovery) {
        this.isRecovery = recovery;

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
        this.map = map;
    }

    @Override
    public Map<String, Object> getParameters() {
        return map;
    }

}
