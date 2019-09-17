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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.PermutingTupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilter;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilterManager;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;

public class FilterBulkLoader implements IChainedComponentBulkLoader {

    private final ILSMComponentFilter filter;
    private final ITreeIndex treeIndex;
    protected final ILSMComponentFilterManager filterManager;
    protected final PermutingTupleReference indexTuple;
    protected final PermutingTupleReference filterTuple;
    protected final MultiComparator filterCmp;

    public FilterBulkLoader(ILSMComponentFilter filter, ITreeIndex treeIndex, ILSMComponentFilterManager filterManager,
            int[] indexFields, int[] filterFields, MultiComparator filterCmp) {
        this.filter = filter;
        this.treeIndex = treeIndex;
        this.filterManager = filterManager;
        this.indexTuple = new PermutingTupleReference(indexFields);
        this.filterTuple = new PermutingTupleReference(filterFields);
        this.filterCmp = filterCmp;
    }

    @Override
    public ITupleReference delete(ITupleReference tuple) throws HyracksDataException {
        indexTuple.reset(tuple);
        updateFilter(tuple);
        return indexTuple;
    }

    @Override
    public void cleanupArtifacts() throws HyracksDataException {
        //Noop
    }

    @Override
    public ITupleReference add(ITupleReference tuple) throws HyracksDataException {
        indexTuple.reset(tuple);
        updateFilter(tuple);
        return indexTuple;
    }

    @Override
    public void end() throws HyracksDataException {
        filterManager.writeFilter(filter, treeIndex);
    }

    @Override
    public void abort() throws HyracksDataException {
        //Noop
    }

    private void updateFilter(ITupleReference tuple) throws HyracksDataException {
        filterTuple.reset(tuple);
        filter.update(filterTuple, filterCmp, NoOpOperationCallback.INSTANCE);
    }

    @Override
    public void writeFailed(ICachedPage page, Throwable failure) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasFailed() {
        return false;
    }

    @Override
    public Throwable getFailure() {
        return null;
    }

    @Override
    public void force() throws HyracksDataException {
        // no op
    }
}
