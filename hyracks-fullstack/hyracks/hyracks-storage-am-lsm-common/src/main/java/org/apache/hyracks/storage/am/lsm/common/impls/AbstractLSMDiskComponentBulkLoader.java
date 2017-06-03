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
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilter;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilterSpecification;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.impls.AbstractTreeIndex.AbstractTreeIndexBulkLoader;
import org.apache.hyracks.storage.am.common.tuples.PermutingTupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilterManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponentBulkLoader;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMTreeTupleWriter;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.MultiComparator;

public abstract class AbstractLSMDiskComponentBulkLoader implements ILSMDiskComponentBulkLoader {
    protected final ILSMDiskComponent component;

    protected final IIndexBulkLoader indexBulkLoader;
    protected final IIndexBulkLoader bloomFilterBuilder;

    protected final ILSMComponentFilterManager filterManager;
    protected final PermutingTupleReference indexTuple;
    protected final PermutingTupleReference filterTuple;
    protected final MultiComparator filterCmp;

    protected boolean cleanedUpArtifacts = false;
    protected boolean isEmptyComponent = true;
    protected boolean endedBloomFilterLoad = false;

    //with filter
    public AbstractLSMDiskComponentBulkLoader(ILSMDiskComponent component, BloomFilterSpecification bloomFilterSpec,
            float fillFactor, boolean verifyInput, long numElementsHint, boolean checkIfEmptyIndex,
            ILSMComponentFilterManager filterManager, int[] indexFields, int[] filterFields, MultiComparator filterCmp)
            throws HyracksDataException {
        this.component = component;
        this.indexBulkLoader =
                getIndex(component).createBulkLoader(fillFactor, verifyInput, numElementsHint, checkIfEmptyIndex);
        if (bloomFilterSpec != null) {
            this.bloomFilterBuilder = getBloomFilter(component).createBuilder(numElementsHint,
                    bloomFilterSpec.getNumHashes(), bloomFilterSpec.getNumBucketsPerElements());
        } else {
            this.bloomFilterBuilder = null;
        }
        if (filterManager != null) {
            this.filterManager = filterManager;
            this.indexTuple = new PermutingTupleReference(indexFields);
            this.filterTuple = new PermutingTupleReference(filterFields);
            this.filterCmp = filterCmp;
        } else {
            this.filterManager = null;
            this.indexTuple = null;
            this.filterTuple = null;
            this.filterCmp = null;
        }
    }

    @Override
    public void add(ITupleReference tuple) throws HyracksDataException {
        try {
            ITupleReference t;
            if (indexTuple != null) {
                indexTuple.reset(tuple);
                t = indexTuple;
            } else {
                t = tuple;
            }

            indexBulkLoader.add(t);
            if (bloomFilterBuilder != null) {
                bloomFilterBuilder.add(t);
            }
            updateFilter(tuple);

        } catch (Exception e) {
            cleanupArtifacts();
            throw e;
        }
        if (isEmptyComponent) {
            isEmptyComponent = false;
        }
    }

    @Override
    public void delete(ITupleReference tuple) throws HyracksDataException {
        ILSMTreeTupleWriter tupleWriter =
                (ILSMTreeTupleWriter) ((AbstractTreeIndexBulkLoader) indexBulkLoader).getLeafFrame().getTupleWriter();
        tupleWriter.setAntimatter(true);
        try {
            ITupleReference t;
            if (indexTuple != null) {
                indexTuple.reset(tuple);
                t = indexTuple;
            } else {
                t = tuple;
            }

            indexBulkLoader.add(t);

            updateFilter(tuple);
        } catch (Exception e) {
            cleanupArtifacts();
            throw e;
        } finally {
            tupleWriter.setAntimatter(false);
        }
        if (isEmptyComponent) {
            isEmptyComponent = false;
        }
    }

    @Override
    public void abort() throws HyracksDataException {
        if (indexBulkLoader != null) {
            indexBulkLoader.abort();
        }
        if (bloomFilterBuilder != null) {
            bloomFilterBuilder.abort();
        }

    }

    @Override
    public void end() throws HyracksDataException {
        if (!cleanedUpArtifacts) {
            if (bloomFilterBuilder != null && !endedBloomFilterLoad) {
                bloomFilterBuilder.end();
                endedBloomFilterLoad = true;
            }

            //use filter
            if (filterManager != null && component.getLSMComponentFilter() != null) {
                filterManager.writeFilter(component.getLSMComponentFilter(), getTreeIndex(component));
            }
            indexBulkLoader.end();

            if (isEmptyComponent) {
                cleanupArtifacts();
            }
        }
    }

    protected void cleanupArtifacts() throws HyracksDataException {
        if (!cleanedUpArtifacts) {
            cleanedUpArtifacts = true;
            if (bloomFilterBuilder != null && !endedBloomFilterLoad) {
                bloomFilterBuilder.abort();
                endedBloomFilterLoad = true;
            }
            getIndex(component).deactivate();
            getIndex(component).destroy();
            if (bloomFilterBuilder != null) {
                getBloomFilter(component).deactivate();
                getBloomFilter(component).destroy();
            }
        }
    }

    protected void updateFilter(ITupleReference tuple) throws HyracksDataException {
        if (filterTuple != null) {
            filterTuple.reset(tuple);
            component.getLSMComponentFilter().update(filterTuple, filterCmp);
        }
    }

    /**
     * TreeIndex is used to hold the filter tuple values
     *
     * @param component
     * @return
     */
    protected ITreeIndex getTreeIndex(ILSMDiskComponent component) {
        return (ITreeIndex) getIndex(component);
    }

    protected abstract IIndex getIndex(ILSMDiskComponent component);

    protected abstract BloomFilter getBloomFilter(ILSMDiskComponent component);

}
