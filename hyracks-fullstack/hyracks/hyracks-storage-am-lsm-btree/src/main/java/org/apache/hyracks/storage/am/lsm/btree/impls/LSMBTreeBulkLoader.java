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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomCalculations;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilterSpecification;
import org.apache.hyracks.storage.am.btree.impls.BTree.BTreeBulkLoader;
import org.apache.hyracks.storage.am.common.tuples.PermutingTupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.LSMOperationType;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.MultiComparator;

public class LSMBTreeBulkLoader implements IIndexBulkLoader {
    private final LSMBTree lsmIndex;
    private final ILSMDiskComponent component;
    private final BTreeBulkLoader bulkLoader;
    private final IIndexBulkLoader builder;
    private boolean cleanedUpArtifacts = false;
    private boolean isEmptyComponent = true;
    private boolean endedBloomFilterLoad = false;
    public final PermutingTupleReference indexTuple;
    public final PermutingTupleReference filterTuple;
    public final MultiComparator filterCmp;

    public LSMBTreeBulkLoader(LSMBTree lsmIndex, float fillFactor, boolean verifyInput, long numElementsHint)
            throws HyracksDataException {
        this.lsmIndex = lsmIndex;
        component = lsmIndex.createBulkLoadTarget();
        bulkLoader = (BTreeBulkLoader) ((LSMBTreeDiskComponent) component).getBTree().createBulkLoader(fillFactor,
                verifyInput, numElementsHint, false);

        if (lsmIndex.hasBloomFilter()) {
            int maxBucketsPerElement = BloomCalculations.maxBucketsPerElement(numElementsHint);
            BloomFilterSpecification bloomFilterSpec =
                    BloomCalculations.computeBloomSpec(maxBucketsPerElement, lsmIndex.bloomFilterFalsePositiveRate());
            builder = ((LSMBTreeDiskComponent) component).getBloomFilter().createBuilder(numElementsHint,
                    bloomFilterSpec.getNumHashes(), bloomFilterSpec.getNumBucketsPerElements());
        } else {
            builder = null;
        }

        if (lsmIndex.getFilterFields() != null) {
            indexTuple = new PermutingTupleReference(lsmIndex.getTreeFields());
            filterCmp = MultiComparator.create(component.getLSMComponentFilter().getFilterCmpFactories());
            filterTuple = new PermutingTupleReference(lsmIndex.getFilterFields());
        } else {
            indexTuple = null;
            filterCmp = null;
            filterTuple = null;
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

            bulkLoader.add(t);
            if (lsmIndex.hasBloomFilter()) {
                builder.add(t);
            }

            if (filterTuple != null) {
                filterTuple.reset(tuple);
                component.getLSMComponentFilter().update(filterTuple, filterCmp);
            }
        } catch (Exception e) {
            cleanupArtifacts();
            throw e;
        }
        if (isEmptyComponent) {
            isEmptyComponent = false;
        }
    }

    private void cleanupArtifacts() throws HyracksDataException {
        if (!cleanedUpArtifacts) {
            cleanedUpArtifacts = true;
            if (lsmIndex.hasBloomFilter() && !endedBloomFilterLoad) {
                builder.abort();
                endedBloomFilterLoad = true;
            }
            ((LSMBTreeDiskComponent) component).getBTree().deactivate();
            ((LSMBTreeDiskComponent) component).getBTree().destroy();
            if (lsmIndex.hasBloomFilter()) {
                ((LSMBTreeDiskComponent) component).getBloomFilter().deactivate();
                ((LSMBTreeDiskComponent) component).getBloomFilter().destroy();
            }
        }
    }

    @Override
    public void end() throws HyracksDataException {
        if (!cleanedUpArtifacts) {
            if (lsmIndex.hasBloomFilter() && !endedBloomFilterLoad) {
                builder.end();
                endedBloomFilterLoad = true;
            }

            if (component.getLSMComponentFilter() != null) {
                lsmIndex.getFilterManager().writeFilter(component.getLSMComponentFilter(),
                        ((LSMBTreeDiskComponent) component).getBTree());
            }
            bulkLoader.end();

            if (isEmptyComponent) {
                cleanupArtifacts();
            } else {
                //TODO(amoudi): Ensure Bulk load follow the same lifecycle Other Operations (Flush, Merge, etc).
                //then after operation should be called from harness as well
                //https://issues.apache.org/jira/browse/ASTERIXDB-1764
                lsmIndex.getIOOperationCallback().afterOperation(LSMOperationType.FLUSH, null, component);
                lsmIndex.getLsmHarness().addBulkLoadedComponent(component);
            }
        }
    }

    @Override
    public void abort() throws HyracksDataException {
        if (bulkLoader != null) {
            bulkLoader.abort();
        }

        if (builder != null) {
            builder.abort();
        }

    }
}