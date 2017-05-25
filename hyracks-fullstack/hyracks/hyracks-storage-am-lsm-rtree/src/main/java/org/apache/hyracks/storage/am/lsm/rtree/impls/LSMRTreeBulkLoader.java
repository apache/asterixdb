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
package org.apache.hyracks.storage.am.lsm.rtree.impls;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.tuples.PermutingTupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.LSMOperationType;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.MultiComparator;

public class LSMRTreeBulkLoader implements IIndexBulkLoader {
    private final ILSMDiskComponent component;
    private final IIndexBulkLoader bulkLoader;
    private final IIndexBulkLoader buddyBTreeBulkloader;
    private boolean cleanedUpArtifacts = false;
    private boolean isEmptyComponent = true;
    public final PermutingTupleReference indexTuple;
    public final PermutingTupleReference filterTuple;
    public final MultiComparator filterCmp;
    private final LSMRTree lsmIndex;

    public LSMRTreeBulkLoader(LSMRTree lsmIndex, float fillFactor, boolean verifyInput, long numElementsHint)
            throws HyracksDataException {
        this.lsmIndex = lsmIndex;
        // Note that by using a flush target file name, we state that the
        // new bulk loaded tree is "newer" than any other merged tree.
        component = lsmIndex.createBulkLoadTarget();
        bulkLoader = ((LSMRTreeDiskComponent) component).getRTree().createBulkLoader(fillFactor, verifyInput,
                numElementsHint, false);
        buddyBTreeBulkloader = ((LSMRTreeDiskComponent) component).getBTree().createBulkLoader(fillFactor, verifyInput,
                numElementsHint, false);
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

    @Override
    public void end() throws HyracksDataException {
        if (!cleanedUpArtifacts) {

            if (component.getLSMComponentFilter() != null) {
                lsmIndex.getFilterManager().writeFilter(component.getLSMComponentFilter(),
                        ((LSMRTreeDiskComponent) component).getRTree());
            }

            bulkLoader.end();
            buddyBTreeBulkloader.end();

            if (isEmptyComponent) {
                cleanupArtifacts();
            } else {
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
        if (buddyBTreeBulkloader != null) {
            buddyBTreeBulkloader.abort();
        }
    }

    protected void cleanupArtifacts() throws HyracksDataException {
        if (!cleanedUpArtifacts) {
            cleanedUpArtifacts = true;
            ((LSMRTreeDiskComponent) component).getRTree().deactivate();
            ((LSMRTreeDiskComponent) component).getRTree().destroy();
            ((LSMRTreeDiskComponent) component).getBTree().deactivate();
            ((LSMRTreeDiskComponent) component).getBTree().destroy();
            ((LSMRTreeDiskComponent) component).getBloomFilter().deactivate();
            ((LSMRTreeDiskComponent) component).getBloomFilter().destroy();
        }
    }
}