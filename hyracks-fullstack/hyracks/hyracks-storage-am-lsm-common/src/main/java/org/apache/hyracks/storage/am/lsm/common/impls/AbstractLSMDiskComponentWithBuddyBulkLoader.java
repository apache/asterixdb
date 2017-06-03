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

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilterSpecification;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilterManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.MultiComparator;

public abstract class AbstractLSMDiskComponentWithBuddyBulkLoader extends AbstractLSMDiskComponentBulkLoader {

    protected final IIndexBulkLoader buddyBTreeBulkLoader;

    //with filter
    public AbstractLSMDiskComponentWithBuddyBulkLoader(ILSMDiskComponent component,
            BloomFilterSpecification bloomFilterSpec, float fillFactor, boolean verifyInput, long numElementsHint,
            boolean checkIfEmptyIndex, ILSMComponentFilterManager filterManager, int[] indexFields, int[] filterFields,
            MultiComparator filterCmp) throws HyracksDataException {
        super(component, bloomFilterSpec, fillFactor, verifyInput, numElementsHint, checkIfEmptyIndex, filterManager,
                indexFields, filterFields, filterCmp);

        // BuddyBTree must be created even if it could be empty,
        // since without it the component is not considered as valid.
        buddyBTreeBulkLoader =
                getBuddyBTree(component).createBulkLoader(fillFactor, verifyInput, numElementsHint, checkIfEmptyIndex);
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
        try {
            ITupleReference t;
            if (indexTuple != null) {
                indexTuple.reset(tuple);
                t = indexTuple;
            } else {
                t = tuple;
            }

            buddyBTreeBulkLoader.add(t);
            if (bloomFilterBuilder != null) {
                bloomFilterBuilder.add(t);
            }

            updateFilter(tuple);
        } catch (HyracksDataException e) {
            //deleting a key multiple times is OK
            if (e.getErrorCode() != ErrorCode.DUPLICATE_KEY) {
                cleanupArtifacts();
                throw e;
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
    public void abort() throws HyracksDataException {
        super.abort();
        if (buddyBTreeBulkLoader != null) {
            buddyBTreeBulkLoader.abort();
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
            buddyBTreeBulkLoader.end();

            if (isEmptyComponent) {
                cleanupArtifacts();
            }
        }
    }

    @Override
    protected void cleanupArtifacts() throws HyracksDataException {
        if (!cleanedUpArtifacts) {
            cleanedUpArtifacts = true;
            if (bloomFilterBuilder != null && !endedBloomFilterLoad) {
                bloomFilterBuilder.abort();
                endedBloomFilterLoad = true;
            }
            getIndex(component).deactivate();
            getIndex(component).destroy();

            getBuddyBTree(component).deactivate();
            getBuddyBTree(component).destroy();

            if (bloomFilterBuilder != null) {
                getBloomFilter(component).deactivate();
                getBloomFilter(component).destroy();
            }
        }
    }

    protected abstract ITreeIndex getBuddyBTree(ILSMDiskComponent component);

}
