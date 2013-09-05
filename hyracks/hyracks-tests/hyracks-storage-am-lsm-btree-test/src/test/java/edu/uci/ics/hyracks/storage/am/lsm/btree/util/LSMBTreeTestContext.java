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

package edu.uci.ics.hyracks.storage.am.lsm.btree.util;

import java.util.Collection;
import java.util.List;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.util.SerdeUtils;
import edu.uci.ics.hyracks.storage.am.btree.OrderedIndexTestContext;
import edu.uci.ics.hyracks.storage.am.common.CheckTuple;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.lsm.btree.impls.LSMBTree;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

@SuppressWarnings("rawtypes")
public final class LSMBTreeTestContext extends OrderedIndexTestContext {

    public LSMBTreeTestContext(ISerializerDeserializer[] fieldSerdes, ITreeIndex treeIndex) throws HyracksDataException {
        super(fieldSerdes, treeIndex);
    }

    @Override
    public int getKeyFieldCount() {
        LSMBTree lsmTree = (LSMBTree) index;
        return lsmTree.getComparatorFactories().length;
    }

    @Override
    public IBinaryComparatorFactory[] getComparatorFactories() {
        LSMBTree lsmTree = (LSMBTree) index;
        return lsmTree.getComparatorFactories();
    }

    /**
     * Override to provide upsert semantics for the check tuples.
     */
    @Override
    public void insertCheckTuple(CheckTuple checkTuple, Collection<CheckTuple> checkTuples) {
        upsertCheckTuple(checkTuple, checkTuples);
    }

    public static LSMBTreeTestContext create(List<IVirtualBufferCache> virtualBufferCaches, FileReference file,
            IBufferCache diskBufferCache, IFileMapProvider diskFileMapProvider, ISerializerDeserializer[] fieldSerdes,
            int numKeyFields, double bloomFilterFalsePositiveRate, ILSMMergePolicy mergePolicy,
            ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler, ILSMIOOperationCallback ioOpCallback)
            throws Exception {
        ITypeTraits[] typeTraits = SerdeUtils.serdesToTypeTraits(fieldSerdes);
        IBinaryComparatorFactory[] cmpFactories = SerdeUtils.serdesToComparatorFactories(fieldSerdes, numKeyFields);
        int[] bloomFilterKeyFields = new int[numKeyFields];
        for (int i = 0; i < numKeyFields; ++i) {
            bloomFilterKeyFields[i] = i;
        }
        LSMBTree lsmTree = LSMBTreeUtils.createLSMTree(virtualBufferCaches, file, diskBufferCache, diskFileMapProvider,
                typeTraits, cmpFactories, bloomFilterKeyFields, bloomFilterFalsePositiveRate, mergePolicy, opTracker,
                ioScheduler, ioOpCallback);
        LSMBTreeTestContext testCtx = new LSMBTreeTestContext(fieldSerdes, lsmTree);
        return testCtx;
    }
}
