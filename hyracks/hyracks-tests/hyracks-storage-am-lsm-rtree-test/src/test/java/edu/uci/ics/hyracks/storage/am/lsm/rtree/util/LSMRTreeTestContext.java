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

package edu.uci.ics.hyracks.storage.am.lsm.rtree.util;

import java.util.Collection;
import java.util.List;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.util.SerdeUtils;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.rtree.impls.LSMRTree;
import edu.uci.ics.hyracks.storage.am.lsm.rtree.utils.LSMRTreeUtils;
import edu.uci.ics.hyracks.storage.am.rtree.AbstractRTreeTestContext;
import edu.uci.ics.hyracks.storage.am.rtree.RTreeCheckTuple;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreePolicyType;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

@SuppressWarnings("rawtypes")
public final class LSMRTreeTestContext extends AbstractRTreeTestContext {

    public LSMRTreeTestContext(ISerializerDeserializer[] fieldSerdes, ITreeIndex treeIndex) throws HyracksDataException {
        super(fieldSerdes, treeIndex);
    }

    @Override
    public int getKeyFieldCount() {
        LSMRTree lsmTree = (LSMRTree) index;
        return lsmTree.getComparatorFactories().length;
    }

    /**
     * Override to provide delete semantics for the check tuples.
     */
    @Override
    public void deleteCheckTuple(RTreeCheckTuple checkTuple, Collection<RTreeCheckTuple> checkTuples) {
        while (checkTuples.remove(checkTuple)) {
        }
    }

    @Override
    public IBinaryComparatorFactory[] getComparatorFactories() {
        LSMRTree lsmTree = (LSMRTree) index;
        return lsmTree.getComparatorFactories();
    }

    public static LSMRTreeTestContext create(List<IVirtualBufferCache> virtualBufferCaches, FileReference file,
            IBufferCache diskBufferCache, IFileMapProvider diskFileMapProvider, ISerializerDeserializer[] fieldSerdes,
            IPrimitiveValueProviderFactory[] valueProviderFactories, int numKeyFields, RTreePolicyType rtreePolicyType,
            double bloomFilterFalsePositiveRate, ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker,
            ILSMIOOperationScheduler ioScheduler, ILSMIOOperationCallback ioOpCallback) throws Exception {
        ITypeTraits[] typeTraits = SerdeUtils.serdesToTypeTraits(fieldSerdes);
        IBinaryComparatorFactory[] rtreeCmpFactories = SerdeUtils
                .serdesToComparatorFactories(fieldSerdes, numKeyFields);
        IBinaryComparatorFactory[] btreeCmpFactories = SerdeUtils.serdesToComparatorFactories(fieldSerdes,
                fieldSerdes.length);
        LSMRTree lsmTree = LSMRTreeUtils.createLSMTree(virtualBufferCaches, file, diskBufferCache, diskFileMapProvider,
                typeTraits, rtreeCmpFactories, btreeCmpFactories, valueProviderFactories, rtreePolicyType,
                bloomFilterFalsePositiveRate, mergePolicy, opTracker, ioScheduler, ioOpCallback,
                LSMRTreeUtils.proposeBestLinearizer(typeTraits, rtreeCmpFactories.length));
        LSMRTreeTestContext testCtx = new LSMRTreeTestContext(fieldSerdes, lsmTree);
        return testCtx;
    }
}
