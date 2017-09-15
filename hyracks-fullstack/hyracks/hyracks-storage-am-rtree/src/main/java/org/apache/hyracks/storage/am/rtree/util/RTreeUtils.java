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

package org.apache.hyracks.storage.am.rtree.util;

import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.data.std.api.IPointableFactory;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IPageManagerFactory;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.data.PointablePrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.rtree.frames.RTreeNSMInteriorFrameFactory;
import org.apache.hyracks.storage.am.rtree.frames.RTreeNSMLeafFrameFactory;
import org.apache.hyracks.storage.am.rtree.frames.RTreePolicyType;
import org.apache.hyracks.storage.am.rtree.impls.RTree;
import org.apache.hyracks.storage.am.rtree.tuples.RTreeTypeAwareTupleWriterFactory;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;

public class RTreeUtils {

    private RTreeUtils() {
    }

    public static RTree createRTree(IBufferCache bufferCache, ITypeTraits[] typeTraits,
            IPrimitiveValueProviderFactory[] valueProviderFactories, IBinaryComparatorFactory[] cmpFactories,
            RTreePolicyType rtreePolicyType, FileReference file, boolean isPointMBR,
            IPageManagerFactory pageManagerFactory) {

        RTreeTypeAwareTupleWriterFactory tupleWriterFactory = new RTreeTypeAwareTupleWriterFactory(typeTraits);
        ITreeIndexFrameFactory interiorFrameFactory = new RTreeNSMInteriorFrameFactory(tupleWriterFactory,
                valueProviderFactories, rtreePolicyType, isPointMBR);
        ITreeIndexFrameFactory leafFrameFactory =
                new RTreeNSMLeafFrameFactory(tupleWriterFactory, valueProviderFactories, rtreePolicyType, isPointMBR);
        return new RTree(bufferCache, pageManagerFactory.createPageManager(bufferCache), interiorFrameFactory,
                leafFrameFactory, cmpFactories, typeTraits.length, file, isPointMBR);
    }

    // Creates a new MultiComparator by constructing new IBinaryComparators.
    public static MultiComparator getSearchMultiComparator(IBinaryComparatorFactory[] cmpFactories,
            ITupleReference searchKey) {
        if (searchKey == null || cmpFactories.length == searchKey.getFieldCount()) {
            return MultiComparator.create(cmpFactories);
        }
        IBinaryComparator[] newCmps = new IBinaryComparator[searchKey.getFieldCount()];
        for (int i = 0; i < searchKey.getFieldCount(); i++) {
            newCmps[i] = cmpFactories[i].createBinaryComparator();
        }
        return new MultiComparator(newCmps);
    }

    public static IPrimitiveValueProviderFactory[] createPrimitiveValueProviderFactories(int len,
            IPointableFactory pf) {
        IPrimitiveValueProviderFactory[] pvpfs = new IPrimitiveValueProviderFactory[len];
        for (int i = 0; i < len; ++i) {
            pvpfs[i] = new PointablePrimitiveValueProviderFactory(pf);
        }
        return pvpfs;
    }
}
