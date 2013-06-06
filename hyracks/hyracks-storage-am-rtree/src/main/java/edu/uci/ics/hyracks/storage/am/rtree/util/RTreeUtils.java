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

package edu.uci.ics.hyracks.storage.am.rtree.util;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.data.std.api.IPointableFactory;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.data.PointablePrimitiveValueProviderFactory;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.freepage.LinkedListFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreeNSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreeNSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreePolicyType;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTree;
import edu.uci.ics.hyracks.storage.am.rtree.tuples.RTreeTypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class RTreeUtils {
    public static RTree createRTree(IBufferCache bufferCache, IFileMapProvider fileMapProvider,
            ITypeTraits[] typeTraits, IPrimitiveValueProviderFactory[] valueProviderFactories,
            IBinaryComparatorFactory[] cmpFactories, RTreePolicyType rtreePolicyType, FileReference file) {

        RTreeTypeAwareTupleWriterFactory tupleWriterFactory = new RTreeTypeAwareTupleWriterFactory(typeTraits);
        ITreeIndexFrameFactory interiorFrameFactory = new RTreeNSMInteriorFrameFactory(tupleWriterFactory,
                valueProviderFactories, rtreePolicyType);
        ITreeIndexFrameFactory leafFrameFactory = new RTreeNSMLeafFrameFactory(tupleWriterFactory,
                valueProviderFactories, rtreePolicyType);
        ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();

        IFreePageManager freePageManager = new LinkedListFreePageManager(bufferCache, 0, metaFrameFactory);
        RTree rtree = new RTree(bufferCache, fileMapProvider, freePageManager, interiorFrameFactory, leafFrameFactory,
                cmpFactories, typeTraits.length, file);
        return rtree;
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

    public static IPrimitiveValueProviderFactory[] createPrimitiveValueProviderFactories(int len, IPointableFactory pf) {
        IPrimitiveValueProviderFactory[] pvpfs = new IPrimitiveValueProviderFactory[len];
        for (int i = 0; i < len; ++i) {
            pvpfs[i] = new PointablePrimitiveValueProviderFactory(pf);
        }
        return pvpfs;
    }
}