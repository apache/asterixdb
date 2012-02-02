/*
 * Copyright 2009-2010 by The Regents of the University of California
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

package edu.uci.ics.hyracks.storage.am.rtree.utils;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.dataflow.common.util.SerdeUtils;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTree;
import edu.uci.ics.hyracks.storage.am.rtree.tests.AbstractRTreeTestContext;
import edu.uci.ics.hyracks.storage.am.rtree.util.RTreeUtils;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;

@SuppressWarnings("rawtypes")
public class RTreeTestContext extends AbstractRTreeTestContext {

    public RTreeTestContext(ISerializerDeserializer[] fieldSerdes, ITreeIndex treeIndex) {
        super(fieldSerdes, treeIndex);
    }

    @Override
    public int getKeyFieldCount() {
        RTree rtree = (RTree) treeIndex;
        return rtree.getComparatorFactories().length;
    }

    @Override
    public IBinaryComparatorFactory[] getComparatorFactories() {
        RTree rtree = (RTree) treeIndex;
        return rtree.getComparatorFactories();
    }

    public static RTreeTestContext create(IBufferCache bufferCache, int rtreeFileId,
            ISerializerDeserializer[] fieldSerdes, IPrimitiveValueProviderFactory[] valueProviderFactories,
            int numKeyFields) throws Exception {
        ITypeTraits[] typeTraits = SerdeUtils.serdesToTypeTraits(fieldSerdes);
        IBinaryComparatorFactory[] cmpFactories = SerdeUtils.serdesToComparatorFactories(fieldSerdes, numKeyFields);
        RTree rtree = RTreeUtils
                .createRTree(bufferCache, rtreeFileId, typeTraits, valueProviderFactories, cmpFactories);
        rtree.create(rtreeFileId);
        rtree.open(rtreeFileId);
        RTreeTestContext testCtx = new RTreeTestContext(fieldSerdes, rtree);
        return testCtx;
    }
}
