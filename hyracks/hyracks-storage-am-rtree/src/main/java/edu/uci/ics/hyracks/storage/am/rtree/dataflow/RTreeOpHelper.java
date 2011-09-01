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

package edu.uci.ics.hyracks.storage.am.rtree.dataflow;

import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProvider;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.ITreeIndexOperatorDescriptorHelper;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IndexHelperOpenMode;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexOpHelper;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.freepage.LinkedListFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTree;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;

public class RTreeOpHelper extends TreeIndexOpHelper {

    public RTreeOpHelper(ITreeIndexOperatorDescriptorHelper opDesc, IHyracksStageletContext ctx, int partition,
            IndexHelperOpenMode mode) {
        super(opDesc, ctx, partition, mode);
    }

    public ITreeIndex createTreeIndex() throws HyracksDataException {
        IBufferCache bufferCache = opDesc.getStorageManager().getBufferCache(ctx);
        ITreeIndexMetaDataFrameFactory metaDataFrameFactory = new LIFOMetaDataFrameFactory();
        IFreePageManager freePageManager = new LinkedListFreePageManager(bufferCache, indexFileId, 0,
                metaDataFrameFactory);

        return new RTree(bufferCache, freePageManager, opDesc.getTreeIndexInteriorFactory(),
                opDesc.getTreeIndexLeafFactory(), cmp);
    }

    public MultiComparator createMultiComparator(IBinaryComparator[] comparators) throws HyracksDataException {
        IPrimitiveValueProvider[] keyValueProvider = new IPrimitiveValueProvider[opDesc
                .getTreeIndexValueProviderFactories().length];
        for (int i = 0; i < opDesc.getTreeIndexComparatorFactories().length; i++) {
            keyValueProvider[i] = opDesc.getTreeIndexValueProviderFactories()[i].createPrimitiveValueProvider();
        }
        return new MultiComparator(opDesc.getTreeIndexTypeTraits(), comparators, keyValueProvider);
    }
}
