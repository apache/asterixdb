/*
 * Copyright 2009-2012 by The Regents of the University of California
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

package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.common.api.IOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.freepage.LinkedListFreePageManagerFactory;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;

public class BTreeFactory extends TreeFactory<BTree> {

    private final IOperationCallback opCallback;
    
    public BTreeFactory(IBufferCache bufferCache, IOperationCallback opCallback, LinkedListFreePageManagerFactory freePageManagerFactory,
            IBinaryComparatorFactory[] cmpFactories, int fieldCount, ITreeIndexFrameFactory interiorFrameFactory,
            ITreeIndexFrameFactory leafFrameFactory) {
        super(bufferCache, freePageManagerFactory, cmpFactories, fieldCount, interiorFrameFactory, leafFrameFactory);
        this.opCallback = opCallback;
    }

    @Override
    public BTree createIndexInstance() {
        return new BTree(bufferCache, opCallback, fieldCount, cmpFactories, freePageManagerFactory.createFreePageManager(),
                interiorFrameFactory, leafFrameFactory);
    }

}
