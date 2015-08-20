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

package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManagerFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class BTreeFactory extends TreeIndexFactory<BTree> {

    public BTreeFactory(IBufferCache bufferCache, IFileMapProvider fileMapProvider,
            IFreePageManagerFactory freePageManagerFactory, ITreeIndexFrameFactory interiorFrameFactory,
            ITreeIndexFrameFactory leafFrameFactory, IBinaryComparatorFactory[] cmpFactories, int fieldCount) {
        super(bufferCache, fileMapProvider, freePageManagerFactory, interiorFrameFactory, leafFrameFactory,
                cmpFactories, fieldCount);
    }

    @Override
    public BTree createIndexInstance(FileReference file) throws IndexException {
        return new BTree(bufferCache, fileMapProvider, freePageManagerFactory.createFreePageManager(),
                interiorFrameFactory, leafFrameFactory, cmpFactories, fieldCount, file);
    }

}