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
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManagerFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public abstract class TreeIndexFactory<T extends ITreeIndex> extends IndexFactory<T> {

    protected final ITreeIndexFrameFactory interiorFrameFactory;
    protected final ITreeIndexFrameFactory leafFrameFactory;
    protected final IBinaryComparatorFactory[] cmpFactories;
    protected final int fieldCount;

    public TreeIndexFactory(IBufferCache bufferCache, IFileMapProvider fileMapProvider,
            IFreePageManagerFactory freePageManagerFactory, ITreeIndexFrameFactory interiorFrameFactory,
            ITreeIndexFrameFactory leafFrameFactory, IBinaryComparatorFactory[] cmpFactories, int fieldCount) {
        super(bufferCache, fileMapProvider, freePageManagerFactory);
        this.interiorFrameFactory = interiorFrameFactory;
        this.leafFrameFactory = leafFrameFactory;
        this.cmpFactories = cmpFactories;
        this.fieldCount = fieldCount;
    }
}