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

package edu.uci.ics.hyracks.storage.am.common.freepage;

import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManagerFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;

public class LinkedListFreePageManagerFactory implements IFreePageManagerFactory {

    private final ITreeIndexMetaDataFrameFactory metaDataFrameFactory;
    private final IBufferCache bufferCache;

    public LinkedListFreePageManagerFactory(IBufferCache bufferCache,
            ITreeIndexMetaDataFrameFactory metaDataFrameFactory) {
        this.metaDataFrameFactory = metaDataFrameFactory;
        this.bufferCache = bufferCache;
    }

    public IFreePageManager createFreePageManager() {
        return new LinkedListFreePageManager(bufferCache, 0, metaDataFrameFactory);
    }
}
