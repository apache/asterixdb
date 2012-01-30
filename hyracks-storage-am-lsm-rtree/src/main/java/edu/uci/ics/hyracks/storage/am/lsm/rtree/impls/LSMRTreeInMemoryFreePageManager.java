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

package edu.uci.ics.hyracks.storage.am.lsm.rtree.impls;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryFreePageManager;

public class LSMRTreeInMemoryFreePageManager extends InMemoryFreePageManager {

    public LSMRTreeInMemoryFreePageManager(int capacity, ITreeIndexMetaDataFrameFactory metaDataFrameFactory) {
        super(capacity, metaDataFrameFactory);
        // We start the currentPageId from 3, because the RTree uses
        // the first page as metadata page, and the second page as root page.
        // And the BTree uses the third page as metadata, and the third page as root page 
        // (when returning free pages we first increment, then get)
        currentPageId.set(3);
    }

    @Override
    public void init(ITreeIndexMetaDataFrame metaFrame, int currentMaxPage) throws HyracksDataException {
        currentPageId.set(3);
    }

    public int getCapacity() {
        return capacity - 4;
    }
    
    public void reset() {
        currentPageId.set(3);
    }
}
