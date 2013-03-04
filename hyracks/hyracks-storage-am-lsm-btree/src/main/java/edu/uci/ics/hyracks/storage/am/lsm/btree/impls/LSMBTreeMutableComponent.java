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

package edu.uci.ics.hyracks.storage.am.lsm.btree.impls;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.common.api.IInMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.AbstractMutableLSMComponent;

public class LSMBTreeMutableComponent extends AbstractMutableLSMComponent {

    private final BTree btree;
    private final IInMemoryFreePageManager mfpm;

    public LSMBTreeMutableComponent(BTree btree, IInMemoryFreePageManager mfpm) {
        this.btree = btree;
        this.mfpm = mfpm;
    }

    public BTree getBTree() {
        return btree;
    }

    @Override
    protected boolean isFull() {
        return mfpm.isFull();
    }

    @Override
    protected void reset() throws HyracksDataException {
        btree.clear();
    }

}
