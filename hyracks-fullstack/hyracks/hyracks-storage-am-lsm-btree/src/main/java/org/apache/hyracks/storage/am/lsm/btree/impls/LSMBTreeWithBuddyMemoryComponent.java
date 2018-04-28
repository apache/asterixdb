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
package org.apache.hyracks.storage.am.lsm.btree.impls;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilter;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMWithBuddyMemoryComponent;

/*
 * This class is also not needed at the moment but is implemented anyway
 */
public class LSMBTreeWithBuddyMemoryComponent extends AbstractLSMWithBuddyMemoryComponent {

    private final BTree btree;
    private final BTree buddyBtree;

    public LSMBTreeWithBuddyMemoryComponent(AbstractLSMIndex lsmIndex, BTree btree, BTree buddyBtree,
            IVirtualBufferCache vbc, ILSMComponentFilter filter) {
        super(lsmIndex, vbc, filter);
        this.btree = btree;
        this.buddyBtree = buddyBtree;
    }

    @Override
    public BTree getIndex() {
        return btree;
    }

    @Override
    public BTree getBuddyIndex() {
        return buddyBtree;
    }

    @Override
    public void validate() throws HyracksDataException {
        throw new UnsupportedOperationException("Validation not implemented for LSM B-Trees with Buddy B-Tree.");
    }

    @Override
    public long getSize() {
        return 0L;
    }
}
