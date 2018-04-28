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

package org.apache.hyracks.storage.am.lsm.invertedindex.impls;

import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilter;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMWithBuddyMemoryComponent;
import org.apache.hyracks.storage.am.lsm.invertedindex.inmemory.InMemoryInvertedIndex;

public class LSMInvertedIndexMemoryComponent extends AbstractLSMWithBuddyMemoryComponent {

    private final InMemoryInvertedIndex invIndex;
    private final BTree deletedKeysBTree;

    public LSMInvertedIndexMemoryComponent(LSMInvertedIndex lsmIndex, InMemoryInvertedIndex invIndex,
            BTree deletedKeysBTree, IVirtualBufferCache vbc, ILSMComponentFilter filter) {
        super(lsmIndex, vbc, filter);
        this.invIndex = invIndex;
        this.deletedKeysBTree = deletedKeysBTree;
    }

    @Override
    public InMemoryInvertedIndex getIndex() {
        return invIndex;
    }

    @Override
    public BTree getBuddyIndex() {
        return deletedKeysBTree;
    }
}
