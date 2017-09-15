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

package org.apache.hyracks.storage.am.btree.util;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.utils.SerdeUtils;
import org.apache.hyracks.storage.am.btree.OrderedIndexTestContext;
import org.apache.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.common.api.IPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;

@SuppressWarnings("rawtypes")
public class BTreeTestContext extends OrderedIndexTestContext {

    public BTreeTestContext(ISerializerDeserializer[] fieldSerdes, ITreeIndex treeIndex) throws HyracksDataException {
        super(fieldSerdes, treeIndex, false);
    }

    @Override
    public int getKeyFieldCount() {
        BTree btree = (BTree) index;
        return btree.getComparatorFactories().length;
    }

    @Override
    public IBinaryComparatorFactory[] getComparatorFactories() {
        BTree btree = (BTree) index;
        return btree.getComparatorFactories();
    }

    public static BTreeTestContext create(IBufferCache bufferCache, FileReference file,
            ISerializerDeserializer[] fieldSerdes, int numKeyFields, BTreeLeafFrameType leafType,
            IPageManager pageManager) throws Exception {
        ITypeTraits[] typeTraits = SerdeUtils.serdesToTypeTraits(fieldSerdes);
        IBinaryComparatorFactory[] cmpFactories = SerdeUtils.serdesToComparatorFactories(fieldSerdes, numKeyFields);
        BTree btree = BTreeUtils.createBTree(bufferCache, typeTraits, cmpFactories, leafType, file, pageManager, false);
        BTreeTestContext testCtx = new BTreeTestContext(fieldSerdes, btree);
        return testCtx;
    }
}
