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

import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.frames.BTreeFieldPrefixNSMLeafFrameFactory;
import org.apache.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import org.apache.hyracks.storage.am.btree.frames.BTreeNSMInteriorFrameFactory;
import org.apache.hyracks.storage.am.btree.frames.BTreeNSMLeafFrameFactory;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.impls.DiskBTree;
import org.apache.hyracks.storage.am.btree.tuples.BTreeTypeAwareTupleWriterFactory;
import org.apache.hyracks.storage.am.common.api.IPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;

public class BTreeUtils {

    private BTreeUtils() {
    }

    public static BTree createBTree(IBufferCache bufferCache, ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] cmpFactories, BTreeLeafFrameType leafType, FileReference file,
            IPageManager freePageManager, boolean updateAware) throws HyracksDataException {
        BTreeTypeAwareTupleWriterFactory tupleWriterFactory =
                new BTreeTypeAwareTupleWriterFactory(typeTraits, updateAware);
        ITreeIndexFrameFactory leafFrameFactory = getLeafFrameFactory(tupleWriterFactory, leafType);
        ITreeIndexFrameFactory interiorFrameFactory = new BTreeNSMInteriorFrameFactory(tupleWriterFactory);
        return new BTree(bufferCache, freePageManager, interiorFrameFactory, leafFrameFactory, cmpFactories,
                typeTraits.length, file);
    }

    public static DiskBTree createDiskBTree(IBufferCache bufferCache, ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] cmpFactories, BTreeLeafFrameType leafType, FileReference file,
            IPageManager freePageManager, boolean updateAware) throws HyracksDataException {
        BTreeTypeAwareTupleWriterFactory tupleWriterFactory =
                new BTreeTypeAwareTupleWriterFactory(typeTraits, updateAware);
        ITreeIndexFrameFactory leafFrameFactory = getLeafFrameFactory(tupleWriterFactory, leafType);
        ITreeIndexFrameFactory interiorFrameFactory = new BTreeNSMInteriorFrameFactory(tupleWriterFactory);
        return new DiskBTree(bufferCache, freePageManager, interiorFrameFactory, leafFrameFactory, cmpFactories,
                typeTraits.length, file);
    }

    public static BTree createBTree(IBufferCache bufferCache, IPageManager freePageManager, ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] cmpFactories, BTreeLeafFrameType leafType, FileReference file,
            boolean updateAware) throws HyracksDataException {
        BTreeTypeAwareTupleWriterFactory tupleWriterFactory =
                new BTreeTypeAwareTupleWriterFactory(typeTraits, updateAware);
        ITreeIndexFrameFactory leafFrameFactory = getLeafFrameFactory(tupleWriterFactory, leafType);
        ITreeIndexFrameFactory interiorFrameFactory = new BTreeNSMInteriorFrameFactory(tupleWriterFactory);
        return new BTree(bufferCache, freePageManager, interiorFrameFactory, leafFrameFactory, cmpFactories,
                typeTraits.length, file);
    }

    // Creates a new MultiComparator by constructing new IBinaryComparators.
    public static MultiComparator getSearchMultiComparator(IBinaryComparatorFactory[] cmpFactories,
            ITupleReference searchKey) {
        if (searchKey == null || cmpFactories.length == searchKey.getFieldCount()) {
            return MultiComparator.create(cmpFactories);
        }
        IBinaryComparator[] newCmps = new IBinaryComparator[searchKey.getFieldCount()];
        for (int i = 0; i < searchKey.getFieldCount(); i++) {
            newCmps[i] = cmpFactories[i].createBinaryComparator();
        }
        return new MultiComparator(newCmps);
    }

    public static ITreeIndexFrameFactory getLeafFrameFactory(BTreeTypeAwareTupleWriterFactory tupleWriterFactory,
            BTreeLeafFrameType leafType) throws HyracksDataException {
        switch (leafType) {
            case REGULAR_NSM: {
                return new BTreeNSMLeafFrameFactory(tupleWriterFactory);
            }
            case FIELD_PREFIX_COMPRESSED_NSM: {
                return new BTreeFieldPrefixNSMLeafFrameFactory(tupleWriterFactory);
            }
            default: {
                throw new HyracksDataException("Unknown BTreeLeafFrameType: " + leafType.toString());
            }
        }
    }
}
