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
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.exceptions.BTreeException;
import org.apache.hyracks.storage.am.btree.frames.BTreeFieldPrefixNSMLeafFrameFactory;
import org.apache.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import org.apache.hyracks.storage.am.btree.frames.BTreeNSMInteriorFrameFactory;
import org.apache.hyracks.storage.am.btree.frames.BTreeNSMLeafFrameFactory;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.common.api.IFreePageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriterFactory;
import org.apache.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import org.apache.hyracks.storage.am.common.freepage.LinkedListFreePageManager;
import org.apache.hyracks.storage.am.common.ophelpers.MultiComparator;
import org.apache.hyracks.storage.am.common.tuples.TypeAwareTupleWriterFactory;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.file.IFileMapProvider;

public class BTreeUtils {
    public static BTree createBTree(IBufferCache bufferCache, IFileMapProvider fileMapProvider,
            ITypeTraits[] typeTraits, IBinaryComparatorFactory[] cmpFactories, BTreeLeafFrameType leafType,
            FileReference file) throws BTreeException {
        TypeAwareTupleWriterFactory tupleWriterFactory = new TypeAwareTupleWriterFactory(typeTraits);
        ITreeIndexFrameFactory leafFrameFactory = getLeafFrameFactory(tupleWriterFactory, leafType);
        ITreeIndexFrameFactory interiorFrameFactory = new BTreeNSMInteriorFrameFactory(tupleWriterFactory);
        ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();
        IFreePageManager freePageManager = new LinkedListFreePageManager(bufferCache, 0, metaFrameFactory);
        BTree btree = new BTree(bufferCache, fileMapProvider, freePageManager, interiorFrameFactory, leafFrameFactory,
                cmpFactories, typeTraits.length, file);
        return btree;
    }

    public static BTree createBTree(IBufferCache bufferCache, IFreePageManager freePageManager,
            IFileMapProvider fileMapProvider, ITypeTraits[] typeTraits, IBinaryComparatorFactory[] cmpFactories,
            BTreeLeafFrameType leafType, FileReference file) throws BTreeException {
        TypeAwareTupleWriterFactory tupleWriterFactory = new TypeAwareTupleWriterFactory(typeTraits);
        ITreeIndexFrameFactory leafFrameFactory = getLeafFrameFactory(tupleWriterFactory, leafType);
        ITreeIndexFrameFactory interiorFrameFactory = new BTreeNSMInteriorFrameFactory(tupleWriterFactory);
        BTree btree = new BTree(bufferCache, fileMapProvider, freePageManager, interiorFrameFactory, leafFrameFactory,
                cmpFactories, typeTraits.length, file);
        return btree;
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

    public static ITreeIndexFrameFactory getLeafFrameFactory(ITreeIndexTupleWriterFactory tupleWriterFactory,
            BTreeLeafFrameType leafType) throws BTreeException {
        switch (leafType) {
            case REGULAR_NSM: {
                return new BTreeNSMLeafFrameFactory(tupleWriterFactory);
            }
            case FIELD_PREFIX_COMPRESSED_NSM: {
                return new BTreeFieldPrefixNSMLeafFrameFactory(tupleWriterFactory);
            }
            default: {
                throw new BTreeException("Unknown BTreeLeafFrameType: " + leafType.toString());
            }
        }
    }
}
