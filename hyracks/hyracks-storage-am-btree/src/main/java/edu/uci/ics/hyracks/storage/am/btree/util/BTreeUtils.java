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
package edu.uci.ics.hyracks.storage.am.btree.util;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeException;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeFieldPrefixNSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriterFactory;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.freepage.LinkedListFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

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
            return MultiComparator.createIgnoreFieldLength(cmpFactories);
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
