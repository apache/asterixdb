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

package edu.uci.ics.hyracks.storage.am.lsm.btree.util;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.dataflow.common.util.SerdeUtils;
import edu.uci.ics.hyracks.storage.am.btree.tests.CheckTuple;
import edu.uci.ics.hyracks.storage.am.btree.tests.OrderedIndexTestContext;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.am.lsm.impls.LSMTree;
import edu.uci.ics.hyracks.storage.am.lsm.util.LSMBTreeUtils;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

@SuppressWarnings("rawtypes")
public final class LSMBTreeTestContext extends OrderedIndexTestContext {    
    
    public LSMBTreeTestContext(ISerializerDeserializer[] fieldSerdes, ITreeIndex treeIndex) {
        super(fieldSerdes, treeIndex);
    }

    @Override
    public int getKeyFieldCount() {
        LSMTree lsmTree = (LSMTree) treeIndex;
        return lsmTree.getMultiComparator().getKeyFieldCount();
    }

    @Override
    public IBinaryComparator[] getComparators() {
        LSMTree lsmTree = (LSMTree) treeIndex;
        return lsmTree.getMultiComparator().getComparators();
    }

    /**
     * Override to provide upsert semantics for the check tuples.
     */
    @Override
    public void insertIntCheckTuple(int[] fieldValues) {        
        CheckTuple<Integer> checkTuple = new CheckTuple<Integer>(getFieldCount(), getKeyFieldCount());
        for(int v : fieldValues) {
            checkTuple.add(v);
        }
        if (checkTuples.contains(checkTuple)) {
            checkTuples.remove(checkTuple);
        }
        checkTuples.add(checkTuple);
    }

    /**
     * Override to provide upsert semantics for the check tuples.
     */
    @Override
    public void insertStringCheckTuple(String[] fieldValues) {
        CheckTuple<String> checkTuple = new CheckTuple<String>(getFieldCount(), getKeyFieldCount());
        for(String s : fieldValues) {
            checkTuple.add((String)s);
        }
        if (checkTuples.contains(checkTuple)) {
            checkTuples.remove(checkTuple);
        }
        checkTuples.add(checkTuple);
    }
    
    public static LSMBTreeTestContext create(InMemoryBufferCache memBufferCache,
            InMemoryFreePageManager memFreePageManager, String onDiskDir, IBufferCache diskBufferCache,
            IFileMapProvider diskFileMapProvider, ISerializerDeserializer[] fieldSerdes, int numKeyFields, int fileId)
            throws Exception {
        ITypeTraits[] typeTraits = SerdeUtils.serdesToTypeTraits(fieldSerdes);
        IBinaryComparator[] cmps = SerdeUtils.serdesToComparators(fieldSerdes, numKeyFields);
        LSMTree lsmTree = LSMBTreeUtils.createLSMTree(memBufferCache, memFreePageManager, onDiskDir, diskBufferCache,
                diskFileMapProvider, typeTraits, cmps);
        lsmTree.create(fileId);
        lsmTree.open(fileId);
        LSMBTreeTestContext testCtx = new LSMBTreeTestContext(fieldSerdes, lsmTree);
        return testCtx;
    }
}
