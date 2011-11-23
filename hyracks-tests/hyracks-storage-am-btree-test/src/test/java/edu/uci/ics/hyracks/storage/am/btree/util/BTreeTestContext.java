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

package edu.uci.ics.hyracks.storage.am.btree.util;

import java.util.TreeSet;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;

@SuppressWarnings("rawtypes")
public final class BTreeTestContext {
    public final ISerializerDeserializer[] fieldSerdes;
    public final IBufferCache bufferCache;
    public final BTree btree;
    public final IBTreeLeafFrame leafFrame;
    public final IBTreeInteriorFrame interiorFrame;
    public final ITreeIndexMetaDataFrame metaFrame;
    public final ArrayTupleBuilder tupleBuilder;
    public final ArrayTupleReference tuple = new ArrayTupleReference();
    public final TreeSet<CheckTuple> checkTuples = new TreeSet<CheckTuple>();
    public final ITreeIndexAccessor indexAccessor;

    public BTreeTestContext(IBufferCache bufferCache, ISerializerDeserializer[] fieldSerdes, BTree btree,
            IBTreeLeafFrame leafFrame, IBTreeInteriorFrame interiorFrame, ITreeIndexMetaDataFrame metaFrame,
            ITreeIndexAccessor indexAccessor) {
        this.bufferCache = bufferCache;
        this.fieldSerdes = fieldSerdes;
        this.btree = btree;
        this.leafFrame = leafFrame;
        this.interiorFrame = interiorFrame;
        this.metaFrame = metaFrame;
        this.indexAccessor = indexAccessor;
        this.tupleBuilder = new ArrayTupleBuilder(fieldSerdes.length);
    }

    public int getFieldCount() {
        return fieldSerdes.length;
    }

    public int getKeyFieldCount() {
        return btree.getMultiComparator().getKeyFieldCount();
    }
}