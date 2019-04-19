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
package org.apache.hyracks.storage.am.lsm.invertedindex.inmemory;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.PermutingTupleReference;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.btree.impls.BTree.BTreeAccessor;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.tuples.ConcatenatingTupleReference;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.InvertedListCursor;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.MultiComparator;

public class InMemoryInvertedListCursor extends InvertedListCursor {
    private RangePredicate btreePred;
    private BTreeAccessor btreeAccessor;
    private IIndexCursor btreeCursor;
    private boolean cursorNeedsClose = false;
    private IIndexCursor countingCursor;
    private MultiComparator tokenFieldsCmp;
    private MultiComparator btreeCmp;
    private final PermutingTupleReference resultTuple;
    private final ConcatenatingTupleReference btreeSearchTuple;

    private final ArrayTupleBuilder tokenTupleBuilder;
    private final ArrayTupleReference tokenTuple = new ArrayTupleReference();

    private int numElements = -1;

    public InMemoryInvertedListCursor(int invListFieldCount, int tokenFieldCount) {
        int[] fieldPermutation = new int[invListFieldCount];
        for (int i = 0; i < invListFieldCount; i++) {
            fieldPermutation[i] = tokenFieldCount + i;
        }
        resultTuple = new PermutingTupleReference(fieldPermutation);
        // Concatenating the tuple with tokens, and the tuple with inverted-list elements.
        btreeSearchTuple = new ConcatenatingTupleReference(2);
        tokenTupleBuilder = new ArrayTupleBuilder(tokenFieldCount);
    }

    public void prepare(BTreeAccessor btreeAccessor, RangePredicate btreePred, MultiComparator tokenFieldsCmp,
            MultiComparator btreeCmp) throws HyracksDataException {
        // Avoid object creation if this.btreeAccessor == btreeAccessor.
        if (this.btreeAccessor != btreeAccessor) {
            this.btreeAccessor = btreeAccessor;
            this.btreeCursor = btreeAccessor.createSearchCursor(false);
            this.countingCursor = btreeAccessor.createCountingSearchCursor();
            this.btreePred = btreePred;
            this.btreePred.setLowKeyComparator(tokenFieldsCmp);
            this.btreePred.setHighKeyComparator(tokenFieldsCmp);
            this.tokenFieldsCmp = tokenFieldsCmp;
            this.btreeCmp = btreeCmp;
        }
    }

    @Override
    public int compareTo(InvertedListCursor cursor) {
        try {
            return size() - cursor.size();
        } catch (HyracksDataException hde) {
            throw new IllegalStateException(hde);
        }
    }

    public void reset(ITupleReference tuple) throws HyracksDataException {
        numElements = -1;
        // Copy the tokens tuple for later use in btree probes.
        TupleUtils.copyTuple(tokenTupleBuilder, tuple, tuple.getFieldCount());
        tokenTuple.reset(tokenTupleBuilder.getFieldEndOffsets(), tokenTupleBuilder.getByteArray());
        btreeSearchTuple.reset();
        btreeSearchTuple.addTuple(tokenTuple);
        btreeCursor.close();
        countingCursor.close();
    }

    @Override
    protected void setInvListInfo(int startPageId, int endPageId, int startOff, int numElements)
            throws HyracksDataException {
        // no-op for this in-memory cursor - everything is in memory
    }

    @Override
    public void loadPages() throws HyracksDataException {
        btreePred.setLowKeyComparator(tokenFieldsCmp);
        btreePred.setHighKeyComparator(tokenFieldsCmp);
        btreePred.setLowKey(tokenTuple, true);
        btreePred.setHighKey(tokenTuple, true);
        btreeAccessor.search(btreeCursor, btreePred);
        cursorNeedsClose = true;
    }

    @Override
    public void unloadPages() throws HyracksDataException {
        if (cursorNeedsClose) {
            btreeCursor.close();
            cursorNeedsClose = false;
        }
    }

    @Override
    public boolean doHasNext() throws HyracksDataException {
        return btreeCursor.hasNext();
    }

    @Override
    public void doNext() throws HyracksDataException {
        btreeCursor.next();
    }

    @Override
    public ITupleReference doGetTuple() {
        resultTuple.reset(btreeCursor.getTuple());
        return resultTuple;
    }

    @Override
    public int size() throws HyracksDataException {
        if (numElements < 0) {
            btreePred.setLowKeyComparator(tokenFieldsCmp);
            btreePred.setHighKeyComparator(tokenFieldsCmp);
            btreePred.setLowKey(tokenTuple, true);
            btreePred.setHighKey(tokenTuple, true);
            // Perform the count.
            btreeAccessor.search(countingCursor, btreePred);
            try {
                while (countingCursor.hasNext()) {
                    countingCursor.next();
                    ITupleReference countTuple = countingCursor.getTuple();
                    numElements = IntegerPointable.getInteger(countTuple.getFieldData(0), countTuple.getFieldStart(0));
                }
            } finally {
                countingCursor.close();
            }
        }
        return numElements;
    }

    @Override
    public boolean containsKey(ITupleReference searchTuple, MultiComparator invListCmp) throws HyracksDataException {
        // Close cursor if necessary.
        unloadPages();
        btreeSearchTuple.addTuple(searchTuple);
        btreePred.setLowKeyComparator(btreeCmp);
        btreePred.setHighKeyComparator(btreeCmp);
        btreePred.setLowKey(btreeSearchTuple, true);
        btreePred.setHighKey(btreeSearchTuple, true);
        try {
            btreeAccessor.search(btreeCursor, btreePred);
            cursorNeedsClose = true;
        } catch (Exception e) {
            btreeSearchTuple.removeLastTuple();
            throw HyracksDataException.create(e);
        }
        boolean containsKey = false;
        try {
            containsKey = btreeCursor.hasNext();
        } finally {
            btreeCursor.close();
            btreeSearchTuple.removeLastTuple();
        }
        return containsKey;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public String printInvList(ISerializerDeserializer[] serdes) throws HyracksDataException {
        StringBuilder strBuilder = new StringBuilder();
        try {
            while (btreeCursor.hasNext()) {
                btreeCursor.next();
                ITupleReference tuple = btreeCursor.getTuple();
                ByteArrayInputStream inStream = new ByteArrayInputStream(tuple.getFieldData(1), tuple.getFieldStart(1),
                        tuple.getFieldLength(1));
                DataInput dataIn = new DataInputStream(inStream);
                Object o = serdes[0].deserialize(dataIn);
                strBuilder.append(o.toString() + " ");
            }
        } finally {
            btreeCursor.close();
        }
        btreeAccessor.search(btreeCursor, btreePred);
        return strBuilder.toString();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public String printCurrentElement(ISerializerDeserializer[] serdes) throws HyracksDataException {
        return null;
    }

    @Override
    public void prepareLoadPages() throws HyracksDataException {
        // no-op for this in-memory cursor - no need to initialize a buffer
    }

    @Override
    public void doClose() throws HyracksDataException {
        btreeCursor.close();
    }

    @Override
    public void doDestroy() throws HyracksDataException {
        btreeCursor.destroy();
    }

}
