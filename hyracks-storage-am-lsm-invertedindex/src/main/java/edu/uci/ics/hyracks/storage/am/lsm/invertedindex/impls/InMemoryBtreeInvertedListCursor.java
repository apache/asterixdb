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
package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.util.TupleUtils;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree.BTreeAccessor;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.dataflow.ConcatenatingTupleReference;
import edu.uci.ics.hyracks.storage.am.common.dataflow.PermutingTupleReference;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedListCursor;

public class InMemoryBtreeInvertedListCursor implements IInvertedListCursor {
    private RangePredicate btreePred;
    private BTreeAccessor btreeAccessor;
    private IIndexCursor btreeCursor;
    private IIndexCursor countingCursor;
    private MultiComparator tokenFieldsCmp;
    private MultiComparator btreeCmp;
    private final PermutingTupleReference resultTuple;
    private final ConcatenatingTupleReference btreeSearchTuple;
    
    private final ArrayTupleBuilder tokenTupleBuilder;
    private final ArrayTupleReference tokenTuple = new ArrayTupleReference();
    
    private int numElements = -1;
    
    public InMemoryBtreeInvertedListCursor(int invListFieldCount, int tokenFieldCount) {
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
            MultiComparator btreeCmp) {
        this.btreeAccessor = btreeAccessor;
        this.btreeCursor = btreeAccessor.createSearchCursor();
        this.countingCursor = btreeAccessor.createCountingSearchCursor();
        this.btreePred = btreePred;
        this.btreePred.setLowKeyComparator(tokenFieldsCmp);
        this.btreePred.setHighKeyComparator(tokenFieldsCmp);
        this.tokenFieldsCmp = tokenFieldsCmp;
        this.btreeCmp = btreeCmp;
    }
    
    @Override
    public int compareTo(IInvertedListCursor cursor) {
        return getNumElements() - cursor.getNumElements();
    }

    public void reset(ITupleReference tuple) throws HyracksDataException, TreeIndexException {
        numElements = -1;
        // Copy the tokens tuple for later use in btree probes.
        TupleUtils.copyTuple(tokenTupleBuilder, tuple, tuple.getFieldCount());
        tokenTuple.reset(tokenTupleBuilder.getFieldEndOffsets(), tokenTupleBuilder.getByteArray());
        btreeSearchTuple.addTuple(tokenTuple);
        btreePred.setLowKey(tuple, true);
        btreePred.setHighKey(tuple, true);
        btreeAccessor.search(btreeCursor, btreePred);
    }

    @Override
    public void reset(int startPageId, int endPageId, int startOff, int numElements) {
        // Do nothing
    }

    @Override
    public void pinPagesSync() throws HyracksDataException {
        // Do nothing
    }

    @Override
    public void pinPagesAsync() throws HyracksDataException {
        // Do nothing
    }

    @Override
    public void unpinPages() throws HyracksDataException {
        btreeCursor.close();
    }

    @Override
    public boolean hasNext() throws HyracksDataException {
        return btreeCursor.hasNext();
    }

    @Override
    public void next() throws HyracksDataException {
        btreeCursor.next();
    }

    @Override
    public ITupleReference getTuple() {
        resultTuple.reset(btreeCursor.getTuple());
        return resultTuple;
    }

    @Override
    public int getNumElements() {
        if (numElements < 0) {
            btreePred.setLowKeyComparator(tokenFieldsCmp);
            btreePred.setHighKeyComparator(tokenFieldsCmp);
            btreePred.setLowKey(tokenTuple, true);
            btreePred.setHighKey(tokenTuple, true);
            
            // Perform the count.
            try {
                btreeAccessor.search(countingCursor, btreePred);
                while (countingCursor.hasNext()) {
                    countingCursor.next();
                    ITupleReference countTuple = countingCursor.getTuple();
                    numElements = IntegerSerializerDeserializer.getInt(countTuple.getFieldData(0), countTuple.getFieldStart(0));
                }
            } catch (HyracksDataException e) {
                e.printStackTrace();
            } catch (IndexException e) {
                e.printStackTrace();
            } finally {
                try {
                    countingCursor.close();
                } catch (HyracksDataException e) {
                    e.printStackTrace();
                }
            }
        }
        return numElements;
    }

    @Override
    public int getStartPageId() {
        return 0;
    }

    @Override
    public int getEndPageId() {
        return 0;
    }

    @Override
    public int getStartOff() {
        return 0;
    }

    @Override
    public boolean containsKey(ITupleReference searchTuple, MultiComparator invListCmp) throws HyracksDataException {
        btreeSearchTuple.addTuple(searchTuple);
        btreePred.setLowKeyComparator(btreeCmp);
        btreePred.setHighKeyComparator(btreeCmp);
        btreePred.setLowKey(btreeSearchTuple, true);
        btreePred.setHighKey(btreeSearchTuple, true);
        try {
            btreeAccessor.search(btreeCursor, btreePred);
        } catch (TreeIndexException e) {
            btreeSearchTuple.removeLastTuple();
            throw new HyracksDataException(e);
        }
        boolean containsKey = false;
        try {
            containsKey = btreeCursor.hasNext();
        } finally {
            btreeCursor.close();
            btreeCursor.reset();
            btreeSearchTuple.removeLastTuple();
        }        
        return containsKey;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public String printInvList(ISerializerDeserializer[] serdes) throws HyracksDataException {
        return null;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public String printCurrentElement(ISerializerDeserializer[] serdes) throws HyracksDataException {
        return null;
    }
}
