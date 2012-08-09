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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.util.TupleUtils;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeCountingSearchCursor;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.dataflow.PermutingTupleReference;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedListCursor;

public class InMemoryBtreeInvertedListCursor implements IInvertedListCursor {
    private final BTree btree;
    private final RangePredicate btreePred;
    private final IBTreeLeafFrame leafFrame;
    private final ITreeIndexAccessor btreeAccessor;
    private BTreeRangeSearchCursor btreeCursor;
    private MultiComparator tokenFieldsCmp;

    private int numElements = -1;
    private ITupleReference tokenTuple;

    public InMemoryBtreeInvertedListCursor(BTree btree, ITypeTraits[] invListFields) {
        this.btree = btree;
        this.btreeAccessor = btree.createAccessor();
        this.btreePred = new RangePredicate(null, null, true, true, null, null);
        this.leafFrame = (IBTreeLeafFrame) btree.getLeafFrameFactory().createFrame();
        this.btreeCursor = new BTreeRangeSearchCursor(leafFrame, false);
        setTokenFieldComparators(btree.getComparatorFactories(), btree.getComparatorFactories().length
                - invListFields.length);
        btreePred.setLowKeyComparator(tokenFieldsCmp);
        btreePred.setHighKeyComparator(tokenFieldsCmp);
    }

    private void setTokenFieldComparators(IBinaryComparatorFactory[] cmpFactories, int numTokenFields) {
        IBinaryComparatorFactory[] keyFieldCmpFactories = new IBinaryComparatorFactory[numTokenFields];
        System.arraycopy(cmpFactories, 0, keyFieldCmpFactories, 0, numTokenFields);
        tokenFieldsCmp = MultiComparator.create(keyFieldCmpFactories);
    }

    @Override
    public int compareTo(IInvertedListCursor cursor) {
        return getNumElements() - cursor.getNumElements();
    }

    public void reset(ITupleReference tuple) throws HyracksDataException, IndexException {
        numElements = -1;
        tokenTuple = TupleUtils.copyTuple(tuple);
        btreeCursor = (BTreeRangeSearchCursor) btreeAccessor.createSearchCursor();
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

    public ITupleReference getTuple() throws HyracksDataException {
        PermutingTupleReference projectedTuple = new PermutingTupleReference();
        ITupleReference tuple = btreeCursor.getTuple();
        int tupleFieldCount = tuple.getFieldCount();
        int tokensFieldCount = tokenFieldsCmp.getKeyFieldCount();

        int[] fEndOffsets = new int[tupleFieldCount];
        int[] fieldPermutation = new int[tupleFieldCount - tokensFieldCount];

        for (int i = 0; i < tupleFieldCount; i++) {
            fEndOffsets[i] = tuple.getFieldStart(i) + tuple.getFieldLength(i);
        }

        for (int i = 0; i < fieldPermutation.length; i++) {
            fieldPermutation[i] = tokensFieldCount + i;
        }

        projectedTuple.reset(fEndOffsets, fieldPermutation, tuple.getFieldData(0));

        return projectedTuple;
    }

    @Override
    public int getNumElements() {
        if (numElements < 0) {
            // perform the count
            IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) btree.getLeafFrameFactory().createFrame();
            BTreeCountingSearchCursor countCursor = new BTreeCountingSearchCursor(leafFrame, false);
            RangePredicate predicate = new RangePredicate(tokenTuple, tokenTuple, true, true, tokenFieldsCmp,
                    tokenFieldsCmp);
            try {
                btreeAccessor.search(countCursor, predicate);
                while (countCursor.hasNext()) {
                    countCursor.next();
                    ITupleReference countTuple = countCursor.getTuple();
                    ByteArrayInputStream bais = new ByteArrayInputStream(countTuple.getFieldData(0));
                    DataInputStream dis = new DataInputStream(bais);
                    numElements = IntegerSerializerDeserializer.INSTANCE.deserialize(dis).intValue();
                }
                countCursor.close();
            } catch (HyracksDataException e) {
                e.printStackTrace();
            } catch (IndexException e) {
                e.printStackTrace();
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
    public boolean containsKey(ITupleReference searchTuple, MultiComparator invListCmp) throws HyracksDataException,
            IndexException {
        int numCompositeFields = tokenTuple.getFieldCount() + invListCmp.getKeyFieldCount();
        ArrayTupleBuilder tb = new ArrayTupleBuilder(numCompositeFields);
        ArrayTupleReference compositeTuple = new ArrayTupleReference();
        for (int i = 0; i < tokenTuple.getFieldCount(); i++) {
            tb.addField(tokenTuple.getFieldData(i), tokenTuple.getFieldStart(i), tokenTuple.getFieldLength(i));
        }
        for (int i = 0; i < invListCmp.getKeyFieldCount(); i++) {
            tb.addField(searchTuple.getFieldData(i), searchTuple.getFieldStart(i), searchTuple.getFieldLength(i));
        }
        compositeTuple.reset(tb.getFieldEndOffsets(), tb.getByteArray());

        MultiComparator cmp = MultiComparator.create(btree.getComparatorFactories());
        RangePredicate predicate = new RangePredicate(compositeTuple, compositeTuple, true, true, cmp, cmp);
        BTreeRangeSearchCursor cursor = (BTreeRangeSearchCursor) btreeAccessor.createSearchCursor();
        btreeAccessor.search(cursor, predicate);

        boolean containsKey = cursor.hasNext();
        cursor.close();

        return containsKey;
    }

    @Override
    public String printInvList(ISerializerDeserializer[] serdes) throws HyracksDataException {
        return null;
    }

    @Override
    public String printCurrentElement(ISerializerDeserializer[] serdes) throws HyracksDataException {
        return null;
    }

}
