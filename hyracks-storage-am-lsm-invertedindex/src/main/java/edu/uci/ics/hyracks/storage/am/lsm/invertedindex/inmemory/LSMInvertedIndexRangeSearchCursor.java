package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.inmemory;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicInteger;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.util.TupleUtils;
import edu.uci.ics.hyracks.storage.am.common.api.ICursorInitialState;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMHarness;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk.InvertedIndexAccessor;

public class LSMInvertedIndexRangeSearchCursor implements IIndexCursor {

    private LSMHarness harness;
    private boolean includeMemComponent;
    private AtomicInteger searcherRefCount;
    private List<IIndexAccessor> indexAccessors;
    private List<IIndexCursor> indexCursors;
    private boolean flagEOF = false;
    private boolean flagFirstNextCall = true;

    private PriorityQueue<PriorityQueueElement> outputPriorityQueue;
    private MultiComparator memoryInvertedIndexComparator;
    private PriorityQueueComparator pqCmp;
    private int tokenFieldCount;
    private int invListFieldCount;
    private ITupleReference resultTuple;
    private BitSet closedCursors;

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        LSMInvertedIndexCursorInitialState lsmInitialState = (LSMInvertedIndexCursorInitialState) initialState;
        this.harness = lsmInitialState.getLSMHarness();
        this.includeMemComponent = lsmInitialState.getIncludeMemComponent();
        this.searcherRefCount = lsmInitialState.getSearcherRefCount();
        this.indexAccessors = lsmInitialState.getIndexAccessors();
        this.indexCursors = new ArrayList<IIndexCursor>(indexAccessors.size());
        LSMInvertedIndexOpContext opContext = (LSMInvertedIndexOpContext) lsmInitialState.getOpContext();
        this.memoryInvertedIndexComparator = opContext.getComparator();
        this.tokenFieldCount = opContext.getTokenFieldCount();
        this.invListFieldCount = opContext.getInvListFieldCount();
        closedCursors = new BitSet(indexAccessors.size());

        //create all cursors
        IIndexCursor cursor;
        for (IIndexAccessor accessor : indexAccessors) {
            InvertedIndexAccessor invIndexAccessor = (InvertedIndexAccessor) accessor;
            cursor = invIndexAccessor.createRangeSearchCursor();
            try {
                invIndexAccessor.rangeSearch(cursor, searchPred);
            } catch (IndexException e) {
                throw new HyracksDataException(e);
            }
            indexCursors.add(cursor);
        }
    }

    @Override
    public boolean hasNext() throws HyracksDataException {

        if (flagEOF) {
            return false;
        }

        if (flagFirstNextCall) {
            for (IIndexCursor c : indexCursors) {
                if (c.hasNext()) {
                    return true;
                }
            }
        } else {
            if (outputPriorityQueue.size() > 0) {
                return true;
            }
        }

        flagEOF = true;
        return false;
    }

    @Override
    public void next() throws HyracksDataException {

        PriorityQueueElement pqElement;
        IIndexCursor cursor;
        int cursorIndex;

        if (flagEOF) {
            return;
        }

        //When the next() is called for the first time, initialize priority queue.
        if (flagFirstNextCall) {
            flagFirstNextCall = false;

            //create and initialize PriorityQueue
            pqCmp = new PriorityQueueComparator(memoryInvertedIndexComparator);
            outputPriorityQueue = new PriorityQueue<PriorityQueueElement>(indexCursors.size(), pqCmp);

            //read the first tuple from each cursor and insert into outputPriorityQueue

            for (int i = 0; i < indexCursors.size(); i++) {
                cursor = indexCursors.get(i);
                if (cursor.hasNext()) {
                    cursor.next();
                    pqElement = new PriorityQueueElement(cursor.getTuple(), i);
                    outputPriorityQueue.offer(pqElement);
                }
                //else {
                //    //do nothing for the cursor who reached EOF.
                //}
            }
        }

        //If you reach here, priority queue is set up to provide the smallest <tokenFields, invListFields>
        //Get the smallest element from priority queue. 
        //This element will be the result tuple which will be served to the caller when getTuple() is called. 
        //Then, insert new element from the cursor where the smallest element came from.
        pqElement = outputPriorityQueue.poll();
        if (pqElement != null) {
            resultTuple = pqElement.getTuple();
            cursorIndex = pqElement.getCursorIndex();
            cursor = indexCursors.get(cursorIndex);
            if (cursor.hasNext()) {
                cursor.next();
                pqElement = new PriorityQueueElement(cursor.getTuple(), cursorIndex);
                outputPriorityQueue.offer(pqElement);
            } else {
                cursor.close();
                closedCursors.set(cursorIndex, true);

//                 If the current cursor reached EOF, read a tuple from another cursor and insert into the priority queue.
                for (int i = 0; i < indexCursors.size(); i++) {
                    if (closedCursors.get(i))
                        continue;

                    cursor = indexCursors.get(i);
                    if (cursor.hasNext()) {
                        cursor.next();
                        pqElement = new PriorityQueueElement(cursor.getTuple(), i);
                        outputPriorityQueue.offer(pqElement);
                        break;
                    } else {
                        cursor.close();
                        closedCursors.set(i, true);
                    }
                }
                //if (i == indexCursors.size()) {
                //    all cursors reached EOF and the only tuples that you have are in the priority queue.
                //    do nothing here!.
                //}
            }
        }
        //else {
        // do nothing!!
        // which means when the getTuple() is called, the pre-existing result tuple or null will be returned to the caller.
        //}

    }

    @Override
    public void close() throws HyracksDataException {
        try {
            for (int i = 0; i < indexCursors.size(); i++) {
                if (closedCursors.get(i)) {
                    continue;
                }
                indexCursors.get(i).close();
                closedCursors.set(i, true);
            }
        } finally {
            harness.closeSearchCursor(searcherRefCount, includeMemComponent);
        }
    }

    @Override
    public void reset() {
        // TODO Auto-generated method stub
    }

    @Override
    public ITupleReference getTuple() {
        return resultTuple;
    }

    public class PriorityQueueComparator implements Comparator<PriorityQueueElement> {

        private final MultiComparator cmp;

        public PriorityQueueComparator(MultiComparator cmp) {
            this.cmp = cmp;
        }

        @Override
        public int compare(PriorityQueueElement elementA, PriorityQueueElement elementB) {
            int result = cmp.compare(elementA.getTuple(), elementB.getTuple());
            if (result != 0) {
                return result;
            }
            if (elementA.getCursorIndex() > elementB.getCursorIndex()) {
                return 1;
            } else {
                return -1;
            }
        }

        public MultiComparator getMultiComparator() {
            return cmp;
        }
    }

    public class PriorityQueueElement {
        private ITupleReference tuple;
        private int cursorIndex;

        public PriorityQueueElement(ITupleReference tuple, int cursorIndex) {
//            reset(tuple, cursorIndex);
            try {
                reset(TupleUtils.copyTuple(tuple), cursorIndex);
            } catch (HyracksDataException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        public ITupleReference getTuple() {
            return tuple;
        }

        public int getCursorIndex() {
            return cursorIndex;
        }

        public void reset(ITupleReference tuple, int cursorIndex) {
            this.tuple = tuple;
            this.cursorIndex = cursorIndex;
        }
    }

}
