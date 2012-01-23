package edu.uci.ics.hyracks.storage.am.lsmtree.impls;

import java.util.PriorityQueue;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ICursorInitialState;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;

public class LSMTreeRangeSearchCursor implements ITreeIndexCursor {

    private BTreeRangeSearchCursor[] rangeCursors;
    private PriorityQueue<LSMPriorityQueueElement> outputPriorityQueue;
    private MultiComparator cmp;
    private LSMPriorityQueueElement outputElement;
    private LSMPriorityQueueElement reusedElement;
    private int numberOfTrees;
    private boolean needPush;
    private LSMTree lsm;

    public LSMTreeRangeSearchCursor() {
        outputElement = null;
        needPush = false;
    }

    public void initPriorityQueue(int numberOfTrees, LSMPriorityQueueComparator LSMPriorityQueueCmp) throws Exception {
        this.numberOfTrees = numberOfTrees;
        outputPriorityQueue = new PriorityQueue<LSMPriorityQueueElement>(numberOfTrees, LSMPriorityQueueCmp);
        for (int i = 0; i < numberOfTrees; i++) {
            LSMPriorityQueueElement element;
            if (rangeCursors[i].hasNext()) {
                rangeCursors[i].next();
                element = new LSMPriorityQueueElement(rangeCursors[i].getTuple(), i);
                outputPriorityQueue.offer(element);
            }
        }
        checkPriorityQueue();
    }

    public BTreeRangeSearchCursor getCursor(int cursorIndex) {
        return rangeCursors[cursorIndex];
    }

    @Override
    public void reset() {
        // do nothing
    }

    @Override
    public boolean hasNext() throws Exception {
        checkPriorityQueue();
        return !outputPriorityQueue.isEmpty();
    }

    @Override
    public void next() throws Exception {

        outputElement = outputPriorityQueue.poll();
        needPush = true;
        if (outputElement == null) {
            throw new Exception("The outputPriorityQueue is empty");
        }
    }

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {

        lsm = ((LSMTreeCursorInitialState) initialState).getLsm();
        cmp = ((LSMTreeCursorInitialState) initialState).getCmp();

        rangeCursors = new BTreeRangeSearchCursor[((LSMTreeCursorInitialState) initialState).getNumberOfTrees()];

        for (int i = 0; i < ((LSMTreeCursorInitialState) initialState).getNumberOfTrees(); i++) {
            rangeCursors[i] = new BTreeRangeSearchCursor((IBTreeLeafFrame) ((LSMTreeCursorInitialState) initialState)
                    .getLeafFrameFactory().createFrame(), false);
        }
    }

    @Override
    public ICachedPage getPage() {
        // do nothing
        return null;
    }

    @Override
    public void close() throws Exception {
        lsm.decreaseThreadReferenceCounter();
        outputPriorityQueue.clear();
        for (int i = 0; i < numberOfTrees; i++) {
            rangeCursors[i].close();
        }
        rangeCursors = null;
    }

    @Override
    public void setBufferCache(IBufferCache bufferCache) {
        // do nothing
    }

    @Override
    public void setFileId(int fileId) {
        // do nothing
    }

    @Override
    public ITupleReference getTuple() {
        return (ITupleReference) outputElement.getTuple();
    }

    private void pushIntoPriorityQueue(int cursorIndex) throws Exception {

        if (rangeCursors[cursorIndex].hasNext()) {
            rangeCursors[cursorIndex].next();
            reusedElement.reset(rangeCursors[cursorIndex].getTuple(), cursorIndex);
            outputPriorityQueue.offer(reusedElement);
        }
    }

    private void checkPriorityQueue() throws Exception {

        while (!outputPriorityQueue.isEmpty() || needPush == true) {
            if (!outputPriorityQueue.isEmpty()) {
                LSMPriorityQueueElement checkElement = outputPriorityQueue.peek();
                // If there is no previous tuple or the previous tuple can be
                // ignored
                if (outputElement == null) {
                    // Test the tuple is a delete tuple or not
                    if (((LSMTypeAwareTupleReference) checkElement.getTuple()).isDelete() == true) {
                        // If the tuple is a delete tuple then pop it and mark
                        // it "needPush"
                        // Cannot push at this time because the tuple may be
                        // modified if "hasNext" is called
                        outputElement = outputPriorityQueue.poll();
                        needPush = true;
                    } else {
                        break;
                    }
                } else {
                    // Compare the previous tuple and the head tuple in the PQ
                    if (cmp.compare(outputElement.getTuple(), checkElement.getTuple()) == 0) {
                        // If the previous tuple and the head tuple are
                        // identical
                        // then pop the head tuple and push the next tuple from
                        // the tree of head tuple

                        // the head element of PQ is useless now
                        reusedElement = outputPriorityQueue.poll();
                        // int treeNum = reusedElement.getTreeNum();
                        pushIntoPriorityQueue(reusedElement.getCursorIndex());
                    } else {
                        // If the previous tuple and the head tuple are
                        // different
                        // the info of previous tuple is useless
                        if (needPush == true) {
                            reusedElement = outputElement;
                            pushIntoPriorityQueue(outputElement.getCursorIndex());
                            needPush = false;
                        }
                        outputElement = null;
                    }
                }
            } else {
                // the priority queue is empty and needPush
                reusedElement = outputElement;
                pushIntoPriorityQueue(outputElement.getCursorIndex());
                needPush = false;
                outputElement = null;
            }
        }
    }

    @Override
    public boolean exclusiveLatchNodes() {
        return false;
    }
}
