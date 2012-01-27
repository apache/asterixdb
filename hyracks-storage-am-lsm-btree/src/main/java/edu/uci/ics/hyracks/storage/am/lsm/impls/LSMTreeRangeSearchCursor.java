package edu.uci.ics.hyracks.storage.am.lsm.impls;

import java.util.PriorityQueue;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ICursorInitialState;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.tuples.LSMTypeAwareTupleReference;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;

public class LSMTreeRangeSearchCursor implements ITreeIndexCursor {

    private BTreeRangeSearchCursor[] rangeCursors;
    private PriorityQueue<LSMPriorityQueueElement> outputPriorityQueue;
    private MultiComparator cmp;
    private LSMPriorityQueueElement outputElement;
    private LSMPriorityQueueElement reusedElement;
    private boolean needPush;
    private LSMTree lsmTree;

    public LSMTreeRangeSearchCursor() {
        outputElement = null;
        needPush = false;
    }

    public void initPriorityQueue(LSMPriorityQueueComparator LSMPriorityQueueCmp)
            throws HyracksDataException {
        outputPriorityQueue = new PriorityQueue<LSMPriorityQueueElement>(rangeCursors.length, LSMPriorityQueueCmp);
        for (int i = 0; i < rangeCursors.length; i++) {
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
        outputElement = null;
        needPush = false;
    }

    @Override
    public boolean hasNext() throws HyracksDataException {
        checkPriorityQueue();
        return !outputPriorityQueue.isEmpty();
    }

    @Override
    public void next() throws HyracksDataException {
        outputElement = outputPriorityQueue.poll();
        needPush = true;
        if (outputElement == null) {
            throw new HyracksDataException("The outputPriorityQueue is empty");
        }
    }

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        LSMTreeCursorInitialState lsmInitialState = (LSMTreeCursorInitialState) initialState;
        lsmTree = lsmInitialState.getLsm();
        cmp = lsmInitialState.getCmp();
        int numBTrees = lsmInitialState.getNumBTrees();
        rangeCursors = new BTreeRangeSearchCursor[numBTrees];
        for (int i = 0; i < numBTrees; i++) {
            IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) lsmInitialState.getLeafFrameFactory().createFrame();
            rangeCursors[i] = new BTreeRangeSearchCursor(leafFrame, false);
        }
    }

    @Override
    public ICachedPage getPage() {
        // do nothing
        return null;
    }

    @Override
    public void close() throws HyracksDataException {
        outputPriorityQueue.clear();
        for (int i = 0; i < rangeCursors.length; i++) {
            rangeCursors[i].close();
        }
        rangeCursors = null;
        try {
            lsmTree.threadExit();
        } catch (TreeIndexException e) {
            throw new HyracksDataException(e);
        }
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

    private void pushIntoPriorityQueue(int cursorIndex) throws HyracksDataException {
        if (rangeCursors[cursorIndex].hasNext()) {
            rangeCursors[cursorIndex].next();
            reusedElement.reset(rangeCursors[cursorIndex].getTuple(), cursorIndex);
            outputPriorityQueue.offer(reusedElement);
        }
    }

    private void checkPriorityQueue() throws HyracksDataException {
        while (!outputPriorityQueue.isEmpty() || needPush == true) {
            if (!outputPriorityQueue.isEmpty()) {
                LSMPriorityQueueElement checkElement = outputPriorityQueue.peek();
                // If there is no previous tuple or the previous tuple can be
                // ignored
                if (outputElement == null) {
                    // Test the tuple is a delete tuple or not
                    if (((LSMTypeAwareTupleReference) checkElement.getTuple()).isAntimatter() == true) {
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
