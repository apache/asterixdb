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
package edu.uci.ics.hyracks.dataflow.std.sort;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;

/**
 * @author pouria Implements Memory Manager based on creating Binary Search Tree
 *         (BST) while Free slot size is the key for the BST nodes. Each node in
 *         BST shows a class of free slots, while all the free slots within a
 *         class have same lengths. Slots in a class are stored as a LinkedList,
 *         whose head is the BST node, corresponding to that class. BST is not
 *         stored as a separate data structure, but the free slots in the memory
 *         are used to hold BST nodes. Each BST node has the logical structure,
 *         defined in the BSTNodeUtil class.
 */
public class BSTMemMgr implements IMemoryManager {

    private final IHyracksTaskContext ctx;
    public static int frameSize;

    private ByteBuffer[] frames;
    private ByteBuffer convertBuffer;
    private Slot root;
    private Slot result; // A reusable object to hold one node returned as
                         // method result
    private Slot insertSlot; // A reusable object to hold one node within insert
                             // process
    private Slot lastLeftParent; // A reusable object for the search process
    private Slot lastLeft; // A reusable object for the search process
    private Slot parent; // A reusable object for the search process

    private Slot[] parentRes;
    private int lastFrame;

    public BSTMemMgr(IHyracksTaskContext ctx, int memSize) {
        this.ctx = ctx;
        frameSize = ctx.getFrameSize();
        convertBuffer = ByteBuffer.allocate(4);
        frames = new ByteBuffer[memSize];
        lastFrame = -1;
        root = new Slot();
        insertSlot = new Slot();
        result = new Slot();
        lastLeftParent = new Slot();
        lastLeft = new Slot();
        parent = new Slot();
        parentRes = new Slot[] { new Slot(), new Slot() };
    }

    /**
     * result is the container sent by the caller to hold the results
     */
    @Override
    public void allocate(int length, Slot result) throws HyracksDataException {
        search(length, parentRes);
        if (parentRes[1].isNull()) {
            addFrame(parentRes);
            if (parentRes[1].isNull()) {
                return;
            }
        }

        int sl = BSTNodeUtil.getLength(parentRes[1], frames, convertBuffer);
        int acLen = BSTNodeUtil.getActualLength(length);
        if (shouldSplit(sl, acLen)) {
            int[] s = split(parentRes[1], parentRes[0], acLen);
            int insertLen = BSTNodeUtil.getLength(s[2], s[3], frames, convertBuffer);
            insert(s[2], s[3], insertLen); // inserting second half of the split
                                           // slot
            BSTNodeUtil.setHeaderFooter(s[0], s[1], length, false, frames);
            result.set(s[0], s[1]);
            return;
        }
        allocate(parentRes[1], parentRes[0], length, result);
    }

    @Override
    public int unallocate(Slot s) throws HyracksDataException {
        int usedLen = BSTNodeUtil.getLength(s, frames, convertBuffer);
        int actualLen = BSTNodeUtil.getActualLength(usedLen);
        int fix = s.getFrameIx();
        int off = s.getOffset();

        int prevMemSlotFooterOffset = ((off - BSTNodeUtil.HEADER_SIZE) >= 0 ? (off - BSTNodeUtil.HEADER_SIZE)
                : BSTNodeUtil.INVALID_INDEX);
        int t = off + 2 * BSTNodeUtil.HEADER_SIZE + actualLen;
        int nextMemSlotHeaderOffset = (t < frameSize ? t : BSTNodeUtil.INVALID_INDEX);
        // Remember: next and prev memory slots have the same frame index as the
        // unallocated slot
        if (!isNodeNull(fix, prevMemSlotFooterOffset) && BSTNodeUtil.isFree(fix, prevMemSlotFooterOffset, frames)) {
            int leftLength = BSTNodeUtil.getLength(fix, prevMemSlotFooterOffset, frames, convertBuffer);
            removeFromList(fix, prevMemSlotFooterOffset - leftLength - BSTNodeUtil.HEADER_SIZE);
            int concatLength = actualLen + leftLength + 2 * BSTNodeUtil.HEADER_SIZE;
            if (!isNodeNull(fix, nextMemSlotHeaderOffset) && BSTNodeUtil.isFree(fix, nextMemSlotHeaderOffset, frames)) {
                removeFromList(fix, nextMemSlotHeaderOffset);
                concatLength += BSTNodeUtil.getLength(fix, nextMemSlotHeaderOffset, frames, convertBuffer) + 2
                        * BSTNodeUtil.HEADER_SIZE;
            }
            insert(fix, prevMemSlotFooterOffset - leftLength - BSTNodeUtil.HEADER_SIZE, concatLength); // newly
                                                                                                       // (merged)
                                                                                                       // slot
                                                                                                       // starts
                                                                                                       // at
                                                                                                       // the
                                                                                                       // prev
                                                                                                       // slot
                                                                                                       // offset
            return concatLength;

        } else if (!isNodeNull(fix, nextMemSlotHeaderOffset)
                && BSTNodeUtil.isFree(fix, nextMemSlotHeaderOffset, frames)) {
            removeFromList(fix, nextMemSlotHeaderOffset);
            int concatLength = actualLen + BSTNodeUtil.getLength(fix, nextMemSlotHeaderOffset, frames, convertBuffer)
                    + 2 * BSTNodeUtil.HEADER_SIZE;
            insert(fix, off, concatLength); // newly (merged) slot starts at the
                                            // unallocated slot offset
            return concatLength;
        }
        // unallocated slot is not merging with any neighbor
        insert(fix, off, actualLen);
        return actualLen;
    }

    @Override
    public boolean readTuple(int frameIx, int offset, FrameTupleAppender dest) {
        int offToRead = offset + BSTNodeUtil.HEADER_SIZE;
        int length = BSTNodeUtil.getLength(frameIx, offset, frames, convertBuffer);
        return dest.append(frames[frameIx].array(), offToRead, length);
    }

    @Override
    public boolean writeTuple(int frameIx, int offset, FrameTupleAccessor src, int tIndex) {
        int offToCopy = offset + BSTNodeUtil.HEADER_SIZE;
        int tStartOffset = src.getTupleStartOffset(tIndex);
        int tEndOffset = src.getTupleEndOffset(tIndex);
        int tupleLength = tEndOffset - tStartOffset;
        ByteBuffer srcBuffer = src.getBuffer();
        System.arraycopy(srcBuffer.array(), tStartOffset, frames[frameIx].array(), offToCopy, tupleLength);
        return true;
    }

    @Override
    public ByteBuffer getFrame(int frameIndex) {
        return frames[frameIndex];
    }

    @Override
    public void close() {
        //clean up all frames
        for (int i = 0; i < frames.length; i++)
            frames[i] = null;
    }

    /**
     * @param parentResult
     *            is the container passed by the caller to contain the results
     * @throws HyracksDataException
     */
    private void addFrame(Slot[] parentResult) throws HyracksDataException {
        clear(parentResult);
        if ((lastFrame + 1) >= frames.length) {
            return;
        }
        frames[++lastFrame] = allocateFrame();
        int l = frameSize - 2 * BSTNodeUtil.HEADER_SIZE;
        BSTNodeUtil.setHeaderFooter(lastFrame, 0, l, true, frames);
        initNewNode(lastFrame, 0);

        parentResult[1].copy(root);
        if (parentResult[1].isNull()) { // root is null
            root.set(lastFrame, 0);
            initNewNode(root.getFrameIx(), root.getOffset());
            parentResult[1].copy(root);
            return;
        }

        while (!parentResult[1].isNull()) {
            if (BSTNodeUtil.getLength(parentResult[1], frames, convertBuffer) == l) {
                append(parentResult[1].getFrameIx(), parentResult[1].getOffset(), lastFrame, 0);
                parentResult[1].set(lastFrame, 0);
                return;
            }
            if (l < BSTNodeUtil.getLength(parentResult[1], frames, convertBuffer)) {
                if (isNodeNull(BSTNodeUtil.getLeftChildFrameIx(parentResult[1], frames, convertBuffer),
                        BSTNodeUtil.getLeftChildOffset(parentResult[1], frames, convertBuffer))) {
                    BSTNodeUtil.setLeftChild(parentResult[1].getFrameIx(), parentResult[1].getOffset(), lastFrame, 0,
                            frames);
                    parentResult[0].copy(parentResult[1]);
                    parentResult[1].set(lastFrame, 0);
                    return;
                } else {
                    parentResult[0].copy(parentResult[1]);
                    parentResult[1].set(BSTNodeUtil.getLeftChildFrameIx(parentResult[1], frames, convertBuffer),
                            BSTNodeUtil.getLeftChildOffset(parentResult[1], frames, convertBuffer));
                }
            } else {
                if (isNodeNull(BSTNodeUtil.getRightChildFrameIx(parentResult[1], frames, convertBuffer),
                        BSTNodeUtil.getRightChildOffset(parentResult[1], frames, convertBuffer))) {
                    BSTNodeUtil.setRightChild(parentResult[1].getFrameIx(), parentResult[1].getOffset(), lastFrame, 0,
                            frames);
                    parentResult[0].copy(parentResult[1]);
                    parentResult[1].set(lastFrame, 0);
                    return;
                } else {
                    parentResult[0].copy(parentResult[1]);
                    parentResult[1].set(BSTNodeUtil.getRightChildFrameIx(parentResult[1], frames, convertBuffer),
                            BSTNodeUtil.getRightChildOffset(parentResult[1], frames, convertBuffer));
                }
            }
        }
        throw new HyracksDataException("New Frame could not be added to BSTMemMgr");
    }

    private void insert(int fix, int off, int length) throws HyracksDataException {
        BSTNodeUtil.setHeaderFooter(fix, off, length, true, frames);
        initNewNode(fix, off);

        if (root.isNull()) {
            root.set(fix, off);
            return;
        }

        insertSlot.clear();
        insertSlot.copy(root);
        while (!insertSlot.isNull()) {
            int curSlotLen = BSTNodeUtil.getLength(insertSlot, frames, convertBuffer);
            if (curSlotLen == length) {
                append(insertSlot.getFrameIx(), insertSlot.getOffset(), fix, off);
                return;
            }
            if (length < curSlotLen) {
                int leftChildFIx = BSTNodeUtil.getLeftChildFrameIx(insertSlot, frames, convertBuffer);
                int leftChildOffset = BSTNodeUtil.getLeftChildOffset(insertSlot, frames, convertBuffer);
                if (isNodeNull(leftChildFIx, leftChildOffset)) {
                    initNewNode(fix, off);
                    BSTNodeUtil.setLeftChild(insertSlot.getFrameIx(), insertSlot.getOffset(), fix, off, frames);
                    return;
                } else {
                    insertSlot.set(leftChildFIx, leftChildOffset);
                }
            } else {
                int rightChildFIx = BSTNodeUtil.getRightChildFrameIx(insertSlot, frames, convertBuffer);
                int rightChildOffset = BSTNodeUtil.getRightChildOffset(insertSlot, frames, convertBuffer);
                if (isNodeNull(rightChildFIx, rightChildOffset)) {
                    initNewNode(fix, off);
                    BSTNodeUtil.setRightChild(insertSlot.getFrameIx(), insertSlot.getOffset(), fix, off, frames);
                    return;
                } else {
                    insertSlot.set(rightChildFIx, rightChildOffset);
                }
            }
        }
        throw new HyracksDataException("Failure in node insertion into BST in BSTMemMgr");
    }

    /**
     * @param length
     * @param target
     *            is the container sent by the caller to hold the results
     */
    private void search(int length, Slot[] target) {
        clear(target);
        result.clear();

        if (root.isNull()) {
            return;
        }

        lastLeftParent.clear();
        lastLeft.clear();
        parent.clear();
        result.copy(root);

        while (!result.isNull()) {
            if (BSTNodeUtil.getLength(result, frames, convertBuffer) == length) {
                target[0].copy(parent);
                target[1].copy(result);
                return;
            }
            if (length < BSTNodeUtil.getLength(result, frames, convertBuffer)) {
                lastLeftParent.copy(parent);
                lastLeft.copy(result);
                parent.copy(result);
                int fix = BSTNodeUtil.getLeftChildFrameIx(result, frames, convertBuffer);
                int off = BSTNodeUtil.getLeftChildOffset(result, frames, convertBuffer);
                result.set(fix, off);
            } else {
                parent.copy(result);
                int fix = BSTNodeUtil.getRightChildFrameIx(result, frames, convertBuffer);
                int off = BSTNodeUtil.getRightChildOffset(result, frames, convertBuffer);
                result.set(fix, off);
            }
        }

        target[0].copy(lastLeftParent);
        target[1].copy(lastLeft);

    }

    private void append(int headFix, int headOff, int nodeFix, int nodeOff) {
        initNewNode(nodeFix, nodeOff);

        int fix = BSTNodeUtil.getNextFrameIx(headFix, headOff, frames, convertBuffer); // frameIx
        // for
        // the
        // current
        // next
        // of
        // head
        int off = BSTNodeUtil.getNextOffset(headFix, headOff, frames, convertBuffer); // offset
                                                                                      // for
                                                                                      // the
                                                                                      // current
                                                                                      // next
                                                                                      // of
                                                                                      // head
        BSTNodeUtil.setNext(nodeFix, nodeOff, fix, off, frames);

        if (!isNodeNull(fix, off)) {
            BSTNodeUtil.setPrev(fix, off, nodeFix, nodeOff, frames);
        }
        BSTNodeUtil.setPrev(nodeFix, nodeOff, headFix, headOff, frames);
        BSTNodeUtil.setNext(headFix, headOff, nodeFix, nodeOff, frames);
    }

    private int[] split(Slot listHead, Slot parent, int length) {
        int l2 = BSTNodeUtil.getLength(listHead, frames, convertBuffer) - length - 2 * BSTNodeUtil.HEADER_SIZE;
        // We split the node after slots-list head
        if (!isNodeNull(BSTNodeUtil.getNextFrameIx(listHead, frames, convertBuffer),
                BSTNodeUtil.getNextOffset(listHead, frames, convertBuffer))) {
            int afterHeadFix = BSTNodeUtil.getNextFrameIx(listHead, frames, convertBuffer);
            int afterHeadOff = BSTNodeUtil.getNextOffset(listHead, frames, convertBuffer);
            int afHNextFix = BSTNodeUtil.getNextFrameIx(afterHeadFix, afterHeadOff, frames, convertBuffer);
            int afHNextOff = BSTNodeUtil.getNextOffset(afterHeadFix, afterHeadOff, frames, convertBuffer);
            BSTNodeUtil.setNext(listHead.getFrameIx(), listHead.getOffset(), afHNextFix, afHNextOff, frames);
            if (!isNodeNull(afHNextFix, afHNextOff)) {
                BSTNodeUtil.setPrev(afHNextFix, afHNextOff, listHead.getFrameIx(), listHead.getOffset(), frames);
            }
            int secondOffset = afterHeadOff + length + 2 * BSTNodeUtil.HEADER_SIZE;
            BSTNodeUtil.setHeaderFooter(afterHeadFix, afterHeadOff, length, true, frames);
            BSTNodeUtil.setHeaderFooter(afterHeadFix, secondOffset, l2, true, frames);

            return new int[] { afterHeadFix, afterHeadOff, afterHeadFix, secondOffset };
        }
        // We split the head
        int secondOffset = listHead.getOffset() + length + 2 * BSTNodeUtil.HEADER_SIZE;
        BSTNodeUtil.setHeaderFooter(listHead.getFrameIx(), listHead.getOffset(), length, true, frames);
        BSTNodeUtil.setHeaderFooter(listHead.getFrameIx(), secondOffset, l2, true, frames);

        fixTreePtrs(listHead.getFrameIx(), listHead.getOffset(), parent.getFrameIx(), parent.getOffset());
        return new int[] { listHead.getFrameIx(), listHead.getOffset(), listHead.getFrameIx(), secondOffset };
    }

    private void fixTreePtrs(int nodeFrameIx, int nodeOffset, int parentFrameIx, int parentOffset) {
        int nodeLeftChildFrameIx = BSTNodeUtil.getLeftChildFrameIx(nodeFrameIx, nodeOffset, frames, convertBuffer);
        int nodeLeftChildOffset = BSTNodeUtil.getLeftChildOffset(nodeFrameIx, nodeOffset, frames, convertBuffer);
        int nodeRightChildFrameIx = BSTNodeUtil.getRightChildFrameIx(nodeFrameIx, nodeOffset, frames, convertBuffer);
        int nodeRightChildOffset = BSTNodeUtil.getRightChildOffset(nodeFrameIx, nodeOffset, frames, convertBuffer);

        int status = -1; // (status==0 if node is left child of parent)
                         // (status==1 if node is right child of parent)
        if (!isNodeNull(parentFrameIx, parentOffset)) {
            int nlen = BSTNodeUtil.getActualLength(BSTNodeUtil
                    .getLength(nodeFrameIx, nodeOffset, frames, convertBuffer));
            int plen = BSTNodeUtil.getActualLength(BSTNodeUtil.getLength(parentFrameIx, parentOffset, frames,
                    convertBuffer));
            status = ((nlen < plen) ? 0 : 1);
        }

        if (!isNodeNull(nodeLeftChildFrameIx, nodeLeftChildOffset)
                && !isNodeNull(nodeRightChildFrameIx, nodeRightChildOffset)) { // Node
            // has
            // two
            // children
            int pMinFIx = nodeFrameIx;
            int pMinOff = nodeOffset;
            int minFIx = nodeRightChildFrameIx;
            int minOff = nodeRightChildOffset;
            int nextLeftFIx = BSTNodeUtil.getLeftChildFrameIx(minFIx, minOff, frames, convertBuffer);
            int nextLeftOff = BSTNodeUtil.getLeftChildOffset(minFIx, minOff, frames, convertBuffer);

            while (!isNodeNull(nextLeftFIx, nextLeftOff)) {
                pMinFIx = minFIx;
                pMinOff = minOff;
                minFIx = nextLeftFIx;
                minOff = nextLeftOff;
                nextLeftFIx = BSTNodeUtil.getLeftChildFrameIx(minFIx, minOff, frames, convertBuffer); // min
                                                                                                      // is
                                                                                                      // now
                                                                                                      // pointing
                                                                                                      // to
                                                                                                      // current
                                                                                                      // (old)
                                                                                                      // next
                                                                                                      // left
                nextLeftOff = BSTNodeUtil.getLeftChildOffset(minFIx, minOff, frames, convertBuffer); // min
                                                                                                     // is
                                                                                                     // now
                                                                                                     // pointing
                                                                                                     // to
                                                                                                     // current
                                                                                                     // (old)
                                                                                                     // next
                                                                                                     // left
            }

            if ((nodeRightChildFrameIx == minFIx) && (nodeRightChildOffset == minOff)) { // nrc
                                                                                         // is
                                                                                         // the
                // same as min
                BSTNodeUtil.setLeftChild(nodeRightChildFrameIx, nodeRightChildOffset, nodeLeftChildFrameIx,
                        nodeLeftChildOffset, frames);
            } else { // min is different from nrc
                int minRightFIx = BSTNodeUtil.getRightChildFrameIx(minFIx, minOff, frames, convertBuffer);
                int minRightOffset = BSTNodeUtil.getRightChildOffset(minFIx, minOff, frames, convertBuffer);
                BSTNodeUtil.setRightChild(minFIx, minOff, nodeRightChildFrameIx, nodeRightChildOffset, frames);
                BSTNodeUtil.setLeftChild(minFIx, minOff, nodeLeftChildFrameIx, nodeLeftChildOffset, frames);
                BSTNodeUtil.setLeftChild(pMinFIx, pMinOff, minRightFIx, minRightOffset, frames);
            }

            // Now dealing with the parent
            if (!isNodeNull(parentFrameIx, parentOffset)) {
                if (status == 0) {
                    BSTNodeUtil.setLeftChild(parentFrameIx, parentOffset, minFIx, minOff, frames);
                } else if (status == 1) {
                    BSTNodeUtil.setRightChild(parentFrameIx, parentOffset, minFIx, minOff, frames);
                }
            } else { // No parent (node was the root)
                root.set(minFIx, minOff);
            }
            return;
        }

        else if (!isNodeNull(nodeLeftChildFrameIx, nodeLeftChildOffset)) { // Node
                                                                           // has
                                                                           // only
                                                                           // left
                                                                           // child
            if (status == 0) {
                BSTNodeUtil
                        .setLeftChild(parentFrameIx, parentOffset, nodeLeftChildFrameIx, nodeLeftChildOffset, frames);
            } else if (status == 1) {
                BSTNodeUtil.setRightChild(parentFrameIx, parentOffset, nodeLeftChildFrameIx, nodeLeftChildOffset,
                        frames);
            } else if (status == -1) { // No parent, so node is root
                root.set(nodeLeftChildFrameIx, nodeLeftChildOffset);
            }
            return;
        }

        else if (!isNodeNull(nodeRightChildFrameIx, nodeRightChildOffset)) { // Node
                                                                             // has
                                                                             // only
                                                                             // right
                                                                             // child
            if (status == 0) {
                BSTNodeUtil.setLeftChild(parentFrameIx, parentOffset, nodeRightChildFrameIx, nodeRightChildOffset,
                        frames);
            } else if (status == 1) {
                BSTNodeUtil.setRightChild(parentFrameIx, parentOffset, nodeRightChildFrameIx, nodeRightChildOffset,
                        frames);
            } else if (status == -1) { // No parent, so node is root
                root.set(nodeRightChildFrameIx, nodeRightChildOffset);
            }
            return;
        }

        else { // Node is leaf (no children)
            if (status == 0) {
                BSTNodeUtil.setLeftChild(parentFrameIx, parentOffset, BSTNodeUtil.INVALID_INDEX,
                        BSTNodeUtil.INVALID_INDEX, frames);
            } else if (status == 1) {
                BSTNodeUtil.setRightChild(parentFrameIx, parentOffset, BSTNodeUtil.INVALID_INDEX,
                        BSTNodeUtil.INVALID_INDEX, frames);
            } else { // node was the only node in the tree
                root.clear();
            }
            return;
        }
    }

    /**
     * Allocation with no splitting but padding
     * 
     * @param node
     * @param parent
     * @param result
     *            is the container sent by the caller to hold the results
     */
    private void allocate(Slot node, Slot parent, int length, Slot result) {
        int nextFix = BSTNodeUtil.getNextFrameIx(node, frames, convertBuffer);
        int nextOff = BSTNodeUtil.getNextOffset(node, frames, convertBuffer);
        if (!isNodeNull(nextFix, nextOff)) {
            int nextOfNextFIx = BSTNodeUtil.getNextFrameIx(nextFix, nextOff, frames, convertBuffer);
            int nextOfNextOffset = BSTNodeUtil.getNextOffset(nextFix, nextOff, frames, convertBuffer);
            BSTNodeUtil.setNext(node.getFrameIx(), node.getOffset(), nextOfNextFIx, nextOfNextOffset, frames);
            if (!isNodeNull(nextOfNextFIx, nextOfNextOffset)) {
                BSTNodeUtil.setPrev(nextOfNextFIx, nextOfNextOffset, node.getFrameIx(), node.getOffset(), frames);
            }
            BSTNodeUtil.setHeaderFooter(nextFix, nextOff, length, false, frames);
            result.set(nextFix, nextOff);
            return;
        }

        fixTreePtrs(node.getFrameIx(), node.getOffset(), parent.getFrameIx(), parent.getOffset());
        BSTNodeUtil.setHeaderFooter(node.getFrameIx(), node.getOffset(), length, false, frames);
        result.copy(node);
    }

    private void removeFromList(int fix, int off) {
        int nextFIx = BSTNodeUtil.getNextFrameIx(fix, off, frames, convertBuffer);
        int nextOffset = BSTNodeUtil.getNextOffset(fix, off, frames, convertBuffer);
        int prevFIx = BSTNodeUtil.getPrevFrameIx(fix, off, frames, convertBuffer);
        int prevOffset = BSTNodeUtil.getPrevOffset(fix, off, frames, convertBuffer);
        if (!isNodeNull(prevFIx, prevOffset) && !isNodeNull(nextFIx, nextOffset)) {
            BSTNodeUtil.setNext(prevFIx, prevOffset, nextFIx, nextOffset, frames);
            BSTNodeUtil.setPrev(nextFIx, nextOffset, prevFIx, prevOffset, frames);
            BSTNodeUtil.setNext(fix, off, BSTNodeUtil.INVALID_INDEX, BSTNodeUtil.INVALID_INDEX, frames);
            BSTNodeUtil.setPrev(fix, off, BSTNodeUtil.INVALID_INDEX, BSTNodeUtil.INVALID_INDEX, frames);
            return;
        }
        if (!isNodeNull(prevFIx, prevOffset)) {
            BSTNodeUtil.setNext(prevFIx, prevOffset, BSTNodeUtil.INVALID_INDEX, BSTNodeUtil.INVALID_INDEX, frames);
            BSTNodeUtil.setPrev(fix, off, BSTNodeUtil.INVALID_INDEX, BSTNodeUtil.INVALID_INDEX, frames);
            return;
        }

        // We need to find the parent, so we can fix the tree
        int parentFIx = BSTNodeUtil.INVALID_INDEX;
        int parentOffset = BSTNodeUtil.INVALID_INDEX;
        int length = BSTNodeUtil.getActualLength(BSTNodeUtil.getLength(fix, off, frames, convertBuffer));
        fix = root.getFrameIx();
        off = root.getOffset();
        int curLen = BSTNodeUtil.getLength(fix, off, frames, convertBuffer);
        while (length != curLen) {
            parentFIx = fix;
            parentOffset = off;
            if (length < curLen) {
                fix = BSTNodeUtil.getLeftChildFrameIx(parentFIx, parentOffset, frames, convertBuffer); // parentFIx
                // is
                // now
                // the
                // old(current)
                // fix
                off = BSTNodeUtil.getLeftChildOffset(parentFIx, parentOffset, frames, convertBuffer); // parentOffset
                // is
                // now
                // the
                // old(current)
                // off
            } else {
                fix = BSTNodeUtil.getRightChildFrameIx(parentFIx, parentOffset, frames, convertBuffer); // parentFIx
                // is
                // now
                // the
                // old(current)
                // fix
                off = BSTNodeUtil.getRightChildOffset(parentFIx, parentOffset, frames, convertBuffer); // parentOffset
                // is
                // now
                // the
                // old(current)
                // off
            }
            curLen = BSTNodeUtil.getLength(fix, off, frames, convertBuffer);
        }

        if (!isNodeNull(nextFIx, nextOffset)) { // it is head of the list (in
                                                // the
            // tree)
            BSTNodeUtil.setPrev(nextFIx, nextOffset, BSTNodeUtil.INVALID_INDEX, BSTNodeUtil.INVALID_INDEX, frames);
            int nodeLeftChildFIx = BSTNodeUtil.getLeftChildFrameIx(fix, off, frames, convertBuffer);
            int nodeLeftChildOffset = BSTNodeUtil.getLeftChildOffset(fix, off, frames, convertBuffer);
            int nodeRightChildFix = BSTNodeUtil.getRightChildFrameIx(fix, off, frames, convertBuffer);
            int nodeRightChildOffset = BSTNodeUtil.getRightChildOffset(fix, off, frames, convertBuffer);
            BSTNodeUtil.setLeftChild(nextFIx, nextOffset, nodeLeftChildFIx, nodeLeftChildOffset, frames);
            BSTNodeUtil.setRightChild(nextFIx, nextOffset, nodeRightChildFix, nodeRightChildOffset, frames);
            if (!isNodeNull(parentFIx, parentOffset)) {
                int parentLength = BSTNodeUtil.getLength(parentFIx, parentOffset, frames, convertBuffer);
                if (length < parentLength) {
                    BSTNodeUtil.setLeftChild(parentFIx, parentOffset, nextFIx, nextOffset, frames);
                } else {
                    BSTNodeUtil.setRightChild(parentFIx, parentOffset, nextFIx, nextOffset, frames);
                }
            }

            if ((root.getFrameIx() == fix) && (root.getOffset() == off)) {
                root.set(nextFIx, nextOffset);
            }

            return;
        }

        fixTreePtrs(fix, off, parentFIx, parentOffset);
    }

    private void clear(Slot[] s) {
        s[0].clear();
        s[1].clear();
    }

    private boolean isNodeNull(int frameIx, int offset) {
        return ((frameIx == BSTNodeUtil.INVALID_INDEX) || (offset == BSTNodeUtil.INVALID_INDEX) || (frames[frameIx] == null));
    }

    private boolean shouldSplit(int slotLength, int reqLength) {
        return ((slotLength - reqLength) >= BSTNodeUtil.MINIMUM_FREE_SLOT_SIZE);
    }

    private void initNewNode(int frameIx, int offset) {
        BSTNodeUtil.setLeftChild(frameIx, offset, BSTNodeUtil.INVALID_INDEX, BSTNodeUtil.INVALID_INDEX, frames);
        BSTNodeUtil.setRightChild(frameIx, offset, BSTNodeUtil.INVALID_INDEX, BSTNodeUtil.INVALID_INDEX, frames);
        BSTNodeUtil.setNext(frameIx, offset, BSTNodeUtil.INVALID_INDEX, BSTNodeUtil.INVALID_INDEX, frames);
        BSTNodeUtil.setPrev(frameIx, offset, BSTNodeUtil.INVALID_INDEX, BSTNodeUtil.INVALID_INDEX, frames);
    }

    private ByteBuffer allocateFrame() throws HyracksDataException {
        return ctx.allocateFrame();
    }

    public String debugPrintMemory() {
        Slot s = new Slot(0, 0);
        if (s.isNull()) {
            return "memory:\tNull";
        }

        String m = "memory:\n" + debugPrintSlot(0, 0) + "\n";
        int length = BSTNodeUtil.getActualLength(BSTNodeUtil.getLength(0, 0, frames, convertBuffer));
        int noff = (length + 2 * BSTNodeUtil.HEADER_SIZE >= frameSize ? BSTNodeUtil.INVALID_INDEX : length + 2
                * BSTNodeUtil.HEADER_SIZE);
        int nfix = (noff == BSTNodeUtil.INVALID_INDEX ? ((frames.length == 1) ? BSTNodeUtil.INVALID_INDEX : 1) : 0);
        if (noff == BSTNodeUtil.INVALID_INDEX && nfix != BSTNodeUtil.INVALID_INDEX) {
            noff = 0;
        }
        s.set(nfix, noff);
        while (!isNodeNull(s.getFrameIx(), s.getOffset())) {
            m += debugPrintSlot(s.getFrameIx(), s.getOffset()) + "\n";
            length = BSTNodeUtil.getActualLength(BSTNodeUtil.getLength(s.getFrameIx(), s.getOffset(), frames,
                    convertBuffer));
            noff = (s.getOffset() + length + 2 * BSTNodeUtil.HEADER_SIZE >= frameSize ? BSTNodeUtil.INVALID_INDEX : s
                    .getOffset() + length + 2 * BSTNodeUtil.HEADER_SIZE);
            nfix = (noff == BSTNodeUtil.INVALID_INDEX ? ((frames.length - 1 == s.getFrameIx()) ? BSTNodeUtil.INVALID_INDEX
                    : s.getFrameIx() + 1)
                    : s.getFrameIx());
            if (noff == BSTNodeUtil.INVALID_INDEX && nfix != BSTNodeUtil.INVALID_INDEX) {
                noff = 0;
            }
            s.set(nfix, noff);
        }
        return m;
    }

    public String debugPrintTree() {
        Slot node = new Slot();
        node.copy(root);
        if (!node.isNull()) {
            return debugPrintSubTree(node);
        }
        return "Null";
    }

    private String debugPrintSubTree(Slot r) {
        Slot node = new Slot();
        node.copy(r);
        int fix = node.getFrameIx();
        int off = node.getOffset();
        int lfix = BSTNodeUtil.getLeftChildFrameIx(node, frames, convertBuffer);
        int loff = BSTNodeUtil.getLeftChildOffset(node, frames, convertBuffer);
        int rfix = BSTNodeUtil.getRightChildFrameIx(node, frames, convertBuffer);
        int roff = BSTNodeUtil.getRightChildOffset(node, frames, convertBuffer);
        int nfix = BSTNodeUtil.getNextFrameIx(node, frames, convertBuffer);
        int noff = BSTNodeUtil.getNextOffset(node, frames, convertBuffer);
        int pfix = BSTNodeUtil.getPrevFrameIx(node, frames, convertBuffer);
        int poff = BSTNodeUtil.getPrevOffset(node, frames, convertBuffer);

        String s = "{" + r.getFrameIx() + ", " + r.getOffset() + " (Len: "
                + BSTNodeUtil.getLength(fix, off, frames, convertBuffer) + ") - " + "(LC: "
                + debugPrintSlot(lfix, loff) + ") - " + "(RC: " + debugPrintSlot(rfix, roff) + ") - " + "(NX: "
                + debugPrintSlot(nfix, noff) + ") - " + "(PR: " + debugPrintSlot(pfix, poff) + ")  }\n";
        if (!isNodeNull(lfix, loff)) {
            s += debugPrintSubTree(new Slot(lfix, loff)) + "\n";
        }
        if (!isNodeNull(rfix, roff)) {
            s += debugPrintSubTree(new Slot(rfix, roff)) + "\n";
        }

        return s;
    }

    private String debugPrintSlot(int fix, int off) {
        if (isNodeNull(fix, off)) {
            return BSTNodeUtil.INVALID_INDEX + ", " + BSTNodeUtil.INVALID_INDEX;
        }
        int l = BSTNodeUtil.getLength(fix, off, frames, convertBuffer);
        int al = BSTNodeUtil.getActualLength(l);
        boolean f = BSTNodeUtil.isFree(fix, off, frames);
        return fix + ", " + off + " (free: " + f + ") (Len: " + l + ") (actual len: " + al + ") ";
    }
}