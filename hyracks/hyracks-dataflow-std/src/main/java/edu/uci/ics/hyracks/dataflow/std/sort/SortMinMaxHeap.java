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
import java.util.Arrays;

import edu.uci.ics.hyracks.api.context.IHyracksCommonContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

/**
 * @author pouria
 *         Implements a MinMax binary heap, used as the selection tree, in
 *         sorting with replacement. Check SortMinHeap for details on comparing
 *         elements.
 */
public class SortMinMaxHeap implements ISelectionTree {
    static final int RUN_ID_IX = 0;
    static final int FRAME_IX = 1;
    static final int OFFSET_IX = 2;
    private static final int PNK_IX = 3;
    private static final int NOT_EXIST = -1;
    private static final int ELEMENT_SIZE = 4;
    private static final int INIT_ARRAY_SIZE = 512;

    private final int[] sortFields;
    private final IBinaryComparator[] comparators;
    private final RecordDescriptor recordDescriptor;
    private final FrameTupleAccessor fta1;
    private final FrameTupleAccessor fta2;

    private int[] elements;
    private int nextIx;

    private final IMemoryManager memMgr;

    public SortMinMaxHeap(IHyracksCommonContext ctx, int[] sortFields, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDesc, IMemoryManager memMgr) {
        this.sortFields = sortFields;
        this.comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            this.comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        this.recordDescriptor = recordDesc;
        fta1 = new FrameTupleAccessor(ctx.getFrameSize(), recordDescriptor);
        fta2 = new FrameTupleAccessor(ctx.getFrameSize(), recordDescriptor);
        this.memMgr = memMgr;
        this.elements = new int[INIT_ARRAY_SIZE];
        Arrays.fill(elements, -1);
        this.nextIx = 0;
    }

    @Override
    public void insert(int[] element) {
        if (nextIx >= elements.length) {
            elements = Arrays.copyOf(elements, elements.length * 2);
        }
        for (int i = 0; i < ELEMENT_SIZE; i++) {
            elements[nextIx + i] = element[i];
        }
        nextIx += ELEMENT_SIZE;
        bubbleUp(nextIx - ELEMENT_SIZE);
    }

    @Override
    public void getMin(int[] result) {
        if (nextIx == 0) {
            result[0] = result[1] = result[2] = result[3] = -1;
            return;
        }

        int[] topElem = delete(0);
        for (int x = 0; x < ELEMENT_SIZE; x++) {
            result[x] = topElem[x];
        }
    }

    @Override
    public void reset() {
        Arrays.fill(elements, -1);
        nextIx = 0;
    }

    @Override
    public boolean isEmpty() {
        return (nextIx < ELEMENT_SIZE);
    }

    @Override
    public void peekMin(int[] result) {
        if (nextIx == 0) {
            result[0] = result[1] = result[2] = result[3] = -1;
            return;
        }

        for (int x = 0; x < ELEMENT_SIZE; x++) {
            result[x] = elements[x];
        }
    }

    @Override
    public void getMax(int[] result) {
        if (nextIx == ELEMENT_SIZE) {
            int[] topElement = removeLast();
            for (int x = 0; x < ELEMENT_SIZE; x++) {
                result[x] = topElement[x];
            }
            return;
        }

        if (nextIx > ELEMENT_SIZE) {
            int lc = getLeftChild(0);
            int rc = getRightChild(0);
            int maxIx = lc;

            if (rc != -1) {
                maxIx = compare(lc, rc) < 0 ? rc : lc;
            }

            int[] maxElem = delete(maxIx);
            for (int x = 0; x < ELEMENT_SIZE; x++) {
                result[x] = maxElem[x];
            }
            return;
        }

        result[0] = result[1] = result[2] = result[3] = -1;

    }

    @Override
    public void peekMax(int[] result) {
        if (nextIx == ELEMENT_SIZE) {
            for (int i = 0; i < ELEMENT_SIZE; i++) {
                result[i] = elements[i];
            }
            return;
        }
        if (nextIx > ELEMENT_SIZE) {
            int lc = getLeftChild(0);
            int rc = getRightChild(0);
            int maxIx = lc;

            if (rc != -1) {
                maxIx = compare(lc, rc) < 0 ? rc : lc;
            }

            for (int x = 0; x < ELEMENT_SIZE; x++) {
                result[x] = elements[maxIx + x];
            }

            return;
        }
        result[0] = result[1] = result[2] = result[3] = -1;
    }

    private int[] delete(int delIx) {
        int s = nextIx;
        if (nextIx > ELEMENT_SIZE) {
            int[] delEntry = Arrays.copyOfRange(elements, delIx, delIx + ELEMENT_SIZE);
            int[] last = removeLast();
            if (delIx != (s - ELEMENT_SIZE)) {
                for (int x = 0; x < ELEMENT_SIZE; x++) {
                    elements[delIx + x] = last[x];
                }
                trickleDown(delIx);
            }
            return delEntry;
        } else if (nextIx == ELEMENT_SIZE) {
            return (removeLast());
        }
        return null;
    }

    private int[] removeLast() {
        if (nextIx < ELEMENT_SIZE) { //this is the very last element
            return new int[] { -1, -1, -1, -1 };
        }
        int[] l = Arrays.copyOfRange(elements, nextIx - ELEMENT_SIZE, nextIx);
        Arrays.fill(elements, nextIx - ELEMENT_SIZE, nextIx, -1);
        nextIx -= ELEMENT_SIZE;
        return l;
    }

    private void bubbleUp(int ix) {
        int p = getParentIx(ix);
        if (isAtMinLevel(ix)) {
            if (p != NOT_EXIST && compare(p, ix) < 0) {
                swap(ix, p);
                bubbleUpMax(p);
            } else {
                bubbleUpMin(ix);
            }
        } else { // i is at max level
            if (p != NOT_EXIST && compare(ix, p) < 0) {
                swap(ix, p);
                bubbleUpMin(p);
            } else {
                bubbleUpMax(ix);
            }
        }
    }

    private void bubbleUpMax(int ix) {
        int gp = getGrandParent(ix);
        if (gp != NOT_EXIST && compare(gp, ix) < 0) {
            swap(ix, gp);
            bubbleUpMax(gp);
        }
    }

    private void bubbleUpMin(int ix) {
        int gp = getGrandParent(ix);
        if (gp != NOT_EXIST && compare(ix, gp) < 0) {
            swap(ix, gp);
            bubbleUpMin(gp);
        }
    }

    private void trickleDown(int ix) {
        if (isAtMinLevel(ix)) {
            trickleDownMin(ix);
        } else {
            trickleDownMax(ix);
        }
    }

    private void trickleDownMax(int ix) {
        int maxIx = getMaxOfDescendents(ix);
        if (maxIx == NOT_EXIST) {
            return;
        }
        if (maxIx > getLeftChild(ix) && maxIx > getRightChild(ix)) { // A grand
                                                                     // children
            if (compare(ix, maxIx) < 0) {
                swap(maxIx, ix);
                int p = getParentIx(maxIx);
                if (p != NOT_EXIST && compare(maxIx, p) < 0) {
                    swap(maxIx, p);
                }
                trickleDownMax(maxIx);
            }
        } else { // A children
            if (compare(ix, maxIx) < 0) {
                swap(ix, maxIx);
            }
        }
    }

    private void trickleDownMin(int ix) {
        int minIx = getMinOfDescendents(ix);
        if (minIx == NOT_EXIST) {
            return;
        }
        if (minIx > getLeftChild(ix) && minIx > getRightChild(ix)) { // A grand
                                                                     // children
            if (compare(minIx, ix) < 0) {
                swap(minIx, ix);
                int p = getParentIx(minIx);
                if (p != NOT_EXIST && compare(p, minIx) < 0) {
                    swap(minIx, p);
                }
                trickleDownMin(minIx);
            }
        } else { // A children
            if (compare(minIx, ix) < 0) {
                swap(ix, minIx);
            }
        }
    }

    // Min among children and grand children
    private int getMinOfDescendents(int ix) {
        int lc = getLeftChild(ix);
        if (lc == NOT_EXIST) {
            return NOT_EXIST;
        }
        int rc = getRightChild(ix);
        if (rc == NOT_EXIST) {
            return lc;
        }
        int min = (compare(lc, rc) < 0) ? lc : rc;
        int[] lgc = getLeftGrandChildren(ix);
        int[] rgc = getRightGrandChildren(ix);
        for (int k = 0; k < 2; k++) {
            if (lgc[k] != NOT_EXIST && compare(lgc[k], min) < 0) {
                min = lgc[k];
            }
            if (rgc[k] != NOT_EXIST && compare(rgc[k], min) < 0) {
                min = rgc[k];
            }
        }
        return min;
    }

    // Max among children and grand children
    private int getMaxOfDescendents(int ix) {
        int lc = getLeftChild(ix);
        if (lc == NOT_EXIST) {
            return NOT_EXIST;
        }
        int rc = getRightChild(ix);
        if (rc == NOT_EXIST) {
            return lc;
        }
        int max = (compare(lc, rc) < 0) ? rc : lc;
        int[] lgc = getLeftGrandChildren(ix);
        int[] rgc = getRightGrandChildren(ix);
        for (int k = 0; k < 2; k++) {
            if (lgc[k] != NOT_EXIST && compare(max, lgc[k]) < 0) {
                max = lgc[k];
            }
            if (rgc[k] != NOT_EXIST && compare(max, rgc[k]) < 0) {
                max = rgc[k];
            }
        }
        return max;
    }

    private void swap(int n1Ix, int n2Ix) {
        int[] temp = Arrays.copyOfRange(elements, n1Ix, n1Ix + ELEMENT_SIZE);
        for (int i = 0; i < ELEMENT_SIZE; i++) {
            elements[n1Ix + i] = elements[n2Ix + i];
            elements[n2Ix + i] = temp[i];
        }
    }

    private int getParentIx(int i) {
        if (i < ELEMENT_SIZE) {
            return NOT_EXIST;
        }
        return ((i - ELEMENT_SIZE) / (2 * ELEMENT_SIZE)) * ELEMENT_SIZE;
    }

    private int getGrandParent(int i) {
        int p = getParentIx(i);
        return p != -1 ? getParentIx(p) : NOT_EXIST;
    }

    private int getLeftChild(int i) {
        int lc = (2 * ELEMENT_SIZE) * (i / ELEMENT_SIZE) + ELEMENT_SIZE;
        return (lc < nextIx ? lc : -1);
    }

    private int[] getLeftGrandChildren(int i) {
        int lc = getLeftChild(i);
        return lc != NOT_EXIST ? new int[] { getLeftChild(lc), getRightChild(lc) } : new int[] { NOT_EXIST, NOT_EXIST };
    }

    private int getRightChild(int i) {
        int rc = (2 * ELEMENT_SIZE) * (i / ELEMENT_SIZE) + (2 * ELEMENT_SIZE);
        return (rc < nextIx ? rc : -1);
    }

    private int[] getRightGrandChildren(int i) {
        int rc = getRightChild(i);
        return rc != NOT_EXIST ? new int[] { getLeftChild(rc), getRightChild(rc) } : new int[] { NOT_EXIST, NOT_EXIST };
    }

    private boolean isAtMinLevel(int i) {
        int l = getLevel(i);
        return l % 2 == 0 ? true : false;
    }

    private int getLevel(int i) {
        if (i < ELEMENT_SIZE) {
            return 0;
        }

        int cnv = i / ELEMENT_SIZE;
        int l = (int) Math.floor(Math.log(cnv) / Math.log(2));
        if (cnv == (((int) Math.pow(2, (l + 1))) - 1)) {
            return (l + 1);
        }
        return l;
    }

    private ByteBuffer getFrame(int frameIx) {
        return (memMgr.getFrame(frameIx));
    }

    // first < sec : -1
    private int compare(int nodeSIx1, int nodeSIx2) {
        int[] n1 = Arrays.copyOfRange(elements, nodeSIx1, nodeSIx1 + ELEMENT_SIZE); //tree.get(nodeSIx1);
        int[] n2 = Arrays.copyOfRange(elements, nodeSIx2, nodeSIx2 + ELEMENT_SIZE); //tree.get(nodeSIx2);
        return (compare(n1, n2));
    }

    // first < sec : -1
    private int compare(int[] n1, int[] n2) {
        // Compare Run Numbers
        if (n1[RUN_ID_IX] != n2[RUN_ID_IX]) {
            return (n1[RUN_ID_IX] < n2[RUN_ID_IX] ? -1 : 1);
        }

        // Compare Poor man Normalized Keys
        if (n1[PNK_IX] != n2[PNK_IX]) {
            return ((((long) n1[PNK_IX]) & 0xffffffffL) < (((long) n2[PNK_IX]) & 0xffffffffL)) ? -1 : 1;
        }

        return compare(getFrame(n1[FRAME_IX]), getFrame(n2[FRAME_IX]), n1[OFFSET_IX], n2[OFFSET_IX]);
    }

    private int compare(ByteBuffer fr1, ByteBuffer fr2, int r1StartOffset, int r2StartOffset) {
        byte[] b1 = fr1.array();
        byte[] b2 = fr2.array();
        fta1.reset(fr1);
        fta2.reset(fr2);
        int headerLen = BSTNodeUtil.HEADER_SIZE;
        r1StartOffset += headerLen;
        r2StartOffset += headerLen;
        for (int f = 0; f < comparators.length; ++f) {
            int fIdx = sortFields[f];
            int f1Start = fIdx == 0 ? 0 : fr1.getInt(r1StartOffset + (fIdx - 1) * 4);
            int f1End = fr1.getInt(r1StartOffset + fIdx * 4);
            int s1 = r1StartOffset + fta1.getFieldSlotsLength() + f1Start;
            int l1 = f1End - f1Start;
            int f2Start = fIdx == 0 ? 0 : fr2.getInt(r2StartOffset + (fIdx - 1) * 4);
            int f2End = fr2.getInt(r2StartOffset + fIdx * 4);
            int s2 = r2StartOffset + fta2.getFieldSlotsLength() + f2Start;
            int l2 = f2End - f2Start;

            int c = comparators[f].compare(b1, s1, l1, b2, s2, l2);

            if (c != 0) {
                return c;
            }
        }
        return 0;
    }
}