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
 *         Implements a minimum binary heap, used as selection tree, for sort
 *         with replacement. This heap structure can only be used as the min
 *         heap (no access to the max element). Elements in the heap are
 *         compared based on their run numbers, and sorting key(s):
 *         Considering two heap elements A and B:
 *         if RunNumber(A) > RunNumber(B) then A is larger than B if
 *         RunNumber(A) == RunNumber(B), then A is smaller than B, if and only
 *         if the value of the sort key(s) in B is greater than A (based on the
 *         sort comparator).
 */
public class SortMinHeap implements ISelectionTree {

    static final int RUN_ID_IX = 0;
    static final int FRAME_IX = 1;
    static final int OFFSET_IX = 2;
    private static final int PNK_IX = 3;
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
    private int[] top; // Used as a temp variable to access the top, to avoid object creation

    public SortMinHeap(IHyracksCommonContext ctx, int[] sortFields, IBinaryComparatorFactory[] comparatorFactories,
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
        this.top = new int[ELEMENT_SIZE];
        Arrays.fill(top, -1);
        this.elements = new int[INIT_ARRAY_SIZE];
        Arrays.fill(elements, -1);
        this.nextIx = 0;
    }

    /*
     * Assumption (element structure): [RunId][FrameIx][Offset][Poorman NK]
     */
    @Override
    public void getMin(int[] result) {
        if (nextIx == 0) {
            result[0] = result[1] = result[2] = result[3] = -1;
            return;
        }

        top = delete(0);
        for (int i = 0; i < top.length; i++) {
            result[i] = top[i];
        }
    }

    @Override
    public void peekMin(int[] result) {
        if (nextIx == 0) {
            result[0] = result[1] = result[2] = result[3] = -1;
            return;
        }
        for (int i = 0; i < ELEMENT_SIZE; i++) {
            result[i] = elements[i];
        }
    }

    @Override
    public void insert(int[] e) {
        if (nextIx >= elements.length) {
            elements = Arrays.copyOf(elements, elements.length * 2);
        }
        for (int i = 0; i < ELEMENT_SIZE; i++) {
            elements[nextIx + i] = e[i];
        }
        siftUp(nextIx);
        nextIx += ELEMENT_SIZE;

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

    public int _debugGetSize() {
        return (nextIx > 0 ? (nextIx - 1) / 4 : 0);
    }

    private int[] delete(int nix) {
        int[] nv = Arrays.copyOfRange(elements, nix, nix + ELEMENT_SIZE);
        int[] lastElem = removeLast();

        if (nextIx == 0) {
            return nv;
        }

        for (int i = 0; i < ELEMENT_SIZE; i++) {
            elements[nix + i] = lastElem[i];
        }
        int pIx = getParent(nix);
        if (pIx > -1 && (compare(lastElem, Arrays.copyOfRange(elements, pIx, pIx + ELEMENT_SIZE)) < 0)) {
            siftUp(nix);
        } else {
            siftDown(nix);
        }
        return nv;
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

    private void siftUp(int nodeIx) {
        int p = getParent(nodeIx);
        if (p < 0) {
            return;
        }
        while (p > -1 && (compare(nodeIx, p) < 0)) {
            swap(p, nodeIx);
            nodeIx = p;
            p = getParent(nodeIx);
            if (p < 0) { // We are at the root
                return;
            }
        }
    }

    private void siftDown(int nodeIx) {
        int mix = getMinOfChildren(nodeIx);
        if (mix < 0) {
            return;
        }
        while (mix > -1 && (compare(mix, nodeIx) < 0)) {
            swap(mix, nodeIx);
            nodeIx = mix;
            mix = getMinOfChildren(nodeIx);
            if (mix < 0) { // We hit the leaf level
                return;
            }
        }
    }

    // first < sec : -1
    private int compare(int nodeSIx1, int nodeSIx2) {
        int[] n1 = Arrays.copyOfRange(elements, nodeSIx1, nodeSIx1 + ELEMENT_SIZE);
        int[] n2 = Arrays.copyOfRange(elements, nodeSIx2, nodeSIx2 + ELEMENT_SIZE);
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

    private int getMinOfChildren(int nix) { // returns index of min child
        int lix = getLeftChild(nix);
        if (lix < 0) {
            return -1;
        }
        int rix = getRightChild(nix);
        if (rix < 0) {
            return lix;
        }
        return ((compare(lix, rix) < 0) ? lix : rix);
    }

    //Assumption: n1Ix and n2Ix are starting indices of two elements
    private void swap(int n1Ix, int n2Ix) {
        int[] temp = Arrays.copyOfRange(elements, n1Ix, n1Ix + ELEMENT_SIZE);
        for (int i = 0; i < ELEMENT_SIZE; i++) {
            elements[n1Ix + i] = elements[n2Ix + i];
            elements[n2Ix + i] = temp[i];
        }
    }

    private int getLeftChild(int ix) {
        int lix = (2 * ELEMENT_SIZE) * (ix / ELEMENT_SIZE) + ELEMENT_SIZE;
        return ((lix < nextIx) ? lix : -1);
    }

    private int getRightChild(int ix) {
        int rix = (2 * ELEMENT_SIZE) * (ix / ELEMENT_SIZE) + (2 * ELEMENT_SIZE);
        return ((rix < nextIx) ? rix : -1);
    }

    private int getParent(int ix) {
        if (ix <= 0) {
            return -1;
        }
        return ((ix - ELEMENT_SIZE) / (2 * ELEMENT_SIZE)) * ELEMENT_SIZE;
    }

    private ByteBuffer getFrame(int frameIx) {
        return (memMgr.getFrame(frameIx));
    }

    @Override
    public void getMax(int[] result) {
        throw new IllegalStateException("getMax() method not applicable to Min Heap");
    }

    @Override
    public void peekMax(int[] result) {
        throw new IllegalStateException("getMax() method not applicable to Min Heap");
    }
}