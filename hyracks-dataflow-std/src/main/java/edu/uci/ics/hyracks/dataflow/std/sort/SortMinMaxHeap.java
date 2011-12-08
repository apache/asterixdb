package edu.uci.ics.hyracks.dataflow.std.sort;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.hyracks.api.context.IHyracksCommonContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

/**
 * @author pouria
 * 
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

    private final int[] sortFields;
    private final IBinaryComparator[] comparators;
    private final RecordDescriptor recordDescriptor;
    private final FrameTupleAccessor fta1;
    private final FrameTupleAccessor fta2;

    List<int[]> tree;
    IMemoryManager memMgr;

    int[] top; // Used as the temp variable to access the top, to avoid object
               // creation

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

        this.tree = new ArrayList<int[]>();
    }

    @Override
    public void insert(int[] element) {
        tree.add(element);
        bubbleUp(tree.size() - 1);
    }

    @Override
    public void getMin(int[] result) {
        if (tree.size() == 0) {
            result[0] = result[1] = result[2] = result[3] = -1;
            return;
        }

        top = delete(0);
        for (int i = 0; i < top.length; i++) {
            result[i] = top[i];
        }
    }

    @Override
    public void reset() {
        this.tree.clear();
    }

    @Override
    public boolean isEmpty() {
        return (tree.size() < 1);
    }

    @Override
    public void peekMin(int[] result) {
        if (tree.size() == 0) {
            result[0] = result[1] = result[2] = result[3] = -1;
            return;
        }

        top = tree.get(0);
        for (int i = 0; i < top.length; i++) {
            result[i] = top[i];
        }
    }

    @Override
    public void getMax(int[] result) {
        if (tree.size() == 1) {
            top = tree.remove(0);
            for (int i = 0; i < top.length; i++) {
                result[i] = top[i];
            }
            return;
        }

        if (tree.size() > 1) {
            int lc = getLeftChild(0);
            int rc = getRightChild(0);

            if (rc == -1) {
                top = delete(lc);
            } else {
                top = (compare(lc, rc) < 0) ? delete(rc) : delete(lc);
            }

            for (int i = 0; i < top.length; i++) {
                result[i] = top[i];
            }
            return;

        }
        result[0] = result[1] = result[2] = result[3] = -1;
    }

    @Override
    public void peekMax(int[] result) {
        if (tree.size() == 1) {
            top = tree.get(0);
            for (int i = 0; i < top.length; i++) {
                result[i] = top[i];
            }
            return;
        }
        if (tree.size() > 1) {
            int lc = getLeftChild(0);
            int rc = getRightChild(0);

            if (rc == -1) {
                top = tree.get(lc);
            } else {
                top = (compare(lc, rc) < 0) ? tree.get(rc) : tree.get(lc);
            }

            for (int i = 0; i < top.length; i++) {
                result[i] = top[i];
            }
            return;
        }
        result[0] = result[1] = result[2] = result[3] = -1;
    }

    private int[] delete(int delIx) {
        int s = tree.size();
        if (s > 1) {
            int[] delEntry = tree.get(delIx);
            int[] last = (tree.remove(s - 1));
            if (delIx != tree.size()) {
                tree.set(delIx, last);
                trickleDown(delIx);
            }
            return delEntry;
        } else if (s == 1) {
            return tree.remove(0);
        }
        return null;
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
        int[] temp = tree.get(n1Ix);
        tree.set(n1Ix, tree.get(n2Ix));
        tree.set(n2Ix, temp);
    }

    private int getParentIx(int i) {
        if (i == 0) {
            return NOT_EXIST;
        }
        return (i - 1) / 2;
    }

    private int getGrandParent(int i) {
        int p = getParentIx(i);
        return p != -1 ? getParentIx(p) : NOT_EXIST;
    }

    private int getLeftChild(int i) {
        int lc = 2 * i + 1;
        return lc < tree.size() ? lc : NOT_EXIST;
    }

    private int[] getLeftGrandChildren(int i) {
        int lc = getLeftChild(i);
        return lc != NOT_EXIST ? new int[] { getLeftChild(lc), getRightChild(lc) } : new int[] { NOT_EXIST, NOT_EXIST };
    }

    private int getRightChild(int i) {
        int rc = 2 * i + 2;
        return rc < tree.size() ? rc : -1;
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
        if (i == 0) {
            return 0;
        }
        int l = (int) Math.floor(Math.log(i) / Math.log(2));
        if (i == (((int) Math.pow(2, (l + 1))) - 1)) {
            return (l + 1);
        }
        return l;
    }

    private ByteBuffer getFrame(int frameIx) {
        return (memMgr.getFrame(frameIx));
    }

    // first < sec : -1
    private int compare(int nodeSIx1, int nodeSIx2) {
        int[] n1 = tree.get(nodeSIx1);
        int[] n2 = tree.get(nodeSIx2);
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

    public String _debugPrintTree() {
        String s = "";
        for (int i = 0; i < tree.size(); i++) {
            int[] n = tree.get(i);
            s += "\t[" + i + "](" + n[RUN_ID_IX] + ", " + n[FRAME_IX] + ", " + n[OFFSET_IX] + "), ";
        }
        return s;
    }
}