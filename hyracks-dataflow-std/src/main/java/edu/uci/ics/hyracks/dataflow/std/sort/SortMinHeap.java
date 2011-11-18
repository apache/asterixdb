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
 * 
 * @author pouria Implements a minimum binary heap, used as selection tree, for
 *         sort with replacement. This heap structure can only be used as the
 *         min heap (no access to the max element) Two elements in the heap are
 *         compared based on their run numbers, and sorting key(s):
 * 
 *         Considering two elements A and B:
 * 
 *         if RunNumber(A) > RunNumber(B) then A is larger than B if
 *         RunNumber(A) == RunNumber(B), then A is smaller than B, only if the
 *         value of the sort key(s) in B is greater than A (based on the sort
 *         comparator)
 * 
 */
public class SortMinHeap implements ISelectionTree {

	static final int RUN_ID_IX = 0;
	static final int FRAME_IX = 1;
	static final int OFFSET_IX = 2;
	final int PNK_IX = 3;

	private final int[] sortFields;
	private final IBinaryComparator[] comparators;
	private final RecordDescriptor recordDescriptor;
	private final FrameTupleAccessor fta1;
	private final FrameTupleAccessor fta2;

	List<int[]> tree;
	IMemoryManager memMgr;

	public SortMinHeap(IHyracksCommonContext ctx, int[] sortFields,
			IBinaryComparatorFactory[] comparatorFactories,
			RecordDescriptor recordDesc, IMemoryManager memMgr) {
		this.sortFields = sortFields;
		this.comparators = new IBinaryComparator[comparatorFactories.length];
		for (int i = 0; i < comparatorFactories.length; ++i) {
			this.comparators[i] = comparatorFactories[i]
					.createBinaryComparator();
		}
		this.recordDescriptor = recordDesc;
		fta1 = new FrameTupleAccessor(ctx.getFrameSize(), recordDescriptor);
		fta2 = new FrameTupleAccessor(ctx.getFrameSize(), recordDescriptor);
		this.memMgr = memMgr;

		this.tree = new ArrayList<int[]>();
	}

	/*
	 * Assumption (element structure): [RunId][FrameIx][Offset][Poorman NK]
	 */
	@Override
	public int[] getMin() {
		return (tree.size() > 0 ? delete(0) : new int[] { -1, -1, -1, -1 });
	}

	@Override
	public int[] peekMin() {
		if (tree.size() == 0) {
			return (new int[] { -1, -1, -1, -1 });
		}
		int[] top = tree.get(0);
		return new int[] { top[0], top[1], top[2], top[3] };
	}

	@Override
	public void insert(int[] e) {
		tree.add(e);
		siftUp(tree.size() - 1);
	}

	@Override
	public void reset() {
		this.tree.clear();
	}

	@Override
	public boolean isEmpty() {
		return (tree.size() < 1);
	}

	public int _debugGetSize() {
		return tree.size();
	}

	private int[] delete(int nix) {
		int[] nv = tree.get(nix);
		int[] last = tree.remove(tree.size() - 1);

		if (tree.size() > 0) {
			tree.set(nix, last);
		} else {
			return nv;
		}

		int pIx = getParent(nix);
		if (pIx > -1 && (compare(last, tree.get(pIx)) < 0)) {
			siftUp(nix);
		} else {
			siftDown(nix);
		}
		return nv;
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
			return ((((long) n1[PNK_IX]) & 0xffffffffL) < (((long) n2[PNK_IX]) & 0xffffffffL)) ? -1
					: 1;
		}

		return compare(getFrame(n1[FRAME_IX]), getFrame(n2[FRAME_IX]),
				n1[OFFSET_IX], n2[OFFSET_IX]);
	}

	private int compare(ByteBuffer fr1, ByteBuffer fr2, int r1StartOffset,
			int r2StartOffset) {
		byte[] b1 = fr1.array();
		byte[] b2 = fr2.array();
		fta1.reset(fr1);
		fta2.reset(fr2);
		int headerLen = BSTNodeUtil.HEADER_SIZE;
		r1StartOffset += headerLen;
		r2StartOffset += headerLen;
		for (int f = 0; f < comparators.length; ++f) {
			int fIdx = sortFields[f];
			int f1Start = fIdx == 0 ? 0 : fr1.getInt(r1StartOffset + (fIdx - 1)
					* 4);
			int f1End = fr1.getInt(r1StartOffset + fIdx * 4);
			int s1 = r1StartOffset + fta1.getFieldSlotsLength() + f1Start;
			int l1 = f1End - f1Start;
			int f2Start = fIdx == 0 ? 0 : fr2.getInt(r2StartOffset + (fIdx - 1)
					* 4);
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

	private void swap(int n1Ix, int n2Ix) {
		int[] temp = tree.get(n1Ix);
		tree.set(n1Ix, tree.get(n2Ix));
		tree.set(n2Ix, temp);
	}

	private int getLeftChild(int ix) {
		int lix = 2 * ix + 1;
		return ((lix < tree.size()) ? lix : -1);
	}

	private int getRightChild(int ix) {
		int rix = 2 * ix + 2;
		return ((rix < tree.size()) ? rix : -1);
	}

	private int getParent(int ix) {
		return ((ix - 1) / 2);
	}

	private ByteBuffer getFrame(int frameIx) {
		return (memMgr.getFrame(frameIx));
	}

	@Override
	public int[] getMax() {
		System.err.println("getMax() method not implemented for Min Heap");
		return null;
	}

	@Override
	public int[] peekMax() {
		System.err.println("peekMax() method not implemented for Min Heap");
		return null;
	}
}