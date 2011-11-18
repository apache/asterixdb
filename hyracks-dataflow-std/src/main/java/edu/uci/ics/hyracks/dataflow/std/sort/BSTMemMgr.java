package edu.uci.ics.hyracks.dataflow.std.sort;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksCommonContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;

/**
 * @author pouria Memory Manager based on Binary Search Tree (BST) Free slot
 *         size is the key for the BST nodes. Each node in BST shows a class of
 *         free slots, while all the free slots with the exact same length are
 *         stored as a LinkedList, whose head is a BST node. BST is not stored
 *         as a separate data structure, but the actual free spaces of the
 *         memory slots are used to hold BST nodes. Each BST node has the
 *         logical structure, defined in the BSTNodeUtil class.
 * 
 */
public class BSTMemMgr implements IMemoryManager {

	private final IHyracksCommonContext ctx;
	public static int FRAME_SIZE;

	private ByteBuffer[] frames;
	private ByteBuffer convertBuffer;
	private Slot root;
	private Slot result; // A reusable object to hold one node returned as
							// methods result
	private Slot insertSlot; // A reusable object to hold one node in the insert
								// process
	private Slot[] par_res;
	private int lastFrame;

	private static int _debug_free_slots = 0;
	private static int _debug_tree_size = 0;
	private int _debug_total_lookup_steps;
	private int _debug_total_lookup_counts;
	private int _debug_depth_counter;
	private int _debug_max_depth;

	public BSTMemMgr(IHyracksCommonContext ctx, int memSize) {
		this.ctx = ctx;
		FRAME_SIZE = ctx.getFrameSize();
		convertBuffer = ByteBuffer.allocate(4);
		frames = new ByteBuffer[memSize];
		lastFrame = -1;
		root = new Slot();
		insertSlot = new Slot();
		result = new Slot();
		par_res = new Slot[] { new Slot(), new Slot() };
		_debug_total_lookup_counts = 0;
		_debug_total_lookup_steps = 0;
	}

	/**
	 * result is the container sent by the caller to hold the results
	 */
	@Override
	public void allocate(int length, Slot result) throws HyracksDataException {
		_debug_total_lookup_counts++;
		search(length, par_res);
		if (par_res[1].isNull()) {
			addFrame(par_res);
			if (par_res[1].isNull()) {
				return;
			}
		}

		int sl = BSTNodeUtil.getLength(par_res[1], frames, convertBuffer);
		int acLen = BSTNodeUtil.getActualLength(length);
		if (shouldSplit(sl, acLen)) {
			int[] s = split(par_res[1], par_res[0], acLen);
			int insertLen = BSTNodeUtil.getLength(s[2], s[3], frames,
					convertBuffer);
			insert(s[2], s[3], insertLen); // inserting second half of the split
											// slot
			BSTNodeUtil.setHeaderFooter(s[0], s[1], length, false, frames);
			result.set(s[0], s[1]);
			return;
		}
		allocate(par_res[1], par_res[0], length, result);
	}

	@Override
	public int unallocate(Slot s) throws HyracksDataException {
		int usedLen = BSTNodeUtil.getLength(s, frames, convertBuffer);
		int actualLen = BSTNodeUtil.getActualLength(usedLen);
		int fix = s.getFrameIx();
		int off = s.getOffset();

		int prevMemSlotFooter_off = ((off - BSTNodeUtil.HEADER_SIZE) >= 0 ? (off - BSTNodeUtil.HEADER_SIZE)
				: BSTNodeUtil.INVALID_INDEX);
		int t = off + 2 * BSTNodeUtil.HEADER_SIZE + actualLen;
		int nextMemSlotHeader_off = (t < FRAME_SIZE ? t
				: BSTNodeUtil.INVALID_INDEX);
		// Remember: next and prev memory slots have the same frame index as the
		// unallocating slot
		if (!isNodeNull(fix, prevMemSlotFooter_off)
				&& BSTNodeUtil.isFree(fix, prevMemSlotFooter_off, frames)) {
			int leftLength = BSTNodeUtil.getLength(fix, prevMemSlotFooter_off,
					frames, convertBuffer);
			removeFromList(fix, prevMemSlotFooter_off - leftLength
					- BSTNodeUtil.HEADER_SIZE);
			int concatLength = actualLen + leftLength + 2
					* BSTNodeUtil.HEADER_SIZE;
			if (!isNodeNull(fix, nextMemSlotHeader_off)
					&& BSTNodeUtil.isFree(fix, nextMemSlotHeader_off, frames)) {
				removeFromList(fix, nextMemSlotHeader_off);
				concatLength += BSTNodeUtil.getLength(fix,
						nextMemSlotHeader_off, frames, convertBuffer)
						+ 2
						* BSTNodeUtil.HEADER_SIZE;
			}
			insert(fix, prevMemSlotFooter_off - leftLength
					- BSTNodeUtil.HEADER_SIZE, concatLength); // newly (merged)
																// slot starts
																// at the prev
																// slot offset
			return concatLength;

		} else if (!isNodeNull(fix, nextMemSlotHeader_off)
				&& BSTNodeUtil.isFree(fix, nextMemSlotHeader_off, frames)) {
			removeFromList(fix, nextMemSlotHeader_off);
			int concatLength = actualLen
					+ BSTNodeUtil.getLength(fix, nextMemSlotHeader_off, frames,
							convertBuffer) + 2 * BSTNodeUtil.HEADER_SIZE;
			insert(fix, off, concatLength); // newly (merged) slot starts at the
											// unallocating slot offset
			return concatLength;
		}
		// unallocating slot is not merging with any neighbor
		insert(fix, off, actualLen);
		return actualLen;
	}

	@Override
	public boolean readTuple(int frameIx, int offset, FrameTupleAppender dest) {
		int offToRead = offset + BSTNodeUtil.HEADER_SIZE;
		int length = BSTNodeUtil.getLength(frameIx, offset, frames,
				convertBuffer);
		return dest.append(frames[frameIx].array(), offToRead, length);
	}

	@Override
	public boolean writeTuple(int frameIx, int offset, FrameTupleAccessor src,
			int tIndex) {
		int offToCopy = offset + BSTNodeUtil.HEADER_SIZE;
		int tStartOffset = src.getTupleStartOffset(tIndex);
		int tEndOffset = src.getTupleEndOffset(tIndex);
		int tupleLength = tEndOffset - tStartOffset;
		ByteBuffer srcBuffer = src.getBuffer();
		System.arraycopy(srcBuffer.array(), tStartOffset,
				frames[frameIx].array(), offToCopy, tupleLength);
		return true;
	}

	@Override
	public ByteBuffer getFrame(int frameIndex) {
		return frames[frameIndex];
	}

	public String _debug_getAvgSearchPath() {
		double avg = (((double) _debug_total_lookup_steps) / ((double) _debug_total_lookup_counts));
		return "\nTotal allocation requests:\t" + _debug_total_lookup_counts
				+ "\nAvg Allocation Path Length:\t" + avg
				+ "\nMax BST Depth:\t" + _debug_max_depth;
	}

	public void _debug_decLookupCount() {
		_debug_total_lookup_counts--;
	}

	/**
	 * 
	 * @param p_r
	 *            is the container passed by the caller to contain the results
	 * @throws HyracksDataException
	 */
	private void addFrame(Slot[] p_r) throws HyracksDataException {
		_debug_depth_counter = 0;
		clear(p_r);
		if ((lastFrame + 1) >= frames.length) {
			return;
		}
		frames[++lastFrame] = allocateFrame();
		int l = FRAME_SIZE - 2 * BSTNodeUtil.HEADER_SIZE;
		BSTNodeUtil.setHeaderFooter(lastFrame, 0, l, true, frames);
		initNewNode(lastFrame, 0);

		p_r[1].copy(root);
		if (p_r[1].isNull()) { // root is null
			root.set(lastFrame, 0);
			initNewNode(root.getFrameIx(), root.getOffset());
			p_r[1].copy(root);
			return;
		}

		while (!p_r[1].isNull()) {
			_debug_depth_counter++;
			if (BSTNodeUtil.getLength(p_r[1], frames, convertBuffer) == l) {
				append(p_r[1].getFrameIx(), p_r[1].getOffset(), lastFrame, 0);
				p_r[1].set(lastFrame, 0);
				if (_debug_depth_counter > _debug_max_depth) {
					_debug_max_depth = _debug_depth_counter;
				}
				return;
			}
			if (l < BSTNodeUtil.getLength(p_r[1], frames, convertBuffer)) {
				if (isNodeNull(BSTNodeUtil.leftChild_fIx(p_r[1], frames,
						convertBuffer), BSTNodeUtil.leftChild_offset(p_r[1],
						frames, convertBuffer))) {
					BSTNodeUtil.setLeftChild(p_r[1].getFrameIx(),
							p_r[1].getOffset(), lastFrame, 0, frames);
					p_r[0].copy(p_r[1]);
					p_r[1].set(lastFrame, 0);
					if (_debug_depth_counter > _debug_max_depth) {
						_debug_max_depth = _debug_depth_counter;
					}
					return;
				} else {
					p_r[0].copy(p_r[1]);
					p_r[1].set(BSTNodeUtil.leftChild_fIx(p_r[1], frames,
							convertBuffer), BSTNodeUtil.leftChild_offset(
							p_r[1], frames, convertBuffer));
				}
			} else {
				if (isNodeNull(BSTNodeUtil.rightChild_fIx(p_r[1], frames,
						convertBuffer), BSTNodeUtil.rightChild_offset(p_r[1],
						frames, convertBuffer))) {
					BSTNodeUtil.setRightChild(p_r[1].getFrameIx(),
							p_r[1].getOffset(), lastFrame, 0, frames);
					p_r[0].copy(p_r[1]);
					p_r[1].set(lastFrame, 0);
					if (_debug_depth_counter > _debug_max_depth) {
						_debug_max_depth = _debug_depth_counter;
					}
					return;
				} else {
					p_r[0].copy(p_r[1]);
					p_r[1].set(BSTNodeUtil.rightChild_fIx(p_r[1], frames,
							convertBuffer), BSTNodeUtil.rightChild_offset(
							p_r[1], frames, convertBuffer));
				}
			}
		}
		throw new HyracksDataException(
				"New Frame could not be added to BSTMemMgr");
	}

	private void insert(int fix, int off, int length)
			throws HyracksDataException {
		_debug_depth_counter = 0;
		BSTNodeUtil.setHeaderFooter(fix, off, length, true, frames);
		initNewNode(fix, off);

		if (root.isNull()) {
			root.set(fix, off);
			return;
		}

		insertSlot.clear();
		insertSlot.copy(root);
		while (!insertSlot.isNull()) {
			_debug_depth_counter++;
			int curSlotLen = BSTNodeUtil.getLength(insertSlot, frames,
					convertBuffer);
			if (curSlotLen == length) {
				append(insertSlot.getFrameIx(), insertSlot.getOffset(), fix,
						off);
				if (_debug_depth_counter > _debug_max_depth) {
					_debug_max_depth = _debug_depth_counter;
				}
				return;
			}
			if (length < curSlotLen) {
				int lc_fix = BSTNodeUtil.leftChild_fIx(insertSlot, frames,
						convertBuffer);
				int lc_off = BSTNodeUtil.leftChild_offset(insertSlot, frames,
						convertBuffer);
				if (isNodeNull(lc_fix, lc_off)) {
					initNewNode(fix, off);
					BSTNodeUtil.setLeftChild(insertSlot.getFrameIx(),
							insertSlot.getOffset(), fix, off, frames);
					if (_debug_depth_counter > _debug_max_depth) {
						_debug_max_depth = _debug_depth_counter;
					}
					return;
				} else {
					insertSlot.set(lc_fix, lc_off);
				}
			} else {
				int rc_fix = BSTNodeUtil.rightChild_fIx(insertSlot, frames,
						convertBuffer);
				int rc_off = BSTNodeUtil.rightChild_offset(insertSlot, frames,
						convertBuffer);
				if (isNodeNull(rc_fix, rc_off)) {
					initNewNode(fix, off);
					BSTNodeUtil.setRightChild(insertSlot.getFrameIx(),
							insertSlot.getOffset(), fix, off, frames);
					if (_debug_depth_counter > _debug_max_depth) {
						_debug_max_depth = _debug_depth_counter;
					}
					return;
				} else {
					insertSlot.set(rc_fix, rc_off);
				}
			}
		}
		throw new HyracksDataException(
				"Failure in node insertion into BST in BSTMemMgr");
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

		Slot lastLeftParent = new Slot();
		Slot lastLeft = new Slot();
		Slot parent = new Slot();
		result.copy(root);

		while (!result.isNull()) {
			_debug_total_lookup_steps++;
			if (BSTNodeUtil.getLength(result, frames, convertBuffer) == length) {
				target[0].copy(parent);
				target[1].copy(result);
				return;
			}
			if (length < BSTNodeUtil.getLength(result, frames, convertBuffer)) {
				lastLeftParent.copy(parent);
				lastLeft.copy(result);
				parent.copy(result);
				int fix = BSTNodeUtil.leftChild_fIx(result, frames,
						convertBuffer);
				int off = BSTNodeUtil.leftChild_offset(result, frames,
						convertBuffer);
				result.set(fix, off);
			} else {
				parent.copy(result);
				int fix = BSTNodeUtil.rightChild_fIx(result, frames,
						convertBuffer);
				int off = BSTNodeUtil.rightChild_offset(result, frames,
						convertBuffer);
				result.set(fix, off);
			}
		}

		target[0].copy(lastLeftParent);
		target[1].copy(lastLeft);

	}

	private void append(int headFix, int headOff, int nodeFix, int nodeOff) {
		initNewNode(nodeFix, nodeOff);

		int fix = BSTNodeUtil.next_fIx(headFix, headOff, frames, convertBuffer); // frameIx
																					// for
																					// the
																					// current
																					// next
																					// of
																					// head
		int off = BSTNodeUtil.next_offset(headFix, headOff, frames,
				convertBuffer); // offset for the current next of head
		BSTNodeUtil.setNext(nodeFix, nodeOff, fix, off, frames);

		if (!isNodeNull(fix, off)) {
			BSTNodeUtil.setPrev(fix, off, nodeFix, nodeOff, frames);
		}
		BSTNodeUtil.setPrev(nodeFix, nodeOff, headFix, headOff, frames);
		BSTNodeUtil.setNext(headFix, headOff, nodeFix, nodeOff, frames);
	}

	private int[] split(Slot listHead, Slot parent, int length) {
		int l2 = BSTNodeUtil.getLength(listHead, frames, convertBuffer)
				- length - 2 * BSTNodeUtil.HEADER_SIZE;
		// We split the node after slots-list head
		if (!isNodeNull(BSTNodeUtil.next_fIx(listHead, frames, convertBuffer),
				BSTNodeUtil.next_offset(listHead, frames, convertBuffer))) {
			int afterHeadFix = BSTNodeUtil.next_fIx(listHead, frames,
					convertBuffer);
			int afterHeadOff = BSTNodeUtil.next_offset(listHead, frames,
					convertBuffer);
			int afHNextFix = BSTNodeUtil.next_fIx(afterHeadFix, afterHeadOff,
					frames, convertBuffer);
			int afHNextOff = BSTNodeUtil.next_offset(afterHeadFix,
					afterHeadOff, frames, convertBuffer);
			BSTNodeUtil.setNext(listHead.getFrameIx(), listHead.getOffset(),
					afHNextFix, afHNextOff, frames);
			if (!isNodeNull(afHNextFix, afHNextOff)) {
				BSTNodeUtil.setPrev(afHNextFix, afHNextOff,
						listHead.getFrameIx(), listHead.getOffset(), frames);
			}
			int secondOffset = afterHeadOff + length + 2
					* BSTNodeUtil.HEADER_SIZE;
			BSTNodeUtil.setHeaderFooter(afterHeadFix, afterHeadOff, length,
					true, frames);
			BSTNodeUtil.setHeaderFooter(afterHeadFix, secondOffset, l2, true,
					frames);

			return new int[] { afterHeadFix, afterHeadOff, afterHeadFix,
					secondOffset };
		}
		// We split the head
		int secondOffset = listHead.getOffset() + length + 2
				* BSTNodeUtil.HEADER_SIZE;
		BSTNodeUtil.setHeaderFooter(listHead.getFrameIx(),
				listHead.getOffset(), length, true, frames);
		BSTNodeUtil.setHeaderFooter(listHead.getFrameIx(), secondOffset, l2,
				true, frames);

		fixTreePtrs(listHead.getFrameIx(), listHead.getOffset(),
				parent.getFrameIx(), parent.getOffset());
		return new int[] { listHead.getFrameIx(), listHead.getOffset(),
				listHead.getFrameIx(), secondOffset };
	}

	private void fixTreePtrs(int node_fix, int node_off, int par_fix,
			int par_off) {
		int nlc_fix = BSTNodeUtil.leftChild_fIx(node_fix, node_off, frames,
				convertBuffer);
		int nlc_off = BSTNodeUtil.leftChild_offset(node_fix, node_off, frames,
				convertBuffer);
		int nrc_fix = BSTNodeUtil.rightChild_fIx(node_fix, node_off, frames,
				convertBuffer);
		int nrc_off = BSTNodeUtil.rightChild_offset(node_fix, node_off, frames,
				convertBuffer);

		int status = -1; // (status==0 if node is left child of parent)
							// (status==1 if node is right child of parent)
		if (!isNodeNull(par_fix, par_off)) {
			int nlen = BSTNodeUtil.getActualLength(BSTNodeUtil.getLength(
					node_fix, node_off, frames, convertBuffer));
			int plen = BSTNodeUtil.getActualLength(BSTNodeUtil.getLength(
					par_fix, par_off, frames, convertBuffer));
			status = ((nlen < plen) ? 0 : 1);
		}

		if (!isNodeNull(nlc_fix, nlc_off) && !isNodeNull(nrc_fix, nrc_off)) { // Node
																				// has
																				// two
																				// children
			int pmin_fix = node_fix;
			int pmin_off = node_off;
			int min_fix = nrc_fix;
			int min_off = nrc_off;
			int next_left_fix = BSTNodeUtil.leftChild_fIx(min_fix, min_off,
					frames, convertBuffer);
			int next_left_off = BSTNodeUtil.leftChild_offset(min_fix, min_off,
					frames, convertBuffer);

			while (!isNodeNull(next_left_fix, next_left_off)) {
				pmin_fix = min_fix;
				pmin_off = min_off;
				min_fix = next_left_fix;
				min_off = next_left_off;
				next_left_fix = BSTNodeUtil.leftChild_fIx(min_fix, min_off,
						frames, convertBuffer); // min is now pointing to
												// current (old) next left
				next_left_off = BSTNodeUtil.leftChild_offset(min_fix, min_off,
						frames, convertBuffer); // min is now pointing to
												// current (old) next left
			}

			if ((nrc_fix == min_fix) && (nrc_off == min_off)) { // nrc is the
																// same as min
				BSTNodeUtil.setLeftChild(nrc_fix, nrc_off, nlc_fix, nlc_off,
						frames);
			} else { // min is different from nrc
				int min_right_fix = BSTNodeUtil.rightChild_fIx(min_fix,
						min_off, frames, convertBuffer);
				int min_right_off = BSTNodeUtil.rightChild_offset(min_fix,
						min_off, frames, convertBuffer);
				BSTNodeUtil.setRightChild(min_fix, min_off, nrc_fix, nrc_off,
						frames);
				BSTNodeUtil.setLeftChild(min_fix, min_off, nlc_fix, nlc_off,
						frames);
				BSTNodeUtil.setLeftChild(pmin_fix, pmin_off, min_right_fix,
						min_right_off, frames);
			}

			// Now dealing with the parent
			if (!isNodeNull(par_fix, par_off)) {
				if (status == 0) {
					BSTNodeUtil.setLeftChild(par_fix, par_off, min_fix,
							min_off, frames);
				} else if (status == 1) {
					BSTNodeUtil.setRightChild(par_fix, par_off, min_fix,
							min_off, frames);
				}
			} else { // No parent (node was the root)
				root.set(min_fix, min_off);
			}
			return;
		}

		else if (!isNodeNull(nlc_fix, nlc_off)) { // Node has only left child
			if (status == 0) {
				BSTNodeUtil.setLeftChild(par_fix, par_off, nlc_fix, nlc_off,
						frames);
			} else if (status == 1) {
				BSTNodeUtil.setRightChild(par_fix, par_off, nlc_fix, nlc_off,
						frames);
			} else if (status == -1) { // No parent, so node is root
				root.set(nlc_fix, nlc_off);
			}
			return;
		}

		else if (!isNodeNull(nrc_fix, nrc_off)) { // Node has only right child
			if (status == 0) {
				BSTNodeUtil.setLeftChild(par_fix, par_off, nrc_fix, nrc_off,
						frames);
			} else if (status == 1) {
				BSTNodeUtil.setRightChild(par_fix, par_off, nrc_fix, nrc_off,
						frames);
			} else if (status == -1) { // No parent, so node is root
				root.set(nrc_fix, nrc_off);
			}
			return;
		}

		else { // Node is leaf (no children)
			if (status == 0) {
				BSTNodeUtil.setLeftChild(par_fix, par_off,
						BSTNodeUtil.INVALID_INDEX, BSTNodeUtil.INVALID_INDEX,
						frames);
			} else if (status == 1) {
				BSTNodeUtil.setRightChild(par_fix, par_off,
						BSTNodeUtil.INVALID_INDEX, BSTNodeUtil.INVALID_INDEX,
						frames);
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
		int nextFix = BSTNodeUtil.next_fIx(node, frames, convertBuffer);
		int nextOff = BSTNodeUtil.next_offset(node, frames, convertBuffer);
		if (!isNodeNull(nextFix, nextOff)) {
			int nextOfNext_fix = BSTNodeUtil.next_fIx(nextFix, nextOff, frames,
					convertBuffer);
			int nextOfNext_off = BSTNodeUtil.next_offset(nextFix, nextOff,
					frames, convertBuffer);
			BSTNodeUtil.setNext(node.getFrameIx(), node.getOffset(),
					nextOfNext_fix, nextOfNext_off, frames);
			if (!isNodeNull(nextOfNext_fix, nextOfNext_off)) {
				BSTNodeUtil.setPrev(nextOfNext_fix, nextOfNext_off,
						node.getFrameIx(), node.getOffset(), frames);
			}
			BSTNodeUtil
					.setHeaderFooter(nextFix, nextOff, length, false, frames);
			result.set(nextFix, nextOff);
			return;
		}

		fixTreePtrs(node.getFrameIx(), node.getOffset(), parent.getFrameIx(),
				parent.getOffset());
		BSTNodeUtil.setHeaderFooter(node.getFrameIx(), node.getOffset(),
				length, false, frames);
		result.copy(node);
	}

	private void removeFromList(int fix, int off) {
		int nx_fix = BSTNodeUtil.next_fIx(fix, off, frames, convertBuffer);
		int nx_off = BSTNodeUtil.next_offset(fix, off, frames, convertBuffer);
		int prev_fix = BSTNodeUtil.prev_fIx(fix, off, frames, convertBuffer);
		int prev_off = BSTNodeUtil.prev_offset(fix, off, frames, convertBuffer);
		if (!isNodeNull(prev_fix, prev_off) && !isNodeNull(nx_fix, nx_off)) {
			BSTNodeUtil.setNext(prev_fix, prev_off, nx_fix, nx_off, frames);
			BSTNodeUtil.setPrev(nx_fix, nx_off, prev_fix, prev_off, frames);
			BSTNodeUtil.setNext(fix, off, BSTNodeUtil.INVALID_INDEX,
					BSTNodeUtil.INVALID_INDEX, frames);
			BSTNodeUtil.setPrev(fix, off, BSTNodeUtil.INVALID_INDEX,
					BSTNodeUtil.INVALID_INDEX, frames);
			return;
		}
		if (!isNodeNull(prev_fix, prev_off)) {
			BSTNodeUtil.setNext(prev_fix, prev_off, BSTNodeUtil.INVALID_INDEX,
					BSTNodeUtil.INVALID_INDEX, frames);
			BSTNodeUtil.setPrev(fix, off, BSTNodeUtil.INVALID_INDEX,
					BSTNodeUtil.INVALID_INDEX, frames);
			return;
		}

		// We need to find the parent, so we can fix the tree
		int par_fix = BSTNodeUtil.INVALID_INDEX;
		int par_off = BSTNodeUtil.INVALID_INDEX;
		int length = BSTNodeUtil.getActualLength(BSTNodeUtil.getLength(fix,
				off, frames, convertBuffer));
		fix = root.getFrameIx();
		off = root.getOffset();
		int curLen = BSTNodeUtil.getLength(fix, off, frames, convertBuffer);
		while (length != curLen) {
			par_fix = fix;
			par_off = off;
			if (length < curLen) {
				fix = BSTNodeUtil.leftChild_fIx(par_fix, par_off, frames,
						convertBuffer); // par_fix is now the old(current) fix
				off = BSTNodeUtil.leftChild_offset(par_fix, par_off, frames,
						convertBuffer); // par_off is now the old(current) off
			} else {
				fix = BSTNodeUtil.rightChild_fIx(par_fix, par_off, frames,
						convertBuffer); // par_fix is now the old(current) fix
				off = BSTNodeUtil.rightChild_offset(par_fix, par_off, frames,
						convertBuffer); // par_off is now the old(current) off
			}
			curLen = BSTNodeUtil.getLength(fix, off, frames, convertBuffer);
		}

		if (!isNodeNull(nx_fix, nx_off)) { // it is head of the list (in the
											// tree)
			BSTNodeUtil.setPrev(nx_fix, nx_off, BSTNodeUtil.INVALID_INDEX,
					BSTNodeUtil.INVALID_INDEX, frames);
			int node_lc_fix = BSTNodeUtil.leftChild_fIx(fix, off, frames,
					convertBuffer);
			int node_lc_off = BSTNodeUtil.leftChild_offset(fix, off, frames,
					convertBuffer);
			int node_rc_fix = BSTNodeUtil.rightChild_fIx(fix, off, frames,
					convertBuffer);
			int node_rc_off = BSTNodeUtil.rightChild_offset(fix, off, frames,
					convertBuffer);
			BSTNodeUtil.setLeftChild(nx_fix, nx_off, node_lc_fix, node_lc_off,
					frames);
			BSTNodeUtil.setRightChild(nx_fix, nx_off, node_rc_fix, node_rc_off,
					frames);
			if (!isNodeNull(par_fix, par_off)) {
				int parentLength = BSTNodeUtil.getLength(par_fix, par_off,
						frames, convertBuffer);
				if (length < parentLength) {
					BSTNodeUtil.setLeftChild(par_fix, par_off, nx_fix, nx_off,
							frames);
				} else {
					BSTNodeUtil.setRightChild(par_fix, par_off, nx_fix, nx_off,
							frames);
				}
			}

			if ((root.getFrameIx() == fix) && (root.getOffset() == off)) {
				root.set(nx_fix, nx_off);
			}

			return;
		}

		fixTreePtrs(fix, off, par_fix, par_off);
	}

	private void clear(Slot[] s) {
		s[0].clear();
		s[1].clear();
	}

	private boolean isNodeNull(int frameIx, int offset) {
		return ((frameIx == BSTNodeUtil.INVALID_INDEX)
				|| (offset == BSTNodeUtil.INVALID_INDEX) || (frames[frameIx] == null));
	}

	private boolean shouldSplit(int slotLength, int reqLength) {
		return ((slotLength - reqLength) >= BSTNodeUtil.MINIMUM_FREE_SLOT_SIZE);
	}

	private void initNewNode(int frameIx, int offset) {
		BSTNodeUtil.setLeftChild(frameIx, offset, BSTNodeUtil.INVALID_INDEX,
				BSTNodeUtil.INVALID_INDEX, frames);
		BSTNodeUtil.setRightChild(frameIx, offset, BSTNodeUtil.INVALID_INDEX,
				BSTNodeUtil.INVALID_INDEX, frames);
		BSTNodeUtil.setNext(frameIx, offset, BSTNodeUtil.INVALID_INDEX,
				BSTNodeUtil.INVALID_INDEX, frames);
		BSTNodeUtil.setPrev(frameIx, offset, BSTNodeUtil.INVALID_INDEX,
				BSTNodeUtil.INVALID_INDEX, frames);
	}

	private ByteBuffer allocateFrame() {
		return ctx.allocateFrame();
	}

	public String _debug_printMemory() {
		_debug_free_slots = 0;
		Slot s = new Slot(0, 0);
		if (s.isNull()) {
			return "memory:\tNull";
		}

		if (BSTNodeUtil.isFree(0, 0, frames)) {
			_debug_free_slots++;
		}

		String m = "memory:\n" + _debug_printSlot(0, 0) + "\n";
		int length = BSTNodeUtil.getActualLength(BSTNodeUtil.getLength(0, 0,
				frames, convertBuffer));
		int noff = (length + 2 * BSTNodeUtil.HEADER_SIZE >= FRAME_SIZE ? BSTNodeUtil.INVALID_INDEX
				: length + 2 * BSTNodeUtil.HEADER_SIZE);
		int nfix = (noff == BSTNodeUtil.INVALID_INDEX ? ((frames.length == 1) ? BSTNodeUtil.INVALID_INDEX
				: 1)
				: 0);
		if (noff == BSTNodeUtil.INVALID_INDEX
				&& nfix != BSTNodeUtil.INVALID_INDEX) {
			noff = 0;
		}
		s.set(nfix, noff);
		while (!isNodeNull(s.getFrameIx(), s.getOffset())) {
			if (BSTNodeUtil.isFree(s.getFrameIx(), s.getOffset(), frames)) {
				_debug_free_slots++;
			}
			m += _debug_printSlot(s.getFrameIx(), s.getOffset()) + "\n";
			length = BSTNodeUtil.getActualLength(BSTNodeUtil.getLength(
					s.getFrameIx(), s.getOffset(), frames, convertBuffer));
			noff = (s.getOffset() + length + 2 * BSTNodeUtil.HEADER_SIZE >= FRAME_SIZE ? BSTNodeUtil.INVALID_INDEX
					: s.getOffset() + length + 2 * BSTNodeUtil.HEADER_SIZE);
			nfix = (noff == BSTNodeUtil.INVALID_INDEX ? ((frames.length - 1 == s
					.getFrameIx()) ? BSTNodeUtil.INVALID_INDEX
					: s.getFrameIx() + 1) : s.getFrameIx());
			if (noff == BSTNodeUtil.INVALID_INDEX
					&& nfix != BSTNodeUtil.INVALID_INDEX) {
				noff = 0;
			}
			s.set(nfix, noff);
		}
		return m + "\nFree Slots:\t" + _debug_free_slots;
	}

	public String _debug_printTree() {
		_debug_tree_size = 0;
		Slot node = new Slot();
		node.copy(root);
		if (!node.isNull()) {
			_debug_tree_size++;
			return _debug_printSubTree(node) + "\nTree Nodes:\t"
					+ _debug_tree_size;
		}
		return "Null";
	}

	private String _debug_printSubTree(Slot r) {
		Slot node = new Slot();
		node.copy(r);
		int fix = node.getFrameIx();
		int off = node.getOffset();
		int lfix = BSTNodeUtil.leftChild_fIx(node, frames, convertBuffer);
		int loff = BSTNodeUtil.leftChild_offset(node, frames, convertBuffer);
		int rfix = BSTNodeUtil.rightChild_fIx(node, frames, convertBuffer);
		int roff = BSTNodeUtil.rightChild_offset(node, frames, convertBuffer);
		int nfix = BSTNodeUtil.next_fIx(node, frames, convertBuffer);
		int noff = BSTNodeUtil.next_offset(node, frames, convertBuffer);
		int pfix = BSTNodeUtil.prev_fIx(node, frames, convertBuffer);
		int poff = BSTNodeUtil.prev_offset(node, frames, convertBuffer);

		String s = "{" + r.getFrameIx() + ", " + r.getOffset() + " (Len: "
				+ BSTNodeUtil.getLength(fix, off, frames, convertBuffer)
				+ ") - " + "(LC: " + _debug_printSlot(lfix, loff) + ") - "
				+ "(RC: " + _debug_printSlot(rfix, roff) + ") - " + "(NX: "
				+ _debug_printSlot(nfix, noff) + ") - " + "(PR: "
				+ _debug_printSlot(pfix, poff) + ")  }\n";
		if (!isNodeNull(lfix, loff)) {
			_debug_tree_size++;
			s += _debug_printSubTree(new Slot(lfix, loff)) + "\n";
		}
		if (!isNodeNull(rfix, roff)) {
			_debug_tree_size++;
			s += _debug_printSubTree(new Slot(rfix, roff)) + "\n";
		}

		return s;
	}

	private String _debug_printSlot(int fix, int off) {
		if (isNodeNull(fix, off)) {
			return BSTNodeUtil.INVALID_INDEX + ", " + BSTNodeUtil.INVALID_INDEX;
		}
		int l = BSTNodeUtil.getLength(fix, off, frames, convertBuffer);
		int al = BSTNodeUtil.getActualLength(l);
		boolean f = BSTNodeUtil.isFree(fix, off, frames);
		return fix + ", " + off + " (free: " + f + ") (Len: " + l
				+ ") (actual len: " + al + ") ";
	}
}