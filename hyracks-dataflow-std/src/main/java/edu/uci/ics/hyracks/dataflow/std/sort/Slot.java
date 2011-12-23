package edu.uci.ics.hyracks.dataflow.std.sort;

/**
 * @author pouria
 *         Defines a slot in the memory, which can be a free or used (allocated)
 *         slot. Memory is a set of frames, ordered as a list. Each tuple is
 *         stored in a slot, where the location of the slot is denoted by a pair
 *         of integers:
 *         - The index of the frame, in the list of frames in memory. (referred
 *         to as frameIx)
 *         - The starting offset of the slot, within that specific frame.
 *         (referred to as offset)
 */
public class Slot {

    private int frameIx;
    private int offset;

    public Slot() {
        this.frameIx = BSTNodeUtil.INVALID_INDEX;
        this.offset = BSTNodeUtil.INVALID_INDEX;
    }

    public Slot(int frameIx, int offset) {
        this.frameIx = frameIx;
        this.offset = offset;
    }

    public void set(int frameIx, int offset) {
        this.frameIx = frameIx;
        this.offset = offset;
    }

    public int getFrameIx() {
        return frameIx;
    }

    public void setFrameIx(int frameIx) {
        this.frameIx = frameIx;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public boolean isNull() {
        return (frameIx == BSTNodeUtil.INVALID_INDEX) || (offset == BSTNodeUtil.INVALID_INDEX);
    }

    public void clear() {
        this.frameIx = BSTNodeUtil.INVALID_INDEX;
        this.offset = BSTNodeUtil.INVALID_INDEX;
    }

    public void copy(Slot s) {
        this.frameIx = s.getFrameIx();
        this.offset = s.getOffset();
    }

    public String toString() {
        return "(" + frameIx + ", " + offset + ")";
    }
}