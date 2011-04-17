package edu.uci.ics.hyracks.storage.am.rtree.impls;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import edu.uci.ics.hyracks.storage.am.common.frames.AbstractSlotManager;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.FindTupleMode;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.FindTupleNoExactMatchPolicy;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.rtree.frames.NSMRTreeFrame;

public class UnorderedSlotManager extends AbstractSlotManager {
    @Override
    public int findTupleIndex(ITupleReference searchKey, ITreeIndexTupleReference frameTuple, MultiComparator multiCmp,
            FindTupleMode mode, FindTupleNoExactMatchPolicy matchPolicy) {
        return -1;
    }

    @Override
    public int insertSlot(int tupleIndex, int tupleOff) {
        int slotOff = getSlotEndOff() - slotSize;
        setSlot(slotOff, tupleOff);
        return slotOff;
    }

    public void modifySlot(int slotOff, int tupleOff) {
        setSlot(slotOff, tupleOff);
    }

    public void deleteEmptySlots() {
        int slotOff = getSlotStartOff();
        int numOfSlots = ((getSlotStartOff() - getSlotEndOff()) / slotSize) + 1;
        for (int i = 0; i < numOfSlots; i++) {
            if (frame.getBuffer().getInt(slotOff) == -1) {
                int slotStartOff = getSlotEndOff();
                int length = slotOff - slotStartOff;
                System.arraycopy(frame.getBuffer().array(), slotStartOff, frame.getBuffer().array(), slotStartOff
                        + slotSize, length);
                ((NSMRTreeFrame) frame).setTupleCount(frame.getTupleCount() - 1);
            } else {
                slotOff -= slotSize;
            }
        }
    }
}
