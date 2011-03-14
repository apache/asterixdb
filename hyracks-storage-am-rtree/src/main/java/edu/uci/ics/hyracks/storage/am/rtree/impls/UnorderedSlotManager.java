package edu.uci.ics.hyracks.storage.am.rtree.impls;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import edu.uci.ics.hyracks.storage.am.common.frames.AbstractSlotManager;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.FindTupleMode;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.FindTupleNoExactMatchPolicy;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;

public class UnorderedSlotManager extends AbstractSlotManager {
    public int findTupleIndex(ITupleReference searchKey, ITreeIndexTupleReference frameTuple, MultiComparator multiCmp,
            FindTupleMode mode, FindTupleNoExactMatchPolicy matchPolicy) {
        if (mode == FindTupleMode.FTM_EXACT) {
            for (int i = 0; i < frame.getTupleCount(); i++) {
                frameTuple.resetByTupleIndex(frame, i);
                int cmp = multiCmp.compare(searchKey, frameTuple);
                if (cmp == 0) {
                    return i;
                }
            }
        }
        return -1;
    }

    @Override
    public int insertSlot(int tupleIndex, int tupleOff) {
        int slotOff = getSlotEndOff() - slotSize;
        setSlot(slotOff, tupleOff);
        return slotOff;
    }

}
