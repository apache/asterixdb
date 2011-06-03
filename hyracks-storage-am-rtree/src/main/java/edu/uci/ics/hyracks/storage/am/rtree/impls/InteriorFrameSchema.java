package edu.uci.ics.hyracks.storage.am.rtree.impls;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.api.dataflow.value.TypeTrait;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;

public class InteriorFrameSchema {

    private MultiComparator interiorCmp;

    public InteriorFrameSchema(MultiComparator leafCmp) {
        // 1 for node's pointer
        ITypeTrait[] interiorTypeTraits = new ITypeTrait[leafCmp.getKeyFieldCount() + 1];
        for (int i = 0; i < leafCmp.getKeyFieldCount(); i++) {
            interiorTypeTraits[i] = new TypeTrait(leafCmp.getTypeTraits()[i].getStaticallyKnownDataLength());
        }

        // the pointer is of type int (size: 4 bytes)
        interiorTypeTraits[leafCmp.getKeyFieldCount()] = new TypeTrait(4);
        interiorCmp = new MultiComparator(interiorTypeTraits, leafCmp.getComparators());
    }

    public MultiComparator getInteriorCmp() {
        return interiorCmp;
    }
}