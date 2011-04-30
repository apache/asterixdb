package edu.uci.ics.hyracks.storage.am.invertedindex.impls;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

public class FixedSizeTupleReference implements ITupleReference {

    private final ITypeTrait[] typeTraits;
    private final int[] fieldStartOffsets;
    private byte[] data;
    private int startOff;    
    
    public FixedSizeTupleReference(ITypeTrait[] typeTraits) {
        this.typeTraits = typeTraits;
        this.fieldStartOffsets = new int[typeTraits.length];
        this.fieldStartOffsets[0] = 0;
        for(int i = 1; i < typeTraits.length; i++) {
            fieldStartOffsets[i] = fieldStartOffsets[i-1] + typeTraits[i-1].getStaticallyKnownDataLength();
        }
    }
    
    public void reset(byte[] data, int startOff) {
        this.data = data;
        this.startOff = startOff;
    }
    
    @Override
    public int getFieldCount() {
        return typeTraits.length;
    }

    @Override
    public byte[] getFieldData(int fIdx) {
        return data;
    }

    @Override
    public int getFieldLength(int fIdx) {
        return typeTraits[fIdx].getStaticallyKnownDataLength();
    }

    @Override
    public int getFieldStart(int fIdx) {
        return startOff + fieldStartOffsets[fIdx];
    }
}
