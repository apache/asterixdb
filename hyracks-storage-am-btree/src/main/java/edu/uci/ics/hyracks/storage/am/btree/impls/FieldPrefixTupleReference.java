package edu.uci.ics.hyracks.storage.am.btree.impls;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeFieldPrefixNSMLeafFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleReference;

public class FieldPrefixTupleReference implements ITreeIndexTupleReference {

    private BTreeFieldPrefixNSMLeafFrame frame;
    private int prefixTupleStartOff;
    private int suffixTupleStartOff;
    private int numPrefixFields;
    private int fieldCount;
    private ITreeIndexTupleReference helperTuple;

    public FieldPrefixTupleReference(ITreeIndexTupleReference helperTuple) {
        this.helperTuple = helperTuple;
        this.fieldCount = helperTuple.getFieldCount();
    }

    @Override
    public void resetByTupleIndex(ITreeIndexFrame frame, int tupleIndex) {
        this.frame = (BTreeFieldPrefixNSMLeafFrame) frame;

        int tupleSlotOff = this.frame.slotManager.getTupleSlotOff(tupleIndex);
        int tupleSlot = this.frame.getBuffer().getInt(tupleSlotOff);
        int prefixSlotNum = this.frame.slotManager.decodeFirstSlotField(tupleSlot);
        suffixTupleStartOff = this.frame.slotManager.decodeSecondSlotField(tupleSlot);

        if (prefixSlotNum != FieldPrefixSlotManager.TUPLE_UNCOMPRESSED) {
            int prefixSlotOff = this.frame.slotManager.getPrefixSlotOff(prefixSlotNum);
            int prefixSlot = this.frame.getBuffer().getInt(prefixSlotOff);
            numPrefixFields = this.frame.slotManager.decodeFirstSlotField(prefixSlot);
            prefixTupleStartOff = this.frame.slotManager.decodeSecondSlotField(prefixSlot);
        } else {
            numPrefixFields = 0;
            prefixTupleStartOff = -1;
        }
    }

    @Override
    public void setFieldCount(int fieldCount) {
        this.fieldCount = fieldCount;
    }

    @Override
    public void setFieldCount(int fieldStartIndex, int fieldCount) {
        throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public int getFieldCount() {
        return fieldCount;
    }

    @Override
    public byte[] getFieldData(int fIdx) {
        return frame.getBuffer().array();
    }

    @Override
    public int getFieldLength(int fIdx) {
        if (fIdx < numPrefixFields) {
            helperTuple.setFieldCount(numPrefixFields);
            helperTuple.resetByTupleOffset(frame.getBuffer(), prefixTupleStartOff);
            return helperTuple.getFieldLength(fIdx);
        } else {
            helperTuple.setFieldCount(numPrefixFields, fieldCount - numPrefixFields);
            helperTuple.resetByTupleOffset(frame.getBuffer(), suffixTupleStartOff);
            return helperTuple.getFieldLength(fIdx - numPrefixFields);
        }
    }

    @Override
    public int getFieldStart(int fIdx) {
        if (fIdx < numPrefixFields) {
            helperTuple.setFieldCount(numPrefixFields);
            helperTuple.resetByTupleOffset(frame.getBuffer(), prefixTupleStartOff);
            return helperTuple.getFieldStart(fIdx);
        } else {
            helperTuple.setFieldCount(numPrefixFields, fieldCount - numPrefixFields);
            helperTuple.resetByTupleOffset(frame.getBuffer(), suffixTupleStartOff);
            return helperTuple.getFieldStart(fIdx - numPrefixFields);
        }
    }

    // unsupported operation
    @Override
    public void resetByTupleOffset(ByteBuffer buf, int tupleStartOffset) {
        throw new UnsupportedOperationException("Resetting this type of frame by offset is not supported.");
    }

    @Override
    public int getTupleSize() {
        return getSuffixTupleSize() + getPrefixTupleSize();
    }
    
    public int getSuffixTupleSize() {
        helperTuple.setFieldCount(numPrefixFields, fieldCount - numPrefixFields);
        helperTuple.resetByTupleOffset(frame.getBuffer(), suffixTupleStartOff);
        return helperTuple.getTupleSize();
    }
    
    public int getPrefixTupleSize() {
        if (numPrefixFields == 0) return 0;
        helperTuple.setFieldCount(numPrefixFields);
        helperTuple.resetByTupleOffset(frame.getBuffer(), prefixTupleStartOff);
        return helperTuple.getTupleSize();
    }
    
    public int getNumPrefixFields() {
        return numPrefixFields;
    }
}
