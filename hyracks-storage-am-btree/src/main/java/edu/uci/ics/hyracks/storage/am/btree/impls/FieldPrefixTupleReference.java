package edu.uci.ics.hyracks.storage.am.btree.impls;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeTupleReference;
import edu.uci.ics.hyracks.storage.am.btree.frames.FieldPrefixNSMLeafFrame;

public class FieldPrefixTupleReference implements IBTreeTupleReference {

    private FieldPrefixNSMLeafFrame frame;
    private int prefixTupleStartOff;
    private int suffixTupleStartOff;
    private int numPrefixFields;
    private int fieldCount;
    private IBTreeTupleReference helperTuple;

    public FieldPrefixTupleReference(IBTreeTupleReference helperTuple) {
        this.helperTuple = helperTuple;
    }

    @Override
    public void resetByTupleIndex(IBTreeFrame frame, int tupleIndex) {
        this.frame = (FieldPrefixNSMLeafFrame) frame;

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
        // not implemented
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
            helperTuple.resetByOffset(frame.getBuffer(), prefixTupleStartOff);
            return helperTuple.getFieldLength(fIdx);
        } else {
            helperTuple.setFieldCount(numPrefixFields, fieldCount - numPrefixFields);
            helperTuple.resetByOffset(frame.getBuffer(), suffixTupleStartOff);
            return helperTuple.getFieldLength(fIdx - numPrefixFields);
        }
    }

    @Override
    public int getFieldStart(int fIdx) {
        if (fIdx < numPrefixFields) {
            helperTuple.setFieldCount(numPrefixFields);
            helperTuple.resetByOffset(frame.getBuffer(), prefixTupleStartOff);
            return helperTuple.getFieldStart(fIdx);
        } else {
            helperTuple.setFieldCount(numPrefixFields, fieldCount - numPrefixFields);
            helperTuple.resetByOffset(frame.getBuffer(), suffixTupleStartOff);
            return helperTuple.getFieldStart(fIdx - numPrefixFields);
        }
    }

    // unsupported operation
    @Override
    public void resetByOffset(ByteBuffer buf, int tupleStartOffset) {
        frame = null;
    }
}
