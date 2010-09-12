package edu.uci.ics.hyracks.storage.am.btree.impls;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IFieldAccessor;
import edu.uci.ics.hyracks.storage.am.btree.frames.FieldPrefixNSMLeafFrame;

public class FieldPrefixPrefixTuple implements ITupleReference {
	
	public int recSlotOff = -1;
    public int prefixSlotOff = -1;
    public int prefixSlotNum = FieldPrefixSlotManager.RECORD_UNCOMPRESSED;
    public int numPrefixFields = 0;
    public IFieldAccessor[] fields;
    public FieldPrefixNSMLeafFrame frame;
    
    public int currentField = -1;
    public int recOffRunner = -1;    
    
    public FieldPrefixPrefixTuple() {    	
    }
    
    public FieldPrefixPrefixTuple(IFieldAccessor[] fields, FieldPrefixNSMLeafFrame frame) {
        this.fields = fields;
        this.frame = frame;
    }
    
    public void setFrame(IBTreeFrame frame) {
        this.frame = (FieldPrefixNSMLeafFrame)frame;        
    }
    
    public void setFields(IFieldAccessor[] fields) {
        this.fields = fields;
    }
    
    public void openPrefixSlotNum(int prefixSlotNum) {          
        openPrefixSlotOff(frame.slotManager.getPrefixSlotOff(prefixSlotNum));        
    }
    
    public void openPrefixSlotOff(int prefixSlotOff) {
        this.prefixSlotOff = prefixSlotOff;
        reset();
    }
       
    // re-position to first field
    public void reset() {
        currentField = 0;        
        int prefixSlot = frame.getBuffer().getInt(prefixSlotOff);
        numPrefixFields = frame.slotManager.decodeFirstSlotField(prefixSlot);
        recOffRunner = frame.slotManager.decodeSecondSlotField(prefixSlot);               
    }
        
    public void nextField() {           
    	recOffRunner += fields[currentField].getLength(frame.getBuffer().array(), recOffRunner);    	
    }
    
    public int getFieldOff() {
        return recOffRunner;
    }        
    
    public int getFieldSize() {
        return fields[currentField].getLength(frame.getBuffer().array(), recOffRunner);                
    }
    
	@Override
	public int getFieldCount() {
		return numPrefixFields;
	}

	@Override
	public byte[] getFieldData(int fIdx) {
		return frame.getBuffer().array();
	}

	@Override
	public int getFieldLength(int fIdx) {
		reset();
		for(int i = 0; i < fIdx; i++) {
			nextField();
		}						
		return getFieldSize();
	}
	
	@Override
	public int getFieldStart(int fIdx) {
		reset();
		for(int i = 0; i < fIdx; i++) {
			nextField();
		}						
		return recOffRunner;
	}
}
