package edu.uci.ics.hyracks.storage.am.btree.impls;

import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeFrame;
import edu.uci.ics.hyracks.storage.am.btree.frames.FieldPrefixNSMLeafFrame;

public class FieldPrefixPrefixTupleReference extends SimpleTupleReference {
	
	// assumes tuple index refers to prefix tuples
	@Override
	public void resetByTupleIndex(IBTreeFrame frame, int tupleIndex) {
		FieldPrefixNSMLeafFrame concreteFrame = (FieldPrefixNSMLeafFrame)frame;		
		int prefixSlotOff = concreteFrame.slotManager.getPrefixSlotOff(tupleIndex);
		int prefixSlot = concreteFrame.getBuffer().getInt(prefixSlotOff);
		setFieldCount(concreteFrame.slotManager.decodeFirstSlotField(prefixSlot));
		tupleStartOff = concreteFrame.slotManager.decodeSecondSlotField(prefixSlot);
		buf = concreteFrame.getBuffer();
	}	
}
