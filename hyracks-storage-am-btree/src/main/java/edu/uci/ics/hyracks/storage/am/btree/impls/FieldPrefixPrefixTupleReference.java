package edu.uci.ics.hyracks.storage.am.btree.impls;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeFrame;
import edu.uci.ics.hyracks.storage.am.btree.frames.FieldPrefixNSMLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.tuples.TypeAwareTupleReference;

public class FieldPrefixPrefixTupleReference extends TypeAwareTupleReference {
	
	public FieldPrefixPrefixTupleReference(ITypeTrait[] typeTraits) {
		super(typeTraits);
	}
	
	// assumes tuple index refers to prefix tuples
	@Override
	public void resetByTupleIndex(IBTreeFrame frame, int tupleIndex) {
		FieldPrefixNSMLeafFrame concreteFrame = (FieldPrefixNSMLeafFrame)frame;		
		int prefixSlotOff = concreteFrame.slotManager.getPrefixSlotOff(tupleIndex);
		int prefixSlot = concreteFrame.getBuffer().getInt(prefixSlotOff);
		setFieldCount(concreteFrame.slotManager.decodeFirstSlotField(prefixSlot));		
		tupleStartOff = concreteFrame.slotManager.decodeSecondSlotField(prefixSlot);
		buf = concreteFrame.getBuffer();
		resetByOffset(buf, tupleStartOff);
	}	
}
