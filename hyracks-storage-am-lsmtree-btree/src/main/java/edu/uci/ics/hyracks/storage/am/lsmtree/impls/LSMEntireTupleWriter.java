package edu.uci.ics.hyracks.storage.am.lsmtree.impls;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleWriter;

public class LSMEntireTupleWriter extends TypeAwareTupleWriter {
	public LSMEntireTupleWriter(ITypeTraits[] typeTraits){
		super(typeTraits);
	}
	@Override
    protected int getNullFlagsBytes(ITupleReference tuple) {
		//+1.0 is for insert/delete tuple checking
        return (int) Math.ceil(((double) tuple.getFieldCount() + 1.0) / 8.0);
    }
	
	@Override
    public int writeTuple(ITupleReference tuple, byte[] targetBuf, int targetOff) {
		int tupleSize = this.bytesRequired(tuple);
		byte[] buf = tuple.getFieldData(0);
		int tupleStartOff = ((LSMTypeAwareTupleReference)tuple).getTupleStart();
		System.arraycopy(buf, tupleStartOff, targetBuf, targetOff, tupleSize);
        return tupleSize;
    }
}
