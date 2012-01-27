package edu.uci.ics.hyracks.storage.am.lsm.tuples;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

public class LSMEntireTupleWriter extends LSMTypeAwareTupleWriter {
	public LSMEntireTupleWriter(ITypeTraits[] typeTraits, int numKeyFields){
		// Third parameter is never used locally, just give false.
	    super(typeTraits, numKeyFields, false);
	}
	
	@Override
    public int writeTuple(ITupleReference tuple, byte[] targetBuf, int targetOff) {
		int tupleSize = bytesRequired(tuple);
		byte[] buf = tuple.getFieldData(0);
		int tupleStartOff = ((LSMTypeAwareTupleReference)tuple).getTupleStart();
		System.arraycopy(buf, tupleStartOff, targetBuf, targetOff, tupleSize);
        return tupleSize;
    }
}
