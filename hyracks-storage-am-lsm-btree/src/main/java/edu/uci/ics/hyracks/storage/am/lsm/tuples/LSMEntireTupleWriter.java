package edu.uci.ics.hyracks.storage.am.lsm.tuples;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.util.TupleUtils;

public class LSMEntireTupleWriter extends LSMTypeAwareTupleWriter {
	public LSMEntireTupleWriter(ITypeTraits[] typeTraits, int numKeyFields){
		// Just give false as third parameter. It is never used locally.
	    super(typeTraits, numKeyFields, false);
	}
	
	@Override
    public int writeTuple(ITupleReference tuple, byte[] targetBuf, int targetOff) {
		int tupleSize = this.bytesRequired(tuple);
		byte[] buf = tuple.getFieldData(0);
		int tupleStartOff = ((LSMTypeAwareTupleReference)tuple).getTupleStart();
		System.arraycopy(buf, tupleStartOff, targetBuf, targetOff, tupleSize);
		//System.out.println("BEF: " + printTuple(tuple));
        return tupleSize;
    }
	
	private String printTuple(ITupleReference tuple) {
        LSMTypeAwareTupleReference lsmTuple = (LSMTypeAwareTupleReference)tuple;
        ISerializerDeserializer[] fieldSerdes = new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
        String s = null;
        try {
            s = TupleUtils.printTuple(lsmTuple, fieldSerdes);
        } catch (HyracksDataException e) {
            e.printStackTrace();
        }
        s += " " + lsmTuple.isDelete();
        return s;
    }
}
