package edu.uci.ics.hyracks.storage.am.btree.dataflow;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class PermutingFrameTupleReference implements IFrameTupleReference {
	private IFrameTupleAccessor fta;
    private int tIndex;
    private int[] fieldPermutation;
    
    public void setFieldPermutation(int[] fieldPermutation) {
    	this.fieldPermutation = fieldPermutation;
    }
    
    public void reset(IFrameTupleAccessor fta, int tIndex) {
        this.fta = fta;
        this.tIndex = tIndex;
    }

    @Override
    public IFrameTupleAccessor getFrameTupleAccessor() {
        return fta;
    }

    @Override
    public int getTupleIndex() {
        return tIndex;
    }

    @Override
    public int getFieldCount() {
        return fieldPermutation.length;
    }

    @Override
    public byte[] getFieldData(int fIdx) {
        return fta.getBuffer().array();
    }

    @Override
    public int getFieldStart(int fIdx) {    	    	
    	return fta.getTupleStartOffset(tIndex) + fta.getFieldSlotsLength() + fta.getFieldStartOffset(tIndex, fieldPermutation[fIdx]);
    }

    @Override
    public int getFieldLength(int fIdx) {
        return fta.getFieldLength(tIndex, fieldPermutation[fIdx]);
    }
}
