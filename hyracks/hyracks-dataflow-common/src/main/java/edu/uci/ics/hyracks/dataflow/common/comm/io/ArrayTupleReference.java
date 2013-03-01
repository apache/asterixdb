package edu.uci.ics.hyracks.dataflow.common.comm.io;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * An ArrayTupleReference provides access to a tuple that is not serialized into
 * a frame. It is meant to be reset directly with the field slots and tuple data
 * provided by ArrayTupleBuilder. The purpose is to avoid coping the built tuple
 * into a frame before being able to use it as an ITupleReference.
 * 
 * @author alexander.behm
 */
public class ArrayTupleReference implements ITupleReference {
	private int[] fEndOffsets;
	private byte[] tupleData;

	public void reset(int[] fEndOffsets, byte[] tupleData) {
		this.fEndOffsets = fEndOffsets;
		this.tupleData = tupleData;
	}
	
	@Override
	public int getFieldCount() {
		return fEndOffsets.length;
	}

	@Override
	public byte[] getFieldData(int fIdx) {
		return tupleData;
	}

	@Override
	public int getFieldStart(int fIdx) {
		return (fIdx == 0) ? 0 : fEndOffsets[fIdx - 1]; 
	}

	@Override
	public int getFieldLength(int fIdx) {
		return (fIdx == 0) ? fEndOffsets[0] : fEndOffsets[fIdx] - fEndOffsets[fIdx - 1];
	}
}
