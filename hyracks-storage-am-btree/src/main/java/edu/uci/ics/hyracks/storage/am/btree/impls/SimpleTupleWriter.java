package edu.uci.ics.hyracks.storage.am.btree.impls;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.api.ITupleWriter;

public class SimpleTupleWriter implements ITupleWriter {

	@Override
	public int bytesRequired(ITupleReference tuple) {
		int bytes = getNullFlagsBytes(tuple) + getFieldSlotsBytes(tuple);
		for(int i = 0; i < tuple.getFieldCount(); i++) {
			bytes += tuple.getFieldLength(i);
		}
		return bytes;
	}
	
	@Override
	public int bytesRequired(ITupleReference tuple, int startField, int numFields) {		
		int bytes = getNullFlagsBytes(tuple, startField, numFields) + getFieldSlotsBytes(tuple, startField, numFields);
		for(int i = startField; i < startField + numFields; i++) {
			bytes += tuple.getFieldLength(i);
		}
		return bytes;	
	}

	@Override
	public ITupleReference createTupleReference() {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public int writeTuple(ITupleReference tuple, ByteBuffer targetBuf, int targetOff) {
		int runner = targetOff;
		int nullFlagsBytes = getNullFlagsBytes(tuple);
		int fieldSlotsBytes = getFieldSlotsBytes(tuple);
		for(int i = 0; i < nullFlagsBytes; i++) {
			targetBuf.put(runner++, (byte)0);			
		}
		runner += fieldSlotsBytes;
		
		int fieldEndOff = 0;
		for(int i = 0; i < tuple.getFieldCount(); i++) {			
			System.arraycopy(tuple.getFieldData(i), tuple.getFieldStart(i), targetBuf.array(), runner, tuple.getFieldLength(i));
			fieldEndOff += tuple.getFieldLength(i);
			runner += tuple.getFieldLength(i);
			targetBuf.putShort(targetOff + nullFlagsBytes + i * 2, (short)fieldEndOff);	
		}
		
		return runner - targetOff;
	}

	@Override
	public int writeTupleFields(ITupleReference tuple, int startField, int numFields, ByteBuffer targetBuf, int targetOff) {	
		int runner = targetOff;
		int nullFlagsBytes = getNullFlagsBytes(tuple, startField, numFields);
		for(int i = 0; i < nullFlagsBytes; i++) {
			targetBuf.put(runner++, (byte)0);
		}
		runner += getFieldSlotsBytes(tuple, startField, numFields);
		
		int fieldEndOff = 0;
		int fieldCounter = 0;
		for(int i = startField; i < startField + numFields; i++) {			
			System.arraycopy(tuple.getFieldData(i), tuple.getFieldStart(i), targetBuf.array(), runner, tuple.getFieldLength(i));			
			fieldEndOff += tuple.getFieldLength(i);			
			runner += tuple.getFieldLength(i);		
			targetBuf.putShort(targetOff + nullFlagsBytes + fieldCounter * 2, (short)fieldEndOff);
			fieldCounter++;
		}
		
		return runner - targetOff;				
	}
	
	private int getNullFlagsBytes(ITupleReference tuple) {
		return (int)Math.ceil((double)tuple.getFieldCount() / 8.0);
	}
	
	private int getFieldSlotsBytes(ITupleReference tuple) {
		return tuple.getFieldCount() * 2;
	}
	
	private int getNullFlagsBytes(ITupleReference tuple, int startField, int numFields) {
		return (int)Math.ceil((double)numFields / 8.0);
	}
	
	private int getFieldSlotsBytes(ITupleReference tuple, int startField, int numFields) {
		return numFields * 2;
	}
}
