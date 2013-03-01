package edu.uci.ics.hivesterix.serde.parser;

import java.io.IOException;

import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;

public interface IHiveParser {
	/**
	 * parse one hive rwo into
	 * 
	 * @param row
	 * @param objectInspector
	 * @param tb
	 */
	public void parse(byte[] data, int start, int length, ArrayTupleBuilder tb)
			throws IOException;
}
