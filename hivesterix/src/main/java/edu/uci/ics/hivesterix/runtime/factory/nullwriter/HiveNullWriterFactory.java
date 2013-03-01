package edu.uci.ics.hivesterix.runtime.factory.nullwriter;

import java.io.DataOutput;

import edu.uci.ics.hyracks.api.dataflow.value.INullWriter;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriterFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class HiveNullWriterFactory implements INullWriterFactory {

	private static final long serialVersionUID = 1L;

	public static HiveNullWriterFactory INSTANCE = new HiveNullWriterFactory();

	@Override
	public INullWriter createNullWriter() {
		return new HiveNullWriter();
	}
}

class HiveNullWriter implements INullWriter {

	@Override
	public void writeNull(DataOutput out) throws HyracksDataException {
		// do nothing
	}

}
