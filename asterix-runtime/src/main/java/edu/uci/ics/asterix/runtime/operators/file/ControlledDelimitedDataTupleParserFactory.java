package edu.uci.ics.asterix.runtime.operators.file;

import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParser;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

public class ControlledDelimitedDataTupleParserFactory implements ITupleParserFactory{
	private static final long serialVersionUID = 1L;
	private IValueParserFactory[] valueParserFactories;
	private char fieldDelimiter;
	protected ARecordType recordType;


	public ControlledDelimitedDataTupleParserFactory(ARecordType recordType, IValueParserFactory[] fieldParserFactories, char fieldDelimiter) {
		this.recordType = recordType;
		this.valueParserFactories = fieldParserFactories;
		this.fieldDelimiter = fieldDelimiter;
	}
	

	@Override
	public ITupleParser createTupleParser(IHyracksTaskContext ctx)
			throws HyracksDataException {
		return new ControlledDelimitedDataTupleParser(ctx, recordType, valueParserFactories, fieldDelimiter);
	}
}