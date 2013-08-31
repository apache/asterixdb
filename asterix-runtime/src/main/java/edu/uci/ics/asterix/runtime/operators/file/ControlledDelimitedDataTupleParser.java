package edu.uci.ics.asterix.runtime.operators.file;

import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;

public class ControlledDelimitedDataTupleParser extends AbstractControlledTupleParser{

	private final DelimitedDataParser dataParser;
	
	public ControlledDelimitedDataTupleParser(IHyracksTaskContext ctx,
			ARecordType recType,  IValueParserFactory[] valueParserFactories, char fieldDelimter) throws HyracksDataException {
		super(ctx, recType);
		dataParser = new DelimitedDataParser(recType, valueParserFactories, fieldDelimter);
	}

	@Override
	public IDataParser getDataParser() {
		return dataParser;
	}

}