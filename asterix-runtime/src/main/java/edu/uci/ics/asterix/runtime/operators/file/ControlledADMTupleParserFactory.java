package edu.uci.ics.asterix.runtime.operators.file;


import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParser;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

/**
 * A Controlled tuple parser factory for creating a tuple parser capable of parsing
 * ADM data.
 */
public class ControlledADMTupleParserFactory implements ITupleParserFactory{
	private static final long serialVersionUID = 1L;

    protected ARecordType recType;
    
    public ControlledADMTupleParserFactory(ARecordType recType){
    	this.recType = recType;
    }

	@Override
	public ITupleParser createTupleParser(IHyracksTaskContext ctx)
			throws HyracksDataException {
		return new ControlledADMTupleParser(ctx, recType);
	}
}