package edu.uci.ics.asterix.runtime.operators.file;

import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

/**
 * An extension of AbstractControlledTupleParser that provides functionality for
 * parsing Adm formatted input.
 */
public class ControlledADMTupleParser extends AbstractControlledTupleParser{

	public ControlledADMTupleParser(IHyracksTaskContext ctx, ARecordType recType)
			throws HyracksDataException {
		super(ctx, recType);
	}

	@Override
	public IDataParser getDataParser() {
		return new ADMDataParser();
	}

}