package edu.uci.ics.hyracks.storage.am.btree.tuples;

import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeTupleWriter;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeTupleWriterFactory;

public class SimpleTupleWriterFactory implements IBTreeTupleWriterFactory {
	
	private static final long serialVersionUID = 1L;

	@Override
	public IBTreeTupleWriter createTupleWriter() {
		return new SimpleTupleWriter();
	}
	
}
