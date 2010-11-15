package edu.uci.ics.hyracks.storage.am.btree.tuples;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeTupleWriter;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeTupleWriterFactory;

public class TypeAwareTupleWriterFactory implements IBTreeTupleWriterFactory {
	
	private static final long serialVersionUID = 1L;
	private ITypeTrait[] typeTraits;
	
	public TypeAwareTupleWriterFactory(ITypeTrait[] typeTraits) {
		this.typeTraits = typeTraits;
	}
	
	@Override
	public IBTreeTupleWriter createTupleWriter() {
		return new TypeAwareTupleWriter(typeTraits);
	}
	
}
