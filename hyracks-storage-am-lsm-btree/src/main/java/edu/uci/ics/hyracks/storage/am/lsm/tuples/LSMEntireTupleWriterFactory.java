package edu.uci.ics.hyracks.storage.am.lsm.tuples;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleWriterFactory;

public class LSMEntireTupleWriterFactory extends TypeAwareTupleWriterFactory {
	private static final long serialVersionUID = 1L;
	private final ITypeTraits[] typeTraits;
	private final int numKeyFields;
	
	public LSMEntireTupleWriterFactory(ITypeTraits[] typeTraits, int numKeyFields) {
		super(typeTraits);
		this.typeTraits = typeTraits;
		this.numKeyFields = numKeyFields;
	}

	@Override
	public ITreeIndexTupleWriter createTupleWriter() {
		return new LSMEntireTupleWriter(typeTraits, numKeyFields);
	}
}
