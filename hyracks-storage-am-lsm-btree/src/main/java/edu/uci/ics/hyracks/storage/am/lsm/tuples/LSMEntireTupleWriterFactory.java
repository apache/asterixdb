package edu.uci.ics.hyracks.storage.am.lsm.tuples;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleWriterFactory;

public class LSMEntireTupleWriterFactory extends TypeAwareTupleWriterFactory {
	private static final long serialVersionUID = 1L;
	private ITypeTraits[] typeTraits;
	
	public LSMEntireTupleWriterFactory(ITypeTraits[] typeTraits) {
		super(typeTraits);
		this.typeTraits = typeTraits;
	}

	@Override
	public ITreeIndexTupleWriter createTupleWriter() {
		return new LSMEntireTupleWriter(typeTraits);
	}
}
