package edu.uci.ics.hyracks.storage.am.lsm.tuples;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleWriterFactory;

public class LSMTypeAwareTupleWriterFactory extends TypeAwareTupleWriterFactory {

	private static final long serialVersionUID = 1L;
	private final ITypeTraits[] typeTraits;
	private final int numKeyFields;
	private final boolean isDelete;
	
	public LSMTypeAwareTupleWriterFactory(ITypeTraits[] typeTraits, int numKeyFields, boolean isDelete) {
		super(typeTraits);
		this.typeTraits = typeTraits;
		this.numKeyFields = numKeyFields;
		this.isDelete = isDelete;
	}

	@Override
	public ITreeIndexTupleWriter createTupleWriter() {
		return new LSMTypeAwareTupleWriter(typeTraits, numKeyFields, isDelete);
	}

}
