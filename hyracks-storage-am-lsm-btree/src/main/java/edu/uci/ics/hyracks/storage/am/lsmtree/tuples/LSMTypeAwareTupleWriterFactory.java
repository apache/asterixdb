package edu.uci.ics.hyracks.storage.am.lsmtree.tuples;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleWriterFactory;

public class LSMTypeAwareTupleWriterFactory extends TypeAwareTupleWriterFactory {

	private static final long serialVersionUID = 1L;
	private ITypeTraits[] typeTraits;
	private final boolean isDelete;
	
	public LSMTypeAwareTupleWriterFactory(ITypeTraits[] typeTraits, boolean isDelete) {
		super(typeTraits);
		this.typeTraits = typeTraits;
		this.isDelete = isDelete;
	}

	@Override
	public ITreeIndexTupleWriter createTupleWriter() {
		return new LSMTypeAwareTupleWriter(typeTraits, isDelete);
	}

}
