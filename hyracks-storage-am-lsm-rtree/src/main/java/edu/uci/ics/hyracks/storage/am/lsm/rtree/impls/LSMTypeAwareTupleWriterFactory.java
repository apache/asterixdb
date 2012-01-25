package edu.uci.ics.hyracks.storage.am.lsm.rtree.impls;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleWriter;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.am.rtree.tuples.RTreeTypeAwareTupleWriter;

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
	    if (isDelete) {
	        return new TypeAwareTupleWriter(typeTraits);
	    } else {
	        return new RTreeTypeAwareTupleWriter(typeTraits);
	    }
	}

}
