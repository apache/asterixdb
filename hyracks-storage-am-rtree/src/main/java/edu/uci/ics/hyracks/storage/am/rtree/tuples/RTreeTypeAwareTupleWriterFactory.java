package edu.uci.ics.hyracks.storage.am.rtree.tuples;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriterFactory;

public class RTreeTypeAwareTupleWriterFactory implements ITreeIndexTupleWriterFactory {

    private static final long serialVersionUID = 1L;
    private ITypeTrait[] typeTraits;

    public RTreeTypeAwareTupleWriterFactory(ITypeTrait[] typeTraits) {
        this.typeTraits = typeTraits;
    }

    @Override
    public ITreeIndexTupleWriter createTupleWriter() {
        return new RTreeTypeAwareTupleWriter(typeTraits);
    }

}
