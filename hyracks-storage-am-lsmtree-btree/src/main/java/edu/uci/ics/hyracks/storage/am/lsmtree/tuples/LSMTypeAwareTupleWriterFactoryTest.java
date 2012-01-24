package edu.uci.ics.hyracks.storage.am.lsmtree.tuples;

import static org.junit.Assert.fail;

import org.junit.Test;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriter;

public class LSMTypeAwareTupleWriterFactoryTest {
    @Test
    public void test01() throws Exception {

        // declare fields
        int fieldCount = 2;
        ITypeTraits[] typeTraits = new ITypeTraits[fieldCount];
        typeTraits[0] = IntegerPointable.TYPE_TRAITS;
        typeTraits[1] = IntegerPointable.TYPE_TRAITS;
	    
	    LSMTypeAwareTupleWriterFactory lsmTupleWriterFactory = new LSMTypeAwareTupleWriterFactory(typeTraits, false);
	    ITreeIndexTupleWriter lsmTupleWriter = lsmTupleWriterFactory.createTupleWriter();

	    if(lsmTupleWriter != null) {
	    	return;
	    }
	    else {
	    	fail("fail to create LSMTypeAwareTupleWriter!");
	    }
	}
}
