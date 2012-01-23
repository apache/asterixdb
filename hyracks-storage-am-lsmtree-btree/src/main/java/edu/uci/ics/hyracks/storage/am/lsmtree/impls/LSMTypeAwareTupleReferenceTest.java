package edu.uci.ics.hyracks.storage.am.lsmtree.impls;

import static org.junit.Assert.fail;

import java.nio.ByteBuffer;

import org.junit.Test;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriter;

public class LSMTypeAwareTupleReferenceTest {

    @Test
    public void test01() throws Exception {
    
    	int targetOff = 0;
    	ByteBuffer buf = ByteBuffer.allocate(32);
    	
        int fieldCount = 2;
        ITypeTraits[] typeTraits = new ITypeTraits[fieldCount];
        typeTraits[0] = IntegerPointable.TYPE_TRAITS;
        typeTraits[1] = IntegerPointable.TYPE_TRAITS;
        
        LSMTypeAwareTupleWriterFactory insertTupleWriterFactory = new LSMTypeAwareTupleWriterFactory(typeTraits, false);
        ITreeIndexTupleWriter insertTupleWriter = insertTupleWriterFactory.createTupleWriter();
        ITreeIndexTupleReference lsmTupleReference  = insertTupleWriter.createTupleReference();
        
        lsmTupleReference.resetByTupleOffset(buf, targetOff);
        insertTupleWriter.writeTuple(lsmTupleReference, buf, targetOff);
        
        boolean del = ((LSMTypeAwareTupleReference) lsmTupleReference).isDelete();
        
        if(del == false) {
        	return;
        }
        else {
        	fail("fail to write tuple");
        }	
    }
    
    @Test
    public void test02() throws Exception {
    
    	int targetOff = 0;
    	ByteBuffer buf = ByteBuffer.allocate(32);
    	
        int fieldCount = 2;
        ITypeTraits[] typeTraits = new ITypeTraits[fieldCount];
        typeTraits[0] = IntegerPointable.TYPE_TRAITS;
        typeTraits[1] = IntegerPointable.TYPE_TRAITS;
        
        LSMTypeAwareTupleWriterFactory deleteTupleWriterFactory = new LSMTypeAwareTupleWriterFactory(typeTraits, true);
        ITreeIndexTupleWriter deleteTupleWriter = deleteTupleWriterFactory.createTupleWriter();
        ITreeIndexTupleReference lsmTupleReference  = deleteTupleWriter.createTupleReference();
        
        lsmTupleReference.resetByTupleOffset(buf, targetOff);
        deleteTupleWriter.writeTuple(lsmTupleReference, buf, targetOff);
        
        boolean del = ((LSMTypeAwareTupleReference) lsmTupleReference).isDelete();
        
        if(del == true) {
        	return;
        }
        else {
        	fail("fail to write tuple");
        }	
    }
}
