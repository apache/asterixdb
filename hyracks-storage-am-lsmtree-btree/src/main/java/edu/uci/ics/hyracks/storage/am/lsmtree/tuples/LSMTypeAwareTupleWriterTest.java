package edu.uci.ics.hyracks.storage.am.lsmtree.tuples;

import static org.junit.Assert.fail;

import java.nio.ByteBuffer;

import org.junit.Test;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriter;

public class LSMTypeAwareTupleWriterTest {

	// test create tuple reference
	@Test
	public void test01() throws Exception {

		// declare fields
		int fieldCount = 2;
		ITypeTraits[] typeTraits = new ITypeTraits[fieldCount];
		typeTraits[0] = IntegerPointable.TYPE_TRAITS;
		typeTraits[1] = IntegerPointable.TYPE_TRAITS;

		LSMTypeAwareTupleWriterFactory lsmTupleWriterFactory = new LSMTypeAwareTupleWriterFactory(
				typeTraits, false);
		ITreeIndexTupleWriter lsmTupleWriter = lsmTupleWriterFactory.createTupleWriter();

		ITreeIndexTupleReference lsmTupleReference = lsmTupleWriter.createTupleReference();

		if (lsmTupleReference != null) {
			return;
		} else {
			fail("fail to create LSMTypeAwareTupleWriter");
		}
	}

	// test insert tuple writer
	@Test
	public void test02() throws Exception {

		int targetOff = 0;
		ByteBuffer buf = ByteBuffer.allocate(32);

		// declare fields
        int fieldCount = 2;
        ITypeTraits[] typeTraits = new ITypeTraits[fieldCount];
        typeTraits[0] = IntegerPointable.TYPE_TRAITS;
        typeTraits[1] = IntegerPointable.TYPE_TRAITS;

		LSMTypeAwareTupleWriterFactory insertTupleWriterFactory = new LSMTypeAwareTupleWriterFactory(
				typeTraits, false);
		ITreeIndexTupleWriter insertTupleWriter = insertTupleWriterFactory.createTupleWriter();
		ITreeIndexTupleReference insertTupleReference = insertTupleWriter.createTupleReference();

		insertTupleReference.resetByTupleOffset(buf, targetOff);

		int num = insertTupleWriter.writeTuple(insertTupleReference, buf, targetOff);

		boolean del = ((LSMTypeAwareTupleReference) insertTupleReference).isDelete();

		if (num == 9 && del == false) {
			return;
		} else {
			fail("fail to write tuple");
		}
	}

	// test delete tuple writer
	@Test
	public void test03() throws Exception {

		int targetOff = 0;
		ByteBuffer buf = ByteBuffer.allocate(32);

		// declare fields
        int fieldCount = 3;
        ITypeTraits[] typeTraits = new ITypeTraits[fieldCount];
        typeTraits[0] = IntegerPointable.TYPE_TRAITS;
        typeTraits[1] = IntegerPointable.TYPE_TRAITS;
        typeTraits[2] = IntegerPointable.TYPE_TRAITS;

		LSMTypeAwareTupleWriterFactory deleteTupleWriterFactory = new LSMTypeAwareTupleWriterFactory(
				typeTraits, true);
		ITreeIndexTupleWriter deleteTupleWriter = deleteTupleWriterFactory.createTupleWriter();
		ITreeIndexTupleReference deleteTupleReference = deleteTupleWriter.createTupleReference();

		deleteTupleReference.resetByTupleOffset(buf, targetOff);

		int num = deleteTupleWriter.writeTuple(deleteTupleReference, buf, targetOff);

		boolean del = ((LSMTypeAwareTupleReference) deleteTupleReference).isDelete();

		if (num == 13 && del == true) {
			return;
		} else {
			fail("fail to write tuple");
		}
	}
}
