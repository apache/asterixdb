package edu.uci.ics.hyracks.storage.am.invertedindex;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Random;

import junit.framework.Assert;

import org.junit.Test;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.api.dataflow.value.TypeTrait;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.FixedSizeFrameTupleAccessor;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.FixedSizeFrameTupleAppender;

public class FixedSizeFrameTupleTest {

	private static int FRAME_SIZE = 4096;
	
	private Random rnd = new Random(50);
	
	@Test
    public void singleFieldTest() throws Exception {
		ByteBuffer buffer = ByteBuffer.allocate(FRAME_SIZE);
		
		ITypeTrait[] fields = new TypeTrait[1];
		fields[0] = new TypeTrait(4);
		
		FixedSizeFrameTupleAppender ftapp = new FixedSizeFrameTupleAppender(FRAME_SIZE, fields);
		FixedSizeFrameTupleAccessor ftacc = new FixedSizeFrameTupleAccessor(FRAME_SIZE, fields);
		
		boolean frameHasSpace = true;		
		
		ArrayList<Integer> check = new ArrayList<Integer>();
		
		ftapp.reset(buffer, true);
		while(frameHasSpace) {			
			int val = rnd.nextInt();
			frameHasSpace = ftapp.append(val);
			if(frameHasSpace) {
				check.add(val);
				ftapp.incrementTupleCount(1);
			}
		}
		
		ftacc.reset(buffer);
		System.out.println("TUPLECOUNT: " + ftacc.getTupleCount());
		System.out.println("CHECKCOUNT: " + check.size());
		for(int i = 0; i < ftacc.getTupleCount(); i++) {
			int val = IntegerSerializerDeserializer.INSTANCE.getInt(ftacc.getBuffer().array(), ftacc.getTupleStartOffset(i));			
			Assert.assertEquals(check.get(i).intValue(), val);						
		}		
	}
}
