/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.storage.am.btree;

import java.io.DataOutput;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.TypeTrait;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.IntegerBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeTupleWriter;
import edu.uci.ics.hyracks.storage.am.btree.api.IPrefixSlotManager;
import edu.uci.ics.hyracks.storage.am.btree.frames.FieldPrefixNSMLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeException;
import edu.uci.ics.hyracks.storage.am.btree.impls.FieldPrefixSlotManager;
import edu.uci.ics.hyracks.storage.am.btree.impls.MultiComparator;
import edu.uci.ics.hyracks.storage.am.btree.tuples.TypeAwareTupleWriter;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICacheMemoryAllocator;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.file.BufferedFileHandle;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.test.support.TestStorageManagerComponentHolder;
import edu.uci.ics.hyracks.test.support.TestUtils;

public class BTreeFieldPrefixNSMTest extends AbstractBTreeTest {

	private static final int PAGE_SIZE = 32768; // 32K
	private static final int NUM_PAGES = 40;
	private static final int HYRACKS_FRAME_SIZE = 128;
	private IHyracksTaskContext ctx = TestUtils.create(HYRACKS_FRAME_SIZE);
	
	public class BufferAllocator implements ICacheMemoryAllocator {
		@Override
		public ByteBuffer[] allocate(int pageSize, int numPages) {
			ByteBuffer[] buffers = new ByteBuffer[numPages];
			for (int i = 0; i < numPages; ++i) {
				buffers[i] = ByteBuffer.allocate(pageSize);
			}
			return buffers;
		}
	}

    private ITupleReference createTuple(IHyracksTaskContext ctx, int f0,
			int f1, int f2, boolean print) throws HyracksDataException {
		if (print)
			System.out.println("CREATING: " + f0 + " " + f1 + " " + f2);

		ByteBuffer buf = ctx.allocateFrame();
		FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
		ArrayTupleBuilder tb = new ArrayTupleBuilder(3);
		DataOutput dos = tb.getDataOutput();

		ISerializerDeserializer[] recDescSers = {
				IntegerSerializerDeserializer.INSTANCE,
				IntegerSerializerDeserializer.INSTANCE,
				IntegerSerializerDeserializer.INSTANCE };
		RecordDescriptor recDesc = new RecordDescriptor(recDescSers);
		IFrameTupleAccessor accessor = new FrameTupleAccessor(ctx
				.getFrameSize(), recDesc);
		accessor.reset(buf);
		FrameTupleReference tuple = new FrameTupleReference();

		tb.reset();
		IntegerSerializerDeserializer.INSTANCE.serialize(f0, dos);
		tb.addFieldEndOffset();
		IntegerSerializerDeserializer.INSTANCE.serialize(f1, dos);
		tb.addFieldEndOffset();
		IntegerSerializerDeserializer.INSTANCE.serialize(f2, dos);
		tb.addFieldEndOffset();

		appender.reset(buf, true);
		appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb
				.getSize());

		tuple.reset(accessor, 0);

		return tuple;
	}

	@Test
	public void test01() throws Exception {
		
		TestStorageManagerComponentHolder.init(PAGE_SIZE, NUM_PAGES);
		IBufferCache bufferCache = TestStorageManagerComponentHolder
				.getBufferCache(ctx);
		IFileMapProvider fmp = TestStorageManagerComponentHolder
				.getFileMapProvider(ctx);
		FileReference file = new FileReference(new File(fileName));
		bufferCache.createFile(file);
		int fileId = fmp.lookupFileId(file);
		bufferCache.openFile(fileId);

		// declare fields
		int fieldCount = 3;
		ITypeTrait[] typeTraits = new ITypeTrait[fieldCount];
		typeTraits[0] = new TypeTrait(4);
		typeTraits[1] = new TypeTrait(4);
		typeTraits[2] = new TypeTrait(4);

		// declare keys
		int keyFieldCount = 3;
		IBinaryComparator[] cmps = new IBinaryComparator[keyFieldCount];
		cmps[0] = IntegerBinaryComparatorFactory.INSTANCE
				.createBinaryComparator();
		cmps[1] = IntegerBinaryComparatorFactory.INSTANCE
				.createBinaryComparator();
		cmps[2] = IntegerBinaryComparatorFactory.INSTANCE
				.createBinaryComparator();
		MultiComparator cmp = new MultiComparator(typeTraits, cmps);

		// just for printing
		ISerializerDeserializer[] sers = {
				IntegerSerializerDeserializer.INSTANCE,
				IntegerSerializerDeserializer.INSTANCE,
				IntegerSerializerDeserializer.INSTANCE };

		Random rnd = new Random();
		rnd.setSeed(50);

		ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(
				fileId, 0), false);
		try {

			IPrefixSlotManager slotManager = new FieldPrefixSlotManager();
			IBTreeTupleWriter tupleWriter = new TypeAwareTupleWriter(typeTraits);
			FieldPrefixNSMLeafFrame frame = new FieldPrefixNSMLeafFrame(
					tupleWriter);
			frame.setPage(page);
			frame.initBuffer((byte) 0);
			slotManager.setFrame(frame);
			frame.setPrefixTupleCount(0);

			String before = new String();
			String after = new String();

			int compactFreq = 5;
			int compressFreq = 5;
			int smallMax = 10;
			int numRecords = 1000;

			int[][] savedFields = new int[numRecords][3];

			// insert records with random calls to compact and compress
			for (int i = 0; i < numRecords; i++) {

				if ((i + 1) % 100 == 0)
					print("INSERTING " + (i + 1) + " / " + numRecords + "\n");

				int a = rnd.nextInt() % smallMax;
				int b = rnd.nextInt() % smallMax;
				int c = i;

				ITupleReference tuple = createTuple(ctx, a, b, c, false);
				try {
					frame.insert(tuple, cmp);
				} catch (BTreeException e) {
					e.printStackTrace();
				} catch (Exception e) {
					e.printStackTrace();
				}

				savedFields[i][0] = a;
				savedFields[i][1] = b;
				savedFields[i][2] = c;

				if (rnd.nextInt() % compactFreq == 0) {
					before = frame.printKeys(cmp, sers);
					frame.compact(cmp);
					after = frame.printKeys(cmp, sers);
					Assert.assertEquals(before, after);
				}

				if (rnd.nextInt() % compressFreq == 0) {
					before = frame.printKeys(cmp, sers);
					frame.compress(cmp);
					after = frame.printKeys(cmp, sers);
					Assert.assertEquals(before, after);
				}

			}

			// delete records with random calls to compact and compress
			for (int i = 0; i < numRecords; i++) {

				if ((i + 1) % 100 == 0)
					print("DELETING " + (i + 1) + " / " + numRecords + "\n");

				ITupleReference tuple = createTuple(ctx,
						savedFields[i][0], savedFields[i][1],
						savedFields[i][2], false);
				try {
					frame.delete(tuple, cmp, true);
				} catch (Exception e) {
				}

				if (rnd.nextInt() % compactFreq == 0) {
					before = frame.printKeys(cmp, sers);
					frame.compact(cmp);
					after = frame.printKeys(cmp, sers);
					Assert.assertEquals(before, after);
				}

				if (rnd.nextInt() % compressFreq == 0) {
					before = frame.printKeys(cmp, sers);
					frame.compress(cmp);
					after = frame.printKeys(cmp, sers);
					Assert.assertEquals(before, after);
				}
			}

		} finally {
			bufferCache.unpin(page);
		}

		bufferCache.closeFile(fileId);
		bufferCache.close();
	}
}
