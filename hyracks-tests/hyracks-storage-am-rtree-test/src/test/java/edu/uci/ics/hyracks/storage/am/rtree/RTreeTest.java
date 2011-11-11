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

package edu.uci.ics.hyracks.storage.am.rtree;

import java.io.DataOutput;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.logging.Level;

import org.junit.Test;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.TypeTrait;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.DoubleBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.IntegerBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.util.TupleUtils;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.freepage.LinkedListFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.impls.TreeDiskOrderScanCursor;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.common.util.TreeIndexStats;
import edu.uci.ics.hyracks.storage.am.common.util.TreeIndexStatsGatherer;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeFrame;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreeNSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreeNSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTree;
import edu.uci.ics.hyracks.storage.am.rtree.tuples.RTreeTypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.am.rtree.util.RTreeUtils;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.test.support.TestStorageManagerComponentHolder;
import edu.uci.ics.hyracks.test.support.TestUtils;

public class RTreeTest extends AbstractRTreeTest {
	private static final int PAGE_SIZE = 256;
	private static final int NUM_PAGES = 10;
	private static final int MAX_OPEN_FILES = 10;
	private static final int HYRACKS_FRAME_SIZE = 128;
	private IHyracksTaskContext ctx = TestUtils.create(HYRACKS_FRAME_SIZE);

	// create an R-tree of two dimensions
	// fill the R-tree with random values using insertions
	// perform ordered scan
	@Test
	public void test01() throws Exception {

		TestStorageManagerComponentHolder.init(PAGE_SIZE, NUM_PAGES,
				MAX_OPEN_FILES);
		IBufferCache bufferCache = TestStorageManagerComponentHolder
				.getBufferCache(ctx);
		IFileMapProvider fmp = TestStorageManagerComponentHolder
				.getFileMapProvider(ctx);
		FileReference file = new FileReference(new File(fileName));
		bufferCache.createFile(file);
		int fileId = fmp.lookupFileId(file);
		bufferCache.openFile(fileId);

		// declare keys
		int keyFieldCount = 4;
		IBinaryComparator[] cmps = new IBinaryComparator[keyFieldCount];
		cmps[0] = DoubleBinaryComparatorFactory.INSTANCE
				.createBinaryComparator();
		cmps[1] = cmps[0];
		cmps[2] = cmps[0];
		cmps[3] = cmps[0];

		// declare tuple fields
		int fieldCount = 7;
		ITypeTrait[] typeTraits = new ITypeTrait[fieldCount];
		typeTraits[0] = new TypeTrait(8);
		typeTraits[1] = new TypeTrait(8);
		typeTraits[2] = new TypeTrait(8);
		typeTraits[3] = new TypeTrait(8);
		typeTraits[4] = new TypeTrait(8);
		typeTraits[5] = new TypeTrait(4);
		typeTraits[6] = new TypeTrait(8);

		// create value providers
		IPrimitiveValueProviderFactory[] valueProviderFactories = RTreeUtils.comparatorsToPrimitiveValueProviderFactories(cmps); 

		MultiComparator cmp = new MultiComparator(cmps);

		RTreeTypeAwareTupleWriterFactory tupleWriterFactory = new RTreeTypeAwareTupleWriterFactory(
				typeTraits);

		ITreeIndexFrameFactory interiorFrameFactory = new RTreeNSMInteriorFrameFactory(
				tupleWriterFactory, valueProviderFactories);
		ITreeIndexFrameFactory leafFrameFactory = new RTreeNSMLeafFrameFactory(
				tupleWriterFactory, valueProviderFactories);
		ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();
		ITreeIndexMetaDataFrame metaFrame = metaFrameFactory.createFrame();

		IRTreeFrame interiorFrame = (IRTreeFrame) interiorFrameFactory
				.createFrame();
		IRTreeFrame leafFrame = (IRTreeFrame) leafFrameFactory.createFrame();
		IFreePageManager freePageManager = new LinkedListFreePageManager(
				bufferCache, fileId, 0, metaFrameFactory);

		RTree rtree = new RTree(bufferCache, fieldCount, cmp, freePageManager,
				interiorFrameFactory, leafFrameFactory);
		rtree.create(fileId);
		rtree.open(fileId);

		ByteBuffer hyracksFrame = ctx.allocateFrame();
		FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
		ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
		DataOutput dos = tb.getDataOutput();

		@SuppressWarnings("rawtypes")
		ISerializerDeserializer[] recDescSers = {
				DoubleSerializerDeserializer.INSTANCE,
				DoubleSerializerDeserializer.INSTANCE,
				DoubleSerializerDeserializer.INSTANCE,
				DoubleSerializerDeserializer.INSTANCE,
				DoubleSerializerDeserializer.INSTANCE,
				IntegerSerializerDeserializer.INSTANCE,
				DoubleSerializerDeserializer.INSTANCE };
		RecordDescriptor recDesc = new RecordDescriptor(recDescSers);
		IFrameTupleAccessor accessor = new FrameTupleAccessor(
				ctx.getFrameSize(), recDesc);
		accessor.reset(hyracksFrame);
		FrameTupleReference tuple = new FrameTupleReference();

		ITreeIndexAccessor indexAccessor = rtree.createAccessor();

		Random rnd = new Random();
		rnd.setSeed(50);

		Random rnd2 = new Random();
		rnd2.setSeed(50);
		for (int i = 0; i < 5000; i++) {

			double p1x = rnd.nextDouble();
			double p1y = rnd.nextDouble();
			double p2x = rnd.nextDouble();
			double p2y = rnd.nextDouble();

			double pk1 = rnd2.nextDouble();
			int pk2 = rnd2.nextInt();
			double pk3 = rnd2.nextDouble();

			tb.reset();
			DoubleSerializerDeserializer.INSTANCE.serialize(Math.min(p1x, p2x),
					dos);
			tb.addFieldEndOffset();
			DoubleSerializerDeserializer.INSTANCE.serialize(Math.min(p1y, p2y),
					dos);
			tb.addFieldEndOffset();
			DoubleSerializerDeserializer.INSTANCE.serialize(Math.max(p1x, p2x),
					dos);
			tb.addFieldEndOffset();
			DoubleSerializerDeserializer.INSTANCE.serialize(Math.max(p1y, p2y),
					dos);
			tb.addFieldEndOffset();
			DoubleSerializerDeserializer.INSTANCE.serialize(pk1, dos);
			tb.addFieldEndOffset();
			IntegerSerializerDeserializer.INSTANCE.serialize(pk2, dos);
			tb.addFieldEndOffset();
			DoubleSerializerDeserializer.INSTANCE.serialize(pk3, dos);
			tb.addFieldEndOffset();

			appender.reset(hyracksFrame, true);
			appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0,
					tb.getSize());

			tuple.reset(accessor, 0);

			if (LOGGER.isLoggable(Level.INFO)) {
				if (i % 1000 == 0) {
					LOGGER.info("INSERTING " + i + " " + Math.min(p1x, p2x)
							+ " " + Math.min(p1y, p2y) + " "
							+ Math.max(p1x, p2x) + " " + Math.max(p1y, p2y));
				}
			}

			try {
				indexAccessor.insert(tuple);
			} catch (TreeIndexException e) {
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		// rtree.printTree(leafFrame, interiorFrame, recDescSers);
		// System.err.println();

		String rtreeStats = rtree.printStats();
		if (LOGGER.isLoggable(Level.INFO)) {
			LOGGER.info(rtreeStats);
		}

		// disk-order scan
		if (LOGGER.isLoggable(Level.INFO)) {
			LOGGER.info("DISK-ORDER SCAN:");
		}
		TreeDiskOrderScanCursor diskOrderCursor = new TreeDiskOrderScanCursor(
				leafFrame);
		indexAccessor.diskOrderScan(diskOrderCursor);
		try {
			while (diskOrderCursor.hasNext()) {
				diskOrderCursor.next();
				ITupleReference frameTuple = diskOrderCursor.getTuple();
				String rec = TupleUtils.printTuple(frameTuple, recDescSers);
				if (LOGGER.isLoggable(Level.INFO)) {
					LOGGER.info(rec);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			diskOrderCursor.close();
		}

		TreeIndexStatsGatherer statsGatherer = new TreeIndexStatsGatherer(
				bufferCache, freePageManager, fileId, rtree.getRootPageId());
		TreeIndexStats stats = statsGatherer.gatherStats(leafFrame,
				interiorFrame, metaFrame);
		String string = stats.toString();
		System.err.println(string);

		rtree.close();
		bufferCache.closeFile(fileId);
		bufferCache.close();

	}

	// create an R-tree of two dimensions
	// fill the R-tree with random values using insertions
	// and then delete all the tuples which result of an empty R-tree
	@Test
	public void test02() throws Exception {

		TestStorageManagerComponentHolder.init(PAGE_SIZE, NUM_PAGES,
				MAX_OPEN_FILES);
		IBufferCache bufferCache = TestStorageManagerComponentHolder
				.getBufferCache(ctx);
		IFileMapProvider fmp = TestStorageManagerComponentHolder
				.getFileMapProvider(ctx);
		FileReference file = new FileReference(new File(fileName));
		bufferCache.createFile(file);
		int fileId = fmp.lookupFileId(file);
		bufferCache.openFile(fileId);

		// declare keys
		int keyFieldCount = 4;
		IBinaryComparator[] cmps = new IBinaryComparator[keyFieldCount];
		cmps[0] = DoubleBinaryComparatorFactory.INSTANCE
				.createBinaryComparator();
		cmps[1] = cmps[0];
		cmps[2] = cmps[0];
		cmps[3] = cmps[0];

		// declare tuple fields
		int fieldCount = 7;
		ITypeTrait[] typeTraits = new ITypeTrait[fieldCount];
		typeTraits[0] = new TypeTrait(8);
		typeTraits[1] = new TypeTrait(8);
		typeTraits[2] = new TypeTrait(8);
		typeTraits[3] = new TypeTrait(8);
		typeTraits[4] = new TypeTrait(8);
		typeTraits[5] = new TypeTrait(4);
		typeTraits[6] = new TypeTrait(8);

		// create value providers
		IPrimitiveValueProviderFactory[] valueProviderFactories = RTreeUtils.comparatorsToPrimitiveValueProviderFactories(cmps); 

		MultiComparator cmp = new MultiComparator(cmps);

		RTreeTypeAwareTupleWriterFactory tupleWriterFactory = new RTreeTypeAwareTupleWriterFactory(
				typeTraits);

		ITreeIndexFrameFactory interiorFrameFactory = new RTreeNSMInteriorFrameFactory(
				tupleWriterFactory, valueProviderFactories);
		ITreeIndexFrameFactory leafFrameFactory = new RTreeNSMLeafFrameFactory(
				tupleWriterFactory, valueProviderFactories);
		ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();
		ITreeIndexMetaDataFrame metaFrame = metaFrameFactory.createFrame();

		IRTreeFrame interiorFrame = (IRTreeFrame) interiorFrameFactory
				.createFrame();
		IRTreeFrame leafFrame = (IRTreeFrame) leafFrameFactory.createFrame();
		IFreePageManager freePageManager = new LinkedListFreePageManager(
				bufferCache, fileId, 0, metaFrameFactory);

		RTree rtree = new RTree(bufferCache, fieldCount, cmp, freePageManager,
				interiorFrameFactory, leafFrameFactory);
		rtree.create(fileId);
		rtree.open(fileId);

		ByteBuffer hyracksFrame = ctx.allocateFrame();
		FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
		ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
		DataOutput dos = tb.getDataOutput();

		@SuppressWarnings("rawtypes")
		ISerializerDeserializer[] recDescSers = {
				DoubleSerializerDeserializer.INSTANCE,
				DoubleSerializerDeserializer.INSTANCE,
				DoubleSerializerDeserializer.INSTANCE,
				DoubleSerializerDeserializer.INSTANCE,
				DoubleSerializerDeserializer.INSTANCE,
				IntegerSerializerDeserializer.INSTANCE,
				DoubleSerializerDeserializer.INSTANCE };
		RecordDescriptor recDesc = new RecordDescriptor(recDescSers);
		IFrameTupleAccessor accessor = new FrameTupleAccessor(
				ctx.getFrameSize(), recDesc);
		accessor.reset(hyracksFrame);
		FrameTupleReference tuple = new FrameTupleReference();

		ITreeIndexAccessor indexAccessor = rtree.createAccessor();

		Random rnd = new Random();
		rnd.setSeed(50);

		for (int i = 0; i < 5000; i++) {

			double p1x = rnd.nextDouble();
			double p1y = rnd.nextDouble();
			double p2x = rnd.nextDouble();
			double p2y = rnd.nextDouble();

			double pk1 = rnd.nextDouble();
			int pk2 = rnd.nextInt();
			double pk3 = rnd.nextDouble();

			tb.reset();
			DoubleSerializerDeserializer.INSTANCE.serialize(Math.min(p1x, p2x),
					dos);
			tb.addFieldEndOffset();
			DoubleSerializerDeserializer.INSTANCE.serialize(Math.min(p1y, p2y),
					dos);
			tb.addFieldEndOffset();
			DoubleSerializerDeserializer.INSTANCE.serialize(Math.max(p1x, p2x),
					dos);
			tb.addFieldEndOffset();
			DoubleSerializerDeserializer.INSTANCE.serialize(Math.max(p1y, p2y),
					dos);
			tb.addFieldEndOffset();
			DoubleSerializerDeserializer.INSTANCE.serialize(pk1, dos);
			tb.addFieldEndOffset();
			IntegerSerializerDeserializer.INSTANCE.serialize(pk2, dos);
			tb.addFieldEndOffset();
			DoubleSerializerDeserializer.INSTANCE.serialize(pk3, dos);
			tb.addFieldEndOffset();

			appender.reset(hyracksFrame, true);
			appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0,
					tb.getSize());

			tuple.reset(accessor, 0);

			if (LOGGER.isLoggable(Level.INFO)) {
				if (i % 1000 == 0) {
					LOGGER.info("INSERTING " + i + " " + Math.min(p1x, p2x) + " "
							+ Math.min(p1y, p2y) + " " + Math.max(p1x, p2x)
							+ " " + Math.max(p1y, p2y));
				}
			}

			try {
				indexAccessor.insert(tuple);
			} catch (TreeIndexException e) {
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		// rtree.printTree(leafFrame, interiorFrame, recDescSers);
		// System.err.println();

		String rtreeStats = rtree.printStats();
		if (LOGGER.isLoggable(Level.INFO)) {
			LOGGER.info(rtreeStats);
		}

		rnd.setSeed(50);
		for (int i = 0; i < 5000; i++) {

			double p1x = rnd.nextDouble();
			double p1y = rnd.nextDouble();
			double p2x = rnd.nextDouble();
			double p2y = rnd.nextDouble();

			double pk1 = rnd.nextDouble();
			int pk2 = rnd.nextInt();
			double pk3 = rnd.nextDouble();

			tb.reset();
			DoubleSerializerDeserializer.INSTANCE.serialize(Math.min(p1x, p2x),
					dos);
			tb.addFieldEndOffset();
			DoubleSerializerDeserializer.INSTANCE.serialize(Math.min(p1y, p2y),
					dos);
			tb.addFieldEndOffset();
			DoubleSerializerDeserializer.INSTANCE.serialize(Math.max(p1x, p2x),
					dos);
			tb.addFieldEndOffset();
			DoubleSerializerDeserializer.INSTANCE.serialize(Math.max(p1y, p2y),
					dos);
			tb.addFieldEndOffset();
			DoubleSerializerDeserializer.INSTANCE.serialize(pk1, dos);
			tb.addFieldEndOffset();
			IntegerSerializerDeserializer.INSTANCE.serialize(pk2, dos);
			tb.addFieldEndOffset();
			DoubleSerializerDeserializer.INSTANCE.serialize(pk3, dos);
			tb.addFieldEndOffset();

			appender.reset(hyracksFrame, true);
			appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0,
					tb.getSize());

			tuple.reset(accessor, 0);
			
			if (LOGGER.isLoggable(Level.INFO)) {
				if (i % 1000 == 0) {
					LOGGER.info("DELETING " + i + " " + Math.min(p1x, p2x) + " "
							+ Math.min(p1y, p2y) + " " + Math.max(p1x, p2x)
							+ " " + Math.max(p1y, p2y));
				}
			}

			try {
				indexAccessor.delete(tuple);
			} catch (TreeIndexException e) {
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		TreeIndexStatsGatherer statsGatherer = new TreeIndexStatsGatherer(
				bufferCache, freePageManager, fileId, rtree.getRootPageId());
		TreeIndexStats stats = statsGatherer.gatherStats(leafFrame,
				interiorFrame, metaFrame);
		String string = stats.toString();
		System.err.println(string);

		rtree.close();
		bufferCache.closeFile(fileId);
		bufferCache.close();

	}

	// create an R-tree of three dimensions
	// fill the R-tree with random values using insertions
	// perform ordered scan
	@Test
	public void test03() throws Exception {

		TestStorageManagerComponentHolder.init(PAGE_SIZE, NUM_PAGES,
				MAX_OPEN_FILES);
		IBufferCache bufferCache = TestStorageManagerComponentHolder
				.getBufferCache(ctx);
		IFileMapProvider fmp = TestStorageManagerComponentHolder
				.getFileMapProvider(ctx);
		FileReference file = new FileReference(new File(fileName));
		bufferCache.createFile(file);
		int fileId = fmp.lookupFileId(file);
		bufferCache.openFile(fileId);

		// declare keys
		int keyFieldCount = 6;
		IBinaryComparator[] cmps = new IBinaryComparator[keyFieldCount];
		cmps[0] = DoubleBinaryComparatorFactory.INSTANCE
				.createBinaryComparator();
		cmps[1] = cmps[0];
		cmps[2] = cmps[0];
		cmps[3] = cmps[0];
		cmps[4] = cmps[0];
		cmps[5] = cmps[0];

		// declare tuple fields
		int fieldCount = 9;
		ITypeTrait[] typeTraits = new ITypeTrait[fieldCount];
		typeTraits[0] = new TypeTrait(8);
		typeTraits[1] = new TypeTrait(8);
		typeTraits[2] = new TypeTrait(8);
		typeTraits[3] = new TypeTrait(8);
		typeTraits[4] = new TypeTrait(8);
		typeTraits[5] = new TypeTrait(8);
		typeTraits[6] = new TypeTrait(8);
		typeTraits[7] = new TypeTrait(4);
		typeTraits[8] = new TypeTrait(8);

		// create value providers
		IPrimitiveValueProviderFactory[] valueProviderFactories = RTreeUtils.comparatorsToPrimitiveValueProviderFactories(cmps); 

		MultiComparator cmp = new MultiComparator(cmps);

		RTreeTypeAwareTupleWriterFactory tupleWriterFactory = new RTreeTypeAwareTupleWriterFactory(
				typeTraits);

		ITreeIndexFrameFactory interiorFrameFactory = new RTreeNSMInteriorFrameFactory(
				tupleWriterFactory, valueProviderFactories);
		ITreeIndexFrameFactory leafFrameFactory = new RTreeNSMLeafFrameFactory(
				tupleWriterFactory, valueProviderFactories);
		ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();
		ITreeIndexMetaDataFrame metaFrame = metaFrameFactory.createFrame();

		IRTreeFrame interiorFrame = (IRTreeFrame) interiorFrameFactory
				.createFrame();
		IRTreeFrame leafFrame = (IRTreeFrame) leafFrameFactory.createFrame();
		IFreePageManager freePageManager = new LinkedListFreePageManager(
				bufferCache, fileId, 0, metaFrameFactory);

		RTree rtree = new RTree(bufferCache, fieldCount, cmp, freePageManager,
				interiorFrameFactory, leafFrameFactory);
		rtree.create(fileId);
		rtree.open(fileId);

		ByteBuffer hyracksFrame = ctx.allocateFrame();
		FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
		ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
		DataOutput dos = tb.getDataOutput();

		@SuppressWarnings("rawtypes")
		ISerializerDeserializer[] recDescSers = {
				DoubleSerializerDeserializer.INSTANCE,
				DoubleSerializerDeserializer.INSTANCE,
				DoubleSerializerDeserializer.INSTANCE,
				DoubleSerializerDeserializer.INSTANCE,
				DoubleSerializerDeserializer.INSTANCE,
				DoubleSerializerDeserializer.INSTANCE,
				DoubleSerializerDeserializer.INSTANCE,
				IntegerSerializerDeserializer.INSTANCE,
				DoubleSerializerDeserializer.INSTANCE };
		RecordDescriptor recDesc = new RecordDescriptor(recDescSers);
		IFrameTupleAccessor accessor = new FrameTupleAccessor(
				ctx.getFrameSize(), recDesc);
		accessor.reset(hyracksFrame);
		FrameTupleReference tuple = new FrameTupleReference();

		ITreeIndexAccessor indexAccessor = rtree.createAccessor();

		Random rnd = new Random();
		rnd.setSeed(50);

		for (int i = 0; i < 5000; i++) {

			double p1x = rnd.nextDouble();
			double p1y = rnd.nextDouble();
			double p1z = rnd.nextDouble();
			double p2x = rnd.nextDouble();
			double p2y = rnd.nextDouble();
			double p2z = rnd.nextDouble();

			double pk1 = rnd.nextDouble();
			int pk2 = rnd.nextInt();
			double pk3 = rnd.nextDouble();

			tb.reset();
			DoubleSerializerDeserializer.INSTANCE.serialize(Math.min(p1x, p2x),
					dos);
			tb.addFieldEndOffset();
			DoubleSerializerDeserializer.INSTANCE.serialize(Math.min(p1y, p2y),
					dos);
			tb.addFieldEndOffset();
			DoubleSerializerDeserializer.INSTANCE.serialize(Math.min(p1z, p2z),
					dos);
			tb.addFieldEndOffset();
			DoubleSerializerDeserializer.INSTANCE.serialize(Math.max(p1x, p2x),
					dos);
			tb.addFieldEndOffset();
			DoubleSerializerDeserializer.INSTANCE.serialize(Math.max(p1y, p2y),
					dos);
			tb.addFieldEndOffset();
			DoubleSerializerDeserializer.INSTANCE.serialize(Math.max(p1z, p2z),
					dos);
			tb.addFieldEndOffset();
			DoubleSerializerDeserializer.INSTANCE.serialize(pk1, dos);
			tb.addFieldEndOffset();
			IntegerSerializerDeserializer.INSTANCE.serialize(pk2, dos);
			tb.addFieldEndOffset();
			DoubleSerializerDeserializer.INSTANCE.serialize(pk3, dos);
			tb.addFieldEndOffset();

			appender.reset(hyracksFrame, true);
			appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0,
					tb.getSize());

			tuple.reset(accessor, 0);

			if (LOGGER.isLoggable(Level.INFO)) {
				if (i % 1000 == 0) {
					LOGGER.info("INSERTING " + i + " " + Math.min(p1x, p2x) + " "
							+ Math.min(p1y, p2y) + " " + Math.min(p1z, p2z)
							+ " " + " " + Math.max(p1x, p2x) + " "
							+ Math.max(p1y, p2y) + " " + Math.max(p1z, p2z));
				}
			}

			try {
				indexAccessor.insert(tuple);
			} catch (TreeIndexException e) {
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		// rtree.printTree(leafFrame, interiorFrame, recDescSers);
		// System.err.println();

		String rtreeStats = rtree.printStats();
		if (LOGGER.isLoggable(Level.INFO)) {
			LOGGER.info(rtreeStats);
		}

		// disk-order scan
		if (LOGGER.isLoggable(Level.INFO)) {
			LOGGER.info("DISK-ORDER SCAN:");
		}
		TreeDiskOrderScanCursor diskOrderCursor = new TreeDiskOrderScanCursor(
				leafFrame);
		indexAccessor.diskOrderScan(diskOrderCursor);
		try {
			while (diskOrderCursor.hasNext()) {
				diskOrderCursor.next();
				ITupleReference frameTuple = diskOrderCursor.getTuple();
				String rec = TupleUtils.printTuple(frameTuple, recDescSers);
				if (LOGGER.isLoggable(Level.INFO)) {
					LOGGER.info(rec);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			diskOrderCursor.close();
		}

		TreeIndexStatsGatherer statsGatherer = new TreeIndexStatsGatherer(
				bufferCache, freePageManager, fileId, rtree.getRootPageId());
		TreeIndexStats stats = statsGatherer.gatherStats(leafFrame,
				interiorFrame, metaFrame);
		String string = stats.toString();
		System.err.println(string);

		rtree.close();
		bufferCache.closeFile(fileId);
		bufferCache.close();

	}

	// create an R-tree of two dimensions
	// fill the R-tree with random integer key values using insertions
	// perform ordered scan
	@Test
	public void test04() throws Exception {

		TestStorageManagerComponentHolder.init(PAGE_SIZE, NUM_PAGES,
				MAX_OPEN_FILES);
		IBufferCache bufferCache = TestStorageManagerComponentHolder
				.getBufferCache(ctx);
		IFileMapProvider fmp = TestStorageManagerComponentHolder
				.getFileMapProvider(ctx);
		FileReference file = new FileReference(new File(fileName));
		bufferCache.createFile(file);
		int fileId = fmp.lookupFileId(file);
		bufferCache.openFile(fileId);

		// declare keys
		int keyFieldCount = 4;
		IBinaryComparator[] cmps = new IBinaryComparator[keyFieldCount];
		cmps[0] = IntegerBinaryComparatorFactory.INSTANCE
				.createBinaryComparator();
		cmps[1] = cmps[0];
		cmps[2] = cmps[0];
		cmps[3] = cmps[0];

		// declare tuple fields
		int fieldCount = 7;
		ITypeTrait[] typeTraits = new ITypeTrait[fieldCount];
		typeTraits[0] = new TypeTrait(4);
		typeTraits[1] = new TypeTrait(4);
		typeTraits[2] = new TypeTrait(4);
		typeTraits[3] = new TypeTrait(4);
		typeTraits[4] = new TypeTrait(8);
		typeTraits[5] = new TypeTrait(4);
		typeTraits[6] = new TypeTrait(8);

		// create value providers
		IPrimitiveValueProviderFactory[] valueProviderFactories = RTreeUtils.comparatorsToPrimitiveValueProviderFactories(cmps); 

		MultiComparator cmp = new MultiComparator(cmps);

		RTreeTypeAwareTupleWriterFactory tupleWriterFactory = new RTreeTypeAwareTupleWriterFactory(
				typeTraits);

		ITreeIndexFrameFactory interiorFrameFactory = new RTreeNSMInteriorFrameFactory(
				tupleWriterFactory, valueProviderFactories);
		ITreeIndexFrameFactory leafFrameFactory = new RTreeNSMLeafFrameFactory(
				tupleWriterFactory, valueProviderFactories);
		ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();
		ITreeIndexMetaDataFrame metaFrame = metaFrameFactory.createFrame();

		IRTreeFrame interiorFrame = (IRTreeFrame) interiorFrameFactory
				.createFrame();
		IRTreeFrame leafFrame = (IRTreeFrame) leafFrameFactory.createFrame();
		IFreePageManager freePageManager = new LinkedListFreePageManager(
				bufferCache, fileId, 0, metaFrameFactory);

		RTree rtree = new RTree(bufferCache, fieldCount, cmp, freePageManager,
				interiorFrameFactory, leafFrameFactory);
		rtree.create(fileId);
		rtree.open(fileId);

		ByteBuffer hyracksFrame = ctx.allocateFrame();
		FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
		ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
		DataOutput dos = tb.getDataOutput();

		@SuppressWarnings("rawtypes")
		ISerializerDeserializer[] recDescSers = {
				IntegerSerializerDeserializer.INSTANCE,
				IntegerSerializerDeserializer.INSTANCE,
				IntegerSerializerDeserializer.INSTANCE,
				IntegerSerializerDeserializer.INSTANCE,
				DoubleSerializerDeserializer.INSTANCE,
				IntegerSerializerDeserializer.INSTANCE,
				DoubleSerializerDeserializer.INSTANCE };
		RecordDescriptor recDesc = new RecordDescriptor(recDescSers);
		IFrameTupleAccessor accessor = new FrameTupleAccessor(
				ctx.getFrameSize(), recDesc);
		accessor.reset(hyracksFrame);
		FrameTupleReference tuple = new FrameTupleReference();

		ITreeIndexAccessor indexAccessor = rtree.createAccessor();

		Random rnd = new Random();
		rnd.setSeed(50);

		Random rnd2 = new Random();
		rnd2.setSeed(50);
		for (int i = 0; i < 5000; i++) {

			int p1x = rnd.nextInt();
			int p1y = rnd.nextInt();
			int p2x = rnd.nextInt();
			int p2y = rnd.nextInt();

			double pk1 = rnd2.nextDouble();
			int pk2 = rnd2.nextInt();
			double pk3 = rnd2.nextDouble();

			tb.reset();
			IntegerSerializerDeserializer.INSTANCE.serialize(
					Math.min(p1x, p2x), dos);
			tb.addFieldEndOffset();
			IntegerSerializerDeserializer.INSTANCE.serialize(
					Math.min(p1y, p2y), dos);
			tb.addFieldEndOffset();
			IntegerSerializerDeserializer.INSTANCE.serialize(
					Math.max(p1x, p2x), dos);
			tb.addFieldEndOffset();
			IntegerSerializerDeserializer.INSTANCE.serialize(
					Math.max(p1y, p2y), dos);
			tb.addFieldEndOffset();
			DoubleSerializerDeserializer.INSTANCE.serialize(pk1, dos);
			tb.addFieldEndOffset();
			IntegerSerializerDeserializer.INSTANCE.serialize(pk2, dos);
			tb.addFieldEndOffset();
			DoubleSerializerDeserializer.INSTANCE.serialize(pk3, dos);
			tb.addFieldEndOffset();

			appender.reset(hyracksFrame, true);
			appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0,
					tb.getSize());

			tuple.reset(accessor, 0);

			if (LOGGER.isLoggable(Level.INFO)) {
				if (i % 1000 == 0) {
					LOGGER.info("INSERTING " + i + " " + Math.min(p1x, p2x) + " "
							+ Math.min(p1y, p2y) + " " + Math.max(p1x, p2x)
							+ " " + Math.max(p1y, p2y));
				}
			}

			try {
				indexAccessor.insert(tuple);
			} catch (TreeIndexException e) {
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		// rtree.printTree(leafFrame, interiorFrame, recDescSers);
		// System.err.println();

		String rtreeStats = rtree.printStats();
		if (LOGGER.isLoggable(Level.INFO)) {
			LOGGER.info(rtreeStats);
		}

		// disk-order scan
		if (LOGGER.isLoggable(Level.INFO)) {
			LOGGER.info("DISK-ORDER SCAN:");
		}
		TreeDiskOrderScanCursor diskOrderCursor = new TreeDiskOrderScanCursor(
				leafFrame);
		indexAccessor.diskOrderScan(diskOrderCursor);
		try {
			while (diskOrderCursor.hasNext()) {
				diskOrderCursor.next();
				ITupleReference frameTuple = diskOrderCursor.getTuple();
				String rec = TupleUtils.printTuple(frameTuple, recDescSers);
				if (LOGGER.isLoggable(Level.INFO)) {
					LOGGER.info(rec);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			diskOrderCursor.close();
		}

		TreeIndexStatsGatherer statsGatherer = new TreeIndexStatsGatherer(
				bufferCache, freePageManager, fileId, rtree.getRootPageId());
		TreeIndexStats stats = statsGatherer.gatherStats(leafFrame,
				interiorFrame, metaFrame);
		String string = stats.toString();
		System.err.println(string);

		rtree.close();
		bufferCache.closeFile(fileId);
		bufferCache.close();

	}
}