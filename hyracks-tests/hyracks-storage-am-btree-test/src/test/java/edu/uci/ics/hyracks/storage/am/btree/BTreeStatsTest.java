package edu.uci.ics.hyracks.storage.am.btree;

import java.io.DataOutput;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Random;

import org.junit.Test;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
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
import edu.uci.ics.hyracks.dataflow.common.data.comparators.IntegerBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.frames.NSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.frames.NSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeOpContext;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.freepage.LinkedListFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.TreeIndexOp;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.am.common.utility.TreeIndexBufferCacheWarmup;
import edu.uci.ics.hyracks.storage.am.common.utility.TreeIndexStats;
import edu.uci.ics.hyracks.storage.am.common.utility.TreeIndexStatsGatherer;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.test.support.TestStorageManagerComponentHolder;
import edu.uci.ics.hyracks.test.support.TestUtils;

public class BTreeStatsTest extends AbstractBTreeTest {

	// private static final int PAGE_SIZE = 256;
	// private static final int NUM_PAGES = 10;
	//private static final int PAGE_SIZE = 32768;
    private static final int PAGE_SIZE = 4096;
    private static final int NUM_PAGES = 1000;
    private static final int MAX_OPEN_FILES = 10;
	private static final int HYRACKS_FRAME_SIZE = 128;
	private IHyracksStageletContext ctx = TestUtils.create(HYRACKS_FRAME_SIZE);
	
	@Test
	public void test01() throws Exception {

		TestStorageManagerComponentHolder.init(PAGE_SIZE, NUM_PAGES, MAX_OPEN_FILES);
		IBufferCache bufferCache = TestStorageManagerComponentHolder
				.getBufferCache(ctx);
		IFileMapProvider fmp = TestStorageManagerComponentHolder
				.getFileMapProvider(ctx);
		FileReference file = new FileReference(new File(fileName));
		bufferCache.createFile(file);
		int fileId = fmp.lookupFileId(file);
		bufferCache.openFile(fileId);

		// declare fields
		int fieldCount = 2;
		ITypeTrait[] typeTraits = new ITypeTrait[fieldCount];
		typeTraits[0] = new TypeTrait(4);
		typeTraits[1] = new TypeTrait(4);

		// declare keys
		int keyFieldCount = 1;
		IBinaryComparator[] cmps = new IBinaryComparator[keyFieldCount];
		cmps[0] = IntegerBinaryComparatorFactory.INSTANCE
				.createBinaryComparator();

		MultiComparator cmp = new MultiComparator(typeTraits, cmps);

		TypeAwareTupleWriterFactory tupleWriterFactory = new TypeAwareTupleWriterFactory(
				typeTraits);
		IBTreeLeafFrameFactory leafFrameFactory = new NSMLeafFrameFactory(
				tupleWriterFactory);
		IBTreeInteriorFrameFactory interiorFrameFactory = new NSMInteriorFrameFactory(
				tupleWriterFactory);
		ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();

		IBTreeLeafFrame leafFrame = leafFrameFactory.getFrame();
		IBTreeInteriorFrame interiorFrame = interiorFrameFactory.getFrame();
		ITreeIndexMetaDataFrame metaFrame = metaFrameFactory.getFrame();

		IFreePageManager freePageManager = new LinkedListFreePageManager(
				bufferCache, fileId, 0, metaFrameFactory);

		BTree btree = new BTree(bufferCache, freePageManager,
				interiorFrameFactory, leafFrameFactory, cmp);
		btree.create(fileId, leafFrame, metaFrame);
		btree.open(fileId);

		Random rnd = new Random();
		rnd.setSeed(50);

		long start = System.currentTimeMillis();

		print("INSERTING INTO TREE\n");

		ByteBuffer frame = ctx.allocateFrame();
		FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
		ArrayTupleBuilder tb = new ArrayTupleBuilder(cmp.getFieldCount());
		DataOutput dos = tb.getDataOutput();

		ISerializerDeserializer[] recDescSers = {
				IntegerSerializerDeserializer.INSTANCE,
				IntegerSerializerDeserializer.INSTANCE };
		RecordDescriptor recDesc = new RecordDescriptor(recDescSers);
		IFrameTupleAccessor accessor = new FrameTupleAccessor(ctx
				.getFrameSize(), recDesc);
		accessor.reset(frame);
		FrameTupleReference tuple = new FrameTupleReference();

		BTreeOpContext insertOpCtx = btree.createOpContext(
				TreeIndexOp.TI_INSERT, leafFrame, interiorFrame, metaFrame);

		// 10000
		for (int i = 0; i < 100000; i++) {

			int f0 = rnd.nextInt() % 100000;
			int f1 = 5;

			tb.reset();
			IntegerSerializerDeserializer.INSTANCE.serialize(f0, dos);
			tb.addFieldEndOffset();
			IntegerSerializerDeserializer.INSTANCE.serialize(f1, dos);
			tb.addFieldEndOffset();

			appender.reset(frame, true);
			appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb
					.getSize());

			tuple.reset(accessor, 0);
			
			if (i % 10000 == 0) {
				long end = System.currentTimeMillis();
				print("INSERTING " + i + " : " + f0 + " " + f1 + " "
						+ (end - start) + "\n");
			}

			try {
				btree.insert(tuple, insertOpCtx);
			} catch (TreeIndexException e) {
			} catch (Exception e) {
				e.printStackTrace();
			}			
		}
		
		TreeIndexStatsGatherer statsGatherer = new TreeIndexStatsGatherer(bufferCache, freePageManager, fileId, btree.getRootPageId());		
		TreeIndexStats stats = statsGatherer.gatherStats(leafFrame, interiorFrame, metaFrame);
		String s = stats.toString();
		System.out.println(s);

		TreeIndexBufferCacheWarmup bufferCacheWarmup = new TreeIndexBufferCacheWarmup(bufferCache, freePageManager, fileId);
		bufferCacheWarmup.warmup(leafFrame, metaFrame, new int[] {1, 2}, new int[] {2, 5});
		
		btree.close();
		bufferCache.closeFile(fileId);
		bufferCache.close();

		print("\n");
	}
}
