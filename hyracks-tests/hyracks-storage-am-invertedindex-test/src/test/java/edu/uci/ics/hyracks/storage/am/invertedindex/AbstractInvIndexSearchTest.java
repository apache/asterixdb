package edu.uci.ics.hyracks.storage.am.invertedindex;

import java.io.DataOutput;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Random;

import org.junit.After;
import org.junit.Before;

import edu.uci.ics.fuzzyjoin.tokenizer.IBinaryTokenizer;
import edu.uci.ics.fuzzyjoin.tokenizer.ITokenFactory;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
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
import edu.uci.ics.hyracks.dataflow.common.data.comparators.IntegerBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.UTF8StringBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.freepage.LinkedListFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndexResultCursor;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.InvertedIndex;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.TOccurrenceSearcher;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.test.support.TestStorageManagerComponentHolder;
import edu.uci.ics.hyracks.test.support.TestUtils;

public abstract class AbstractInvIndexSearchTest extends AbstractInvIndexTest {
	protected final int PAGE_SIZE = 32768;
	protected final int NUM_PAGES = 100;
	protected final int MAX_OPEN_FILES = 10;
	protected final int HYRACKS_FRAME_SIZE = 32768;
	protected IHyracksStageletContext stageletCtx = TestUtils
			.create(HYRACKS_FRAME_SIZE);

	protected IBufferCache bufferCache;
	protected IFileMapProvider fmp;

	// --- BTREE ---

	// create file refs
	protected FileReference btreeFile = new FileReference(new File(
			btreeFileName));
	protected int btreeFileId;

	// declare btree fields
	protected int fieldCount = 5;
	protected ITypeTrait[] typeTraits = new ITypeTrait[fieldCount];

	// declare btree keys
	protected int btreeKeyFieldCount = 1;
	protected IBinaryComparator[] btreeBinCmps = new IBinaryComparator[btreeKeyFieldCount];
	protected MultiComparator btreeCmp = new MultiComparator(typeTraits,
			btreeBinCmps);

	// btree frame factories
	protected TypeAwareTupleWriterFactory tupleWriterFactory = new TypeAwareTupleWriterFactory(
			typeTraits);
	protected ITreeIndexFrameFactory leafFrameFactory = new BTreeNSMLeafFrameFactory(
			tupleWriterFactory);
	protected ITreeIndexFrameFactory interiorFrameFactory = new BTreeNSMInteriorFrameFactory(
			tupleWriterFactory);
	protected ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();

	// btree frames
	protected ITreeIndexFrame leafFrame = leafFrameFactory.createFrame();
	protected ITreeIndexMetaDataFrame metaFrame = metaFrameFactory
			.createFrame();

	protected IFreePageManager freePageManager;

	protected BTree btree;

	// --- INVERTED INDEX ---

	protected FileReference invListsFile = new FileReference(new File(
			invListsFileName));
	protected int invListsFileId;

	protected int invListFields = 1;
	protected ITypeTrait[] invListTypeTraits = new ITypeTrait[invListFields];

	protected int invListKeys = 1;
	protected IBinaryComparator[] invListBinCmps = new IBinaryComparator[invListKeys];
	protected MultiComparator invListCmp = new MultiComparator(
			invListTypeTraits, invListBinCmps);

	protected InvertedIndex invIndex;

	protected Random rnd = new Random();

	protected ByteBuffer frame = stageletCtx.allocateFrame();
	protected FrameTupleAppender appender = new FrameTupleAppender(
			stageletCtx.getFrameSize());
	protected ArrayTupleBuilder tb = new ArrayTupleBuilder(2);
	protected DataOutput dos = tb.getDataOutput();

	protected ISerializerDeserializer[] insertSerde = {
			UTF8StringSerializerDeserializer.INSTANCE,
			IntegerSerializerDeserializer.INSTANCE };
	protected RecordDescriptor insertRecDesc = new RecordDescriptor(insertSerde);
	protected IFrameTupleAccessor accessor = new FrameTupleAccessor(
			stageletCtx.getFrameSize(), insertRecDesc);

	protected FrameTupleReference tuple = new FrameTupleReference();

	protected ArrayList<ArrayList<Integer>> checkInvLists = new ArrayList<ArrayList<Integer>>();

	protected int maxId = 1000000;
	// protected int maxId = 1000;
	protected int[] scanCountArray = new int[maxId];
	protected ArrayList<Integer> expectedResults = new ArrayList<Integer>();

	protected ISerializerDeserializer[] querySerde = { UTF8StringSerializerDeserializer.INSTANCE };
	protected RecordDescriptor queryRecDesc = new RecordDescriptor(querySerde);

	protected FrameTupleAppender queryAppender = new FrameTupleAppender(
			stageletCtx.getFrameSize());
	protected ArrayTupleBuilder queryTb = new ArrayTupleBuilder(
			querySerde.length);
	protected DataOutput queryDos = queryTb.getDataOutput();

	protected IFrameTupleAccessor queryAccessor = new FrameTupleAccessor(
			stageletCtx.getFrameSize(), queryRecDesc);
	protected FrameTupleReference queryTuple = new FrameTupleReference();

	protected ITokenFactory tokenFactory;
	protected IBinaryTokenizer tokenizer;

	protected TOccurrenceSearcher searcher;
	protected IInvertedIndexResultCursor resultCursor;

	/**
	 * Initialize members, generate data, and bulk load the inverted index.
	 * 
	 */
	@Before
	public void start() throws Exception {
		TestStorageManagerComponentHolder.init(PAGE_SIZE, NUM_PAGES,
				MAX_OPEN_FILES);
		bufferCache = TestStorageManagerComponentHolder
				.getBufferCache(stageletCtx);
		fmp = TestStorageManagerComponentHolder.getFileMapProvider(stageletCtx);

		// --- BTREE ---

		bufferCache.createFile(btreeFile);
		btreeFileId = fmp.lookupFileId(btreeFile);
		bufferCache.openFile(btreeFileId);

		// token (key)
		typeTraits[0] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);
		// startPageId
		typeTraits[1] = new TypeTrait(4);
		// endPageId
		typeTraits[2] = new TypeTrait(4);
		// startOff
		typeTraits[3] = new TypeTrait(4);
		// numElements
		typeTraits[4] = new TypeTrait(4);

		btreeBinCmps[0] = UTF8StringBinaryComparatorFactory.INSTANCE
				.createBinaryComparator();

		freePageManager = new LinkedListFreePageManager(bufferCache,
				btreeFileId, 0, metaFrameFactory);

		btree = new BTree(bufferCache, freePageManager, interiorFrameFactory,
				leafFrameFactory, btreeCmp);
		btree.create(btreeFileId, leafFrame, metaFrame);
		btree.open(btreeFileId);

		// --- INVERTED INDEX ---

		bufferCache.createFile(invListsFile);
		invListsFileId = fmp.lookupFileId(invListsFile);
		bufferCache.openFile(invListsFileId);

		invListTypeTraits[0] = new TypeTrait(4);
		invListBinCmps[0] = IntegerBinaryComparatorFactory.INSTANCE
				.createBinaryComparator();

		invIndex = new InvertedIndex(bufferCache, btree, invListCmp);
		invIndex.open(invListsFileId);

		rnd.setSeed(50);

		accessor.reset(frame);
		queryAccessor.reset(frame);
	}

	@After
	public void deinit() throws HyracksDataException {
		AbstractInvIndexTest.tearDown();
		btree.close();
		invIndex.close();
		bufferCache.closeFile(btreeFileId);
		bufferCache.closeFile(invListsFileId);
		bufferCache.close();
	}
}
