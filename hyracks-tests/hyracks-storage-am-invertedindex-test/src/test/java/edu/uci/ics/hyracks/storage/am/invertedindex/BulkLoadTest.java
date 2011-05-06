package edu.uci.ics.hyracks.storage.am.invertedindex;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import edu.uci.ics.fuzzyjoin.tokenizer.DelimitedUTF8StringBinaryTokenizer;
import edu.uci.ics.fuzzyjoin.tokenizer.IBinaryTokenizer;
import edu.uci.ics.fuzzyjoin.tokenizer.ITokenFactory;
import edu.uci.ics.fuzzyjoin.tokenizer.UTF8WordTokenFactory;
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
import edu.uci.ics.hyracks.dataflow.common.data.comparators.UTF8StringBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.frames.NSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.frames.NSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.freepage.LinkedListFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedListBuilder;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.FixedSizeElementInvertedListBuilder;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.InvertedIndex;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.TOccurrenceSearcher;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.test.support.TestStorageManagerComponentHolder;
import edu.uci.ics.hyracks.test.support.TestUtils;

public class BulkLoadTest extends AbstractInvIndexTest {
	// testing params
    //private static final int PAGE_SIZE = 256;
    //private static final int NUM_PAGES = 100;
    //private static final int HYRACKS_FRAME_SIZE = 256;

    // realistic params
    // private static final int PAGE_SIZE = 65536;
    private static final int PAGE_SIZE = 32768;
    private static final int NUM_PAGES = 100;
    private static final int HYRACKS_FRAME_SIZE = 32768;
    private IHyracksStageletContext stageletCtx = TestUtils.create(HYRACKS_FRAME_SIZE);    

    @Test
    public void test01() throws Exception {

        TestStorageManagerComponentHolder.init(PAGE_SIZE, NUM_PAGES);
        IBufferCache bufferCache = TestStorageManagerComponentHolder.getBufferCache(stageletCtx);
        IFileMapProvider fmp = TestStorageManagerComponentHolder.getFileMapProvider(stageletCtx);
        
        // create file refs
        System.out.println(btreeFileName);
        FileReference btreeFile = new FileReference(new File(btreeFileName));
        bufferCache.createFile(btreeFile);
        int btreeFileId = fmp.lookupFileId(btreeFile);
        bufferCache.openFile(btreeFileId);

        System.out.println(invListsFileName);
        FileReference invListsFile = new FileReference(new File(invListsFileName));
        bufferCache.createFile(invListsFile);
        int invListsFileId = fmp.lookupFileId(invListsFile);
        bufferCache.openFile(invListsFileId);
                
        // declare btree fields
        int fieldCount = 5;
        ITypeTrait[] typeTraits = new ITypeTrait[fieldCount];
        typeTraits[0] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);
        typeTraits[1] = new TypeTrait(4);
        typeTraits[2] = new TypeTrait(4);
        typeTraits[3] = new TypeTrait(4);
        typeTraits[4] = new TypeTrait(4);

        // declare btree keys
        int keyFieldCount = 1;
        IBinaryComparator[] cmps = new IBinaryComparator[keyFieldCount];
        cmps[0] = UTF8StringBinaryComparatorFactory.INSTANCE.createBinaryComparator();

        MultiComparator cmp = new MultiComparator(typeTraits, cmps);

        TypeAwareTupleWriterFactory tupleWriterFactory = new TypeAwareTupleWriterFactory(typeTraits);        
        IBTreeLeafFrameFactory leafFrameFactory = new NSMLeafFrameFactory(tupleWriterFactory);
        // IBTreeLeafFrameFactory leafFrameFactory = new
        // FieldPrefixNSMLeafFrameFactory(tupleWriterFactory);
        IBTreeInteriorFrameFactory interiorFrameFactory = new NSMInteriorFrameFactory(tupleWriterFactory);
        ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();

        IBTreeLeafFrame leafFrame = leafFrameFactory.getFrame();
        IBTreeInteriorFrame interiorFrame = interiorFrameFactory.getFrame();
        ITreeIndexMetaDataFrame metaFrame = metaFrameFactory.getFrame();

        IFreePageManager freePageManager = new LinkedListFreePageManager(bufferCache, btreeFileId, 0, metaFrameFactory);
        
        BTree btree = new BTree(bufferCache, freePageManager, interiorFrameFactory, leafFrameFactory, cmp);            
        btree.create(btreeFileId, leafFrame, metaFrame);
        btree.open(btreeFileId);
        
        int invListFields = 1;
        ITypeTrait[] invListTypeTraits = new ITypeTrait[invListFields];
        invListTypeTraits[0] = new TypeTrait(4);        
        
        int invListKeys = 1;
        IBinaryComparator[] invListBinCmps = new IBinaryComparator[invListKeys];
        invListBinCmps[0] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        
        MultiComparator invListCmp = new MultiComparator(invListTypeTraits, invListBinCmps);
        
        InvertedIndex invIndex = new InvertedIndex(bufferCache, btree, invListCmp);
        invIndex.open(invListsFileId);        
        
        Random rnd = new Random();
        rnd.setSeed(50);

        ByteBuffer frame = stageletCtx.allocateFrame();
        FrameTupleAppender appender = new FrameTupleAppender(stageletCtx.getFrameSize());
        ArrayTupleBuilder tb = new ArrayTupleBuilder(2);
        DataOutput dos = tb.getDataOutput();

        ISerializerDeserializer[] insertSerde = { UTF8StringSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE };
        RecordDescriptor insertRecDesc = new RecordDescriptor(insertSerde);
        IFrameTupleAccessor accessor = new FrameTupleAccessor(stageletCtx.getFrameSize(), insertRecDesc);
        accessor.reset(frame);
        FrameTupleReference tuple = new FrameTupleReference();

        List<String> tokens = new ArrayList<String>();
        tokens.add("compilers");
        tokens.add("computer");
        tokens.add("databases");
        tokens.add("fast");
        tokens.add("hyracks");  
        tokens.add("major");
        tokens.add("science");
        tokens.add("systems");
        tokens.add("university");      
        
        int maxId = 1000000;
        int addProb = 0;
        int addProbStep = 10;        

        IInvertedListBuilder invListBuilder = new FixedSizeElementInvertedListBuilder(invListTypeTraits);
        InvertedIndex.BulkLoadContext ctx = invIndex.beginBulkLoad(invListBuilder, HYRACKS_FRAME_SIZE);
        
        int totalElements = 0;
        int tokenField = 0;
        int[] elementFields = { 1 };
        for (int i = 0; i < tokens.size(); i++) {

            addProb += addProbStep * (i+1);
            StringBuilder strBuilder = new StringBuilder();
            for (int j = 0; j < maxId; j++) {
                if ((Math.abs(rnd.nextInt()) % addProb) == 0) {                   
                	
                	totalElements++;
                	
                	tb.reset();
                    UTF8StringSerializerDeserializer.INSTANCE.serialize(tokens.get(i), dos);
                    tb.addFieldEndOffset();
                    IntegerSerializerDeserializer.INSTANCE.serialize(j, dos);
                    tb.addFieldEndOffset();

                    //strBuilder.append(j + " ");
                    
                    appender.reset(frame, true);
                    appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
                    
                    tuple.reset(accessor, 0);                                      
                    
                    try {
                        invIndex.bulkLoadAddTuple(ctx, tuple);                    
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            System.out.println(tokens.get(i));
            System.out.println(strBuilder.toString());
        }
        invIndex.endBulkLoad(ctx);
        
        int numPages = btree.getFreePageManager().getMaxPage(metaFrame);
        System.out.println("NUMPAGES: " + numPages);
        System.out.println("TOTAL ELEMENTS: " + totalElements);
        
        // --------------------------- TEST A
        // build query as tuple reference
        ISerializerDeserializer[] querySerde = { UTF8StringSerializerDeserializer.INSTANCE };
        RecordDescriptor queryRecDesc = new RecordDescriptor(querySerde);

        FrameTupleAppender queryAppender = new FrameTupleAppender(stageletCtx.getFrameSize());
        ArrayTupleBuilder queryTb = new ArrayTupleBuilder(querySerde.length);
        DataOutput queryDos = queryTb.getDataOutput();

        IFrameTupleAccessor queryAccessor = new FrameTupleAccessor(stageletCtx.getFrameSize(), queryRecDesc);
        queryAccessor.reset(frame);
        FrameTupleReference queryTuple = new FrameTupleReference();

        //String query = "computer hyracks fast";
        //String query = "compilers fast university hyracks";
        String query = "compilers fast";
        
        ITokenFactory tokenFactory = new UTF8WordTokenFactory();
        IBinaryTokenizer queryTokenizer = new DelimitedUTF8StringBinaryTokenizer(true, false, tokenFactory);

        queryTb.reset();
        UTF8StringSerializerDeserializer.INSTANCE.serialize(query, queryDos);
        queryTb.addFieldEndOffset();

        queryAppender.reset(frame, true);
        queryAppender.append(queryTb.getFieldEndOffsets(), queryTb.getByteArray(), 0, queryTb.getSize());
        queryTuple.reset(queryAccessor, 0);
        
        UTF8StringSerializerDeserializer serde = UTF8StringSerializerDeserializer.INSTANCE;        
        ByteArrayInputStream inStream = new ByteArrayInputStream(queryTuple.getFieldData(0), queryTuple.getFieldStart(0), queryTuple.getFieldLength(0));
        DataInput dataIn = new DataInputStream(inStream);
        Object o = serde.deserialize(dataIn);
        System.out.println(o.toString());
        
        TOccurrenceSearcher searcher = new TOccurrenceSearcher(stageletCtx, invIndex, queryTokenizer);
        //TOccurrenceSearcherSuffixProbeOnly searcher = new TOccurrenceSearcherSuffixProbeSingle(stageletCtx, invIndex, queryTokenizer);
        //TOccurrenceSearcherSuffixScanOnly searcher = new TOccurrenceSearcherSuffixScan(stageletCtx, invIndex, queryTokenizer);

        int repeats = 100;
        double totalTime = 0;
        for(int i = 0; i < repeats; i++) {
        	long timeStart = System.currentTimeMillis();
        	searcher.reset();
        	searcher.search(queryTuple, 0);
        	long timeEnd = System.currentTimeMillis();
        	//System.out.println("SEARCH TIME: " + (timeEnd - timeStart) + "ms");
        	totalTime += timeEnd - timeStart;
        }
        double avgTime = totalTime / (double)repeats;
        System.out.println("AVG TIME: " + avgTime + "ms");                        
        
        /*
        // ------------------------- TEST B
        IInvertedListCursor invListCursor = new FixedSizeElementInvertedListCursor(bufferCache, invListsFileId, invListTypeTraits);        
        
        ISerializerDeserializer[] btreeSerde = { UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };              
        ISerializerDeserializer[] invListSerdes = { IntegerSerializerDeserializer.INSTANCE };
        
        System.out.println("ORDERED SCAN:");
		IBTreeCursor scanCursor = new RangeSearchCursor(leafFrame);
		RangePredicate nullPred = new RangePredicate(true, null, null, true,
				true, null, null);
		BTreeOpContext searchOpCtx = btree.createOpContext(TreeIndexOp.TI_SEARCH,
				leafFrame, interiorFrame, null);
		btree.search(scanCursor, nullPred, searchOpCtx);
		try {
			while (scanCursor.hasNext()) {
				scanCursor.next();
				ITupleReference frameTuple = scanCursor.getTuple();
				String rec = cmp.printTuple(frameTuple, btreeSerde);
				System.out.println(rec);
				
				int startPageId = IntegerSerializerDeserializer.getInt(frameTuple.getFieldData(1), frameTuple.getFieldStart(1));
				int endPageId = IntegerSerializerDeserializer.getInt(frameTuple.getFieldData(2), frameTuple.getFieldStart(2));
				int startOff = IntegerSerializerDeserializer.getInt(frameTuple.getFieldData(3), frameTuple.getFieldStart(3));
				int numElements = IntegerSerializerDeserializer.getInt(frameTuple.getFieldData(4), frameTuple.getFieldStart(4));
								
				invListCursor.reset(startPageId, endPageId, startOff, numElements);								
				invListCursor.pinPagesSync();
				try {				
					String invList = invListCursor.printInvList(invListSerdes);
					System.out.println(invList);
					
					for(int i = 0; i < numElements; i++) {
						invListCursor.positionCursor(i);
						String curr = invListCursor.printCurrentElement(invListSerdes);
						System.out.print(curr + " ");
					}
					System.out.println();
					
					ByteBuffer buf = ByteBuffer.allocate(4);
					IBinaryComparator intCmp = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();
					for(int i = 0; i < maxId; i++) {
						buf.putInt(0, i);
						invListCursor.reset(startPageId, endPageId, startOff, numElements);		
						if(invListCursor.containsKey(buf.array(), 0, 4, intCmp)) {
							System.out.print(i + " ");
						}
					}
					System.out.println();
					
				} finally {
					invListCursor.unpinPages();	
				}																
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			scanCursor.close();
		}
		*/
		
        btree.close();
        bufferCache.closeFile(btreeFileId);
        bufferCache.closeFile(invListsFileId);
        bufferCache.close();
    }
}
