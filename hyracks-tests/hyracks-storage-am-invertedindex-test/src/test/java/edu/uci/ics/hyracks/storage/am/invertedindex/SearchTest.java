package edu.uci.ics.hyracks.storage.am.invertedindex;

import java.io.DataOutput;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
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
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.IntegerBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.UTF8StringBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
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
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndexResultCursor;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndexSearchModifier;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedListBuilder;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.FixedSizeElementInvertedListBuilder;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.InvertedIndex;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.OccurrenceThresholdPanicException;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.SearchResultCursor;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.TOccurrenceSearcher;
import edu.uci.ics.hyracks.storage.am.invertedindex.searchmodifiers.ConjunctiveSearchModifier;
import edu.uci.ics.hyracks.storage.am.invertedindex.searchmodifiers.JaccardSearchModifier;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.test.support.TestStorageManagerComponentHolder;
import edu.uci.ics.hyracks.test.support.TestUtils;

public class SearchTest extends AbstractInvIndexTest {
    
    // private static final int PAGE_SIZE = 65536;
    private static final int PAGE_SIZE = 32768;
    private static final int NUM_PAGES = 100;
    private static final int MAX_OPEN_FILES = 10;
    private static final int HYRACKS_FRAME_SIZE = 32768;
    private static IHyracksStageletContext stageletCtx = TestUtils.create(HYRACKS_FRAME_SIZE);    
    
    private static IBufferCache bufferCache;
    private static IFileMapProvider fmp;
    
    // --- BTREE ---
    
    // create file refs
    private static FileReference btreeFile = new FileReference(new File(btreeFileName));
    private static int btreeFileId;            
               
    // declare btree fields
    private static int fieldCount = 5;
    private static ITypeTrait[] typeTraits = new ITypeTrait[fieldCount];
        
    // declare btree keys
    private static int btreeKeyFieldCount = 1;
    private static IBinaryComparator[] btreeBinCmps = new IBinaryComparator[btreeKeyFieldCount];    
    private static MultiComparator btreeCmp = new MultiComparator(typeTraits, btreeBinCmps);
    
    // btree frame factories
    private static TypeAwareTupleWriterFactory tupleWriterFactory = new TypeAwareTupleWriterFactory(typeTraits);        
    private static IBTreeLeafFrameFactory leafFrameFactory = new NSMLeafFrameFactory(tupleWriterFactory);
    private static IBTreeInteriorFrameFactory interiorFrameFactory = new NSMInteriorFrameFactory(tupleWriterFactory);
    private static ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();
    
    // btree frames
    private static IBTreeLeafFrame leafFrame = leafFrameFactory.getFrame();
    private static ITreeIndexMetaDataFrame metaFrame = metaFrameFactory.getFrame();
    
    private static IFreePageManager freePageManager;
    
    private static BTree btree;
        
    
    // --- INVERTED INDEX ---
    
    private static FileReference invListsFile = new FileReference(new File(invListsFileName));    
    private static int invListsFileId;
    
    private static int invListFields = 1;
    private static ITypeTrait[] invListTypeTraits = new ITypeTrait[invListFields];
        
    private static int invListKeys = 1;
    private static IBinaryComparator[] invListBinCmps = new IBinaryComparator[invListKeys];        
    private static MultiComparator invListCmp = new MultiComparator(invListTypeTraits, invListBinCmps);
    
    private static InvertedIndex invIndex;          
    
    private static Random rnd = new Random();
    
    private static ByteBuffer frame = stageletCtx.allocateFrame();
    private static FrameTupleAppender appender = new FrameTupleAppender(stageletCtx.getFrameSize());
    private static ArrayTupleBuilder tb = new ArrayTupleBuilder(2);
    private static DataOutput dos = tb.getDataOutput();

    private static ISerializerDeserializer[] insertSerde = { UTF8StringSerializerDeserializer.INSTANCE,
            IntegerSerializerDeserializer.INSTANCE };
    private static RecordDescriptor insertRecDesc = new RecordDescriptor(insertSerde);
    private static IFrameTupleAccessor accessor = new FrameTupleAccessor(stageletCtx.getFrameSize(), insertRecDesc);
    
    private static FrameTupleReference tuple = new FrameTupleReference();
    

    private static List<String> tokens = new ArrayList<String>();
    private static ArrayList<ArrayList<Integer>> checkInvLists = new ArrayList<ArrayList<Integer>>();
    
    private static int maxId = 1000000;
    //private static int maxId = 1000;
    private static int[] scanCountArray = new int[maxId];
    private static ArrayList<Integer> expectedResults = new ArrayList<Integer>();
    
    private static ISerializerDeserializer[] querySerde = { UTF8StringSerializerDeserializer.INSTANCE };
    private static RecordDescriptor queryRecDesc = new RecordDescriptor(querySerde);

    private static FrameTupleAppender queryAppender = new FrameTupleAppender(stageletCtx.getFrameSize());
    private static ArrayTupleBuilder queryTb = new ArrayTupleBuilder(querySerde.length);
    private static DataOutput queryDos = queryTb.getDataOutput();

    private static IFrameTupleAccessor queryAccessor = new FrameTupleAccessor(stageletCtx.getFrameSize(), queryRecDesc);    
    private static FrameTupleReference queryTuple = new FrameTupleReference();
            
    private static ITokenFactory tokenFactory = new UTF8WordTokenFactory();
    private static IBinaryTokenizer queryTokenizer = new DelimitedUTF8StringBinaryTokenizer(true, false, tokenFactory);
    
    private static TOccurrenceSearcher searcher;
    private static IInvertedIndexResultCursor resultCursor;
    
    @BeforeClass
    public static void start() throws Exception {
    	TestStorageManagerComponentHolder.init(PAGE_SIZE, NUM_PAGES, MAX_OPEN_FILES);
    	bufferCache = TestStorageManagerComponentHolder.getBufferCache(stageletCtx);
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
    	
    	btreeBinCmps[0] = UTF8StringBinaryComparatorFactory.INSTANCE.createBinaryComparator();
    	
    	freePageManager = new LinkedListFreePageManager(bufferCache, btreeFileId, 0, metaFrameFactory);
    	
    	btree = new BTree(bufferCache, freePageManager, interiorFrameFactory, leafFrameFactory, btreeCmp);
    	btree.create(btreeFileId, leafFrame, metaFrame);
    	btree.open(btreeFileId);
    	    	
    	
        // --- INVERTED INDEX ---
    	
    	bufferCache.createFile(invListsFile);
    	invListsFileId = fmp.lookupFileId(invListsFile);
    	bufferCache.openFile(invListsFileId);
    	
    	invListTypeTraits[0] = new TypeTrait(4);
    	invListBinCmps[0] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();
    	
    	invIndex = new InvertedIndex(bufferCache, btree, invListCmp);
    	invIndex.open(invListsFileId);  
    	
    	searcher = new TOccurrenceSearcher(stageletCtx, invIndex, queryTokenizer);
    	resultCursor = new SearchResultCursor(searcher.createResultFrameTupleAccessor(), searcher.createResultTupleReference());
    	
    	rnd.setSeed(50);
    	
    	accessor.reset(frame);
    	queryAccessor.reset(frame);
    	
    	loadData();
    }
    
    private static void loadData() throws HyracksDataException {    	
        tokens.add("compilers");
        tokens.add("computer");
        tokens.add("databases");
        tokens.add("fast");
        tokens.add("hyracks");  
        tokens.add("major");
        tokens.add("science");
        tokens.add("systems");
        tokens.add("university");        
    	
        for(int i = 0; i < tokens.size(); i++) {
        	checkInvLists.add(new ArrayList<Integer>());
        }
        
        // for generating length-skewed inverted lists
        int addProb = 0;
        int addProbStep = 10;        
        
        IInvertedListBuilder invListBuilder = new FixedSizeElementInvertedListBuilder(invListTypeTraits);
        InvertedIndex.BulkLoadContext ctx = invIndex.beginBulkLoad(invListBuilder, HYRACKS_FRAME_SIZE);
        
        int totalElements = 0;        
        for (int i = 0; i < tokens.size(); i++) {

            addProb += addProbStep * (i+1);
            for (int j = 0; j < maxId; j++) {
                if ((Math.abs(rnd.nextInt()) % addProb) == 0) {                   
                	
                	totalElements++;
                	
                	tb.reset();
                    UTF8StringSerializerDeserializer.INSTANCE.serialize(tokens.get(i), dos);
                    tb.addFieldEndOffset();
                    IntegerSerializerDeserializer.INSTANCE.serialize(j, dos);
                    tb.addFieldEndOffset();
                    
                    checkInvLists.get(i).add(j);
                    
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
        }
        invIndex.endBulkLoad(ctx);    	
    }
    
    private void fillExpectedResults(int[] queryTokenIndexes, int numQueryTokens, int occurrenceThreshold) {    	    	
    	// reset scan count array
        for(int i = 0; i < maxId; i++) {
        	scanCountArray[i] = 0;
        }        
        
        // count occurrences        
        for(int i = 0; i < numQueryTokens; i++) {
        	ArrayList<Integer> list = checkInvLists.get(queryTokenIndexes[i]);        	
        	for(int j = 0; j < list.size(); j++) {
        		scanCountArray[list.get(j)]++;
        	}
        }
        
        // check threshold
        expectedResults.clear();
        for(int i = 0; i < maxId; i++) {
        	if(scanCountArray[i] >= occurrenceThreshold) {
        		expectedResults.add(i);
        	}
        }               
    }
    
    private void runQueries(IInvertedIndexSearchModifier searchModifier) throws Exception {    	                    
    	
    	rnd.setSeed(50);
    	
        // generate random queries
        int queries = 50;
        int[] queryTokenIndexes = new int[tokens.size()];
        for(int i = 0; i < queries; i++) {
        	
        	int numQueryTokens = Math.abs(rnd.nextInt() % tokens.size()) + 1;
        	for(int j = 0; j < numQueryTokens; j++) {
        		queryTokenIndexes[j] = Math.abs(rnd.nextInt() % tokens.size());        		        		
        	}
        	
        	StringBuilder strBuilder = new StringBuilder();
        	for(int j = 0; j < numQueryTokens; j++) {
        		strBuilder.append(tokens.get(queryTokenIndexes[j]));
        		if(j+1 != numQueryTokens) strBuilder.append(" ");
        	}
        	
        	String queryString = strBuilder.toString();        	
        	
        	queryTb.reset();
            UTF8StringSerializerDeserializer.INSTANCE.serialize(queryString, queryDos);
            queryTb.addFieldEndOffset();

            queryAppender.reset(frame, true);
            queryAppender.append(queryTb.getFieldEndOffsets(), queryTb.getByteArray(), 0, queryTb.getSize());
            queryTuple.reset(queryAccessor, 0);
        
            boolean panic = false;
            
            int repeats = 1;
            double totalTime = 0;
            for(int j = 0; j < repeats; j++) {
            	long timeStart = System.currentTimeMillis();
            	try {
            		searcher.reset();
            		searcher.search(resultCursor, queryTuple, 0, searchModifier);
            	} catch(OccurrenceThresholdPanicException e) {           
            		panic = true;
            	}
            	long timeEnd = System.currentTimeMillis();
            	totalTime += timeEnd - timeStart;
            }
            double avgTime = totalTime / (double)repeats;
            System.out.println(i + ": " + "\"" + queryString + "\": " + avgTime + "ms");            
                                               
            if(!panic) {

            	fillExpectedResults(queryTokenIndexes, numQueryTokens, searcher.getOccurrenceThreshold());
            	
            	// verify results
            	int checkIndex = 0;
            	while(resultCursor.hasNext()) {
            		resultCursor.next();
            		ITupleReference resultTuple = resultCursor.getTuple();
            		int id = IntegerSerializerDeserializer.getInt(resultTuple.getFieldData(0), resultTuple.getFieldStart(0));            	
            		Assert.assertEquals(expectedResults.get(checkIndex).intValue(), id);
            		checkIndex++;            	
            	}

            	if(expectedResults.size() != checkIndex) {
            		System.out.println("CHECKING");
            		for(Integer x : expectedResults) {
            			System.out.print(x + " ");
            		}
            		System.out.println();
            	}

            	Assert.assertEquals(expectedResults.size(), checkIndex);  
            }
        }               
    }
        
    @Test
    public void conjunctiveKeywordQueryTest() throws Exception {
    	IInvertedIndexSearchModifier searchModifier = new ConjunctiveSearchModifier();
    	runQueries(searchModifier);
    }
    
    @Test
    public void jaccardKeywordQueryTest() throws Exception {
    	JaccardSearchModifier searchModifier = new JaccardSearchModifier(1.0f);
    	
    	System.out.println("JACCARD: " + 1.0f);
    	searchModifier.setJaccThresh(1.0f);
    	runQueries(searchModifier);
    	
    	System.out.println("JACCARD: " + 0.9f);
    	searchModifier.setJaccThresh(0.9f);
    	runQueries(searchModifier);
    	
    	System.out.println("JACCARD: " + 0.8f);
    	searchModifier.setJaccThresh(0.8f);
    	runQueries(searchModifier);
    	
    	System.out.println("JACCARD: " + 0.7f);
    	searchModifier.setJaccThresh(0.7f);
    	runQueries(searchModifier);
    	
    	System.out.println("JACCARD: " + 0.6f);
    	searchModifier.setJaccThresh(0.6f);
    	runQueries(searchModifier);
    	
    	System.out.println("JACCARD: " + 0.5f);
    	searchModifier.setJaccThresh(0.5f);
    	runQueries(searchModifier);
    }    
    
    @AfterClass
    public static void deinit() throws HyracksDataException {
    	AbstractInvIndexTest.tearDown();
    	btree.close();
    	invIndex.close();
    	bufferCache.closeFile(btreeFileId);
        bufferCache.closeFile(invListsFileId);        
    	bufferCache.close();
    }        
}
