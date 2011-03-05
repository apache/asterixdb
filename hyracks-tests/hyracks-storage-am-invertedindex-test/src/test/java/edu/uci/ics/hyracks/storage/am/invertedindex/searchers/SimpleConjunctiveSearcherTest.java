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

package edu.uci.ics.hyracks.storage.am.invertedindex.searchers;

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
import edu.uci.ics.hyracks.storage.am.btree.impls.TreeIndexOp;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeOpContext;
import edu.uci.ics.hyracks.storage.am.btree.impls.MultiComparator;
import edu.uci.ics.hyracks.storage.am.btree.tuples.TypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.freepage.LinkedListFreePageManager;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IBinaryTokenizer;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndexResultCursor;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.SimpleConjunctiveSearcher;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.DelimitedUTF8StringBinaryTokenizer;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICacheMemoryAllocator;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.test.support.TestStorageManagerComponentHolder;
import edu.uci.ics.hyracks.test.support.TestUtils;

public class SimpleConjunctiveSearcherTest extends AbstractInvIndexTest {

    // testing params
    // private static final int PAGE_SIZE = 256;
    // private static final int NUM_PAGES = 10;
    // private static final int HYRACKS_FRAME_SIZE = 256;

    // realistic params
    // private static final int PAGE_SIZE = 65536;
    private static final int PAGE_SIZE = 32768;
    private static final int NUM_PAGES = 10;
    private static final int HYRACKS_FRAME_SIZE = 32768;
    private IHyracksStageletContext ctx = TestUtils.create(HYRACKS_FRAME_SIZE);

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

    @Test
    public void test01() throws Exception {

        TestStorageManagerComponentHolder.init(PAGE_SIZE, NUM_PAGES);
        IBufferCache bufferCache = TestStorageManagerComponentHolder.getBufferCache(ctx);
        IFileMapProvider fmp = TestStorageManagerComponentHolder.getFileMapProvider(ctx);
        FileReference file = new FileReference(new File(fileName));
        bufferCache.createFile(file);
        int fileId = fmp.lookupFileId(file);
        bufferCache.openFile(fileId);

        // declare fields
        int fieldCount = 2;
        ITypeTrait[] typeTraits = new ITypeTrait[fieldCount];
        typeTraits[0] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);
        typeTraits[1] = new TypeTrait(4);

        // declare keys
        int keyFieldCount = 2;
        IBinaryComparator[] cmps = new IBinaryComparator[keyFieldCount];
        cmps[0] = UTF8StringBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        cmps[1] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();

        MultiComparator cmp = new MultiComparator(typeTraits, cmps);

        TypeAwareTupleWriterFactory tupleWriterFactory = new TypeAwareTupleWriterFactory(typeTraits);
        // SimpleTupleWriterFactory tupleWriterFactory = new
        // SimpleTupleWriterFactory();
        IBTreeLeafFrameFactory leafFrameFactory = new NSMLeafFrameFactory(tupleWriterFactory);
        // IBTreeLeafFrameFactory leafFrameFactory = new
        // FieldPrefixNSMLeafFrameFactory(tupleWriterFactory);
        IBTreeInteriorFrameFactory interiorFrameFactory = new NSMInteriorFrameFactory(tupleWriterFactory);
        ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();

        IBTreeLeafFrame leafFrame = leafFrameFactory.getFrame();
        IBTreeInteriorFrame interiorFrame = interiorFrameFactory.getFrame();
        ITreeIndexMetaDataFrame metaFrame = metaFrameFactory.getFrame();

        IFreePageManager freePageManager = new LinkedListFreePageManager(bufferCache, fileId, 0);
        
        BTree btree = new BTree(bufferCache, freePageManager, interiorFrameFactory, leafFrameFactory, cmp);
        btree.create(fileId, leafFrame, metaFrame);
        btree.open(fileId);

        Random rnd = new Random();
        rnd.setSeed(50);

        ByteBuffer frame = ctx.allocateFrame();
        FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
        ArrayTupleBuilder tb = new ArrayTupleBuilder(cmp.getFieldCount());
        DataOutput dos = tb.getDataOutput();

        ISerializerDeserializer[] btreeSerde = { UTF8StringSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE };
        RecordDescriptor btreeRecDesc = new RecordDescriptor(btreeSerde);
        IFrameTupleAccessor accessor = new FrameTupleAccessor(ctx.getFrameSize(), btreeRecDesc);
        accessor.reset(frame);
        FrameTupleReference tuple = new FrameTupleReference();

        List<String> tokens = new ArrayList<String>();
        tokens.add("computer");
        tokens.add("hyracks");
        tokens.add("fast");
        tokens.add("university");
        tokens.add("science");
        tokens.add("major");

        int maxId = 10000;
        int addProb = 0;
        int addProbStep = 2;

        BTreeOpContext opCtx = btree.createOpContext(TreeIndexOp.TI_INSERT, leafFrame, interiorFrame, metaFrame);

        for (int i = 0; i < tokens.size(); i++) {

            addProb += addProbStep;
            for (int j = 0; j < maxId; j++) {
                if ((Math.abs(rnd.nextInt()) % addProb) == 0) {
                    tb.reset();
                    UTF8StringSerializerDeserializer.INSTANCE.serialize(tokens.get(i), dos);
                    tb.addFieldEndOffset();
                    IntegerSerializerDeserializer.INSTANCE.serialize(j, dos);
                    tb.addFieldEndOffset();

                    appender.reset(frame, true);
                    appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());

                    tuple.reset(accessor, 0);

                    try {
                        btree.insert(tuple, opCtx);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        int numPages = btree.getFreePageManager().getMaxPage(metaFrame);
        System.out.println("NUMPAGES: " + numPages);

        // build query as tuple reference
        ISerializerDeserializer[] querySerde = { UTF8StringSerializerDeserializer.INSTANCE };
        RecordDescriptor queryRecDesc = new RecordDescriptor(querySerde);

        FrameTupleAppender queryAppender = new FrameTupleAppender(ctx.getFrameSize());
        ArrayTupleBuilder queryTb = new ArrayTupleBuilder(querySerde.length);
        DataOutput queryDos = queryTb.getDataOutput();

        IFrameTupleAccessor queryAccessor = new FrameTupleAccessor(ctx.getFrameSize(), queryRecDesc);
        queryAccessor.reset(frame);
        FrameTupleReference queryTuple = new FrameTupleReference();

        String query = "computer hyracks fast";
        char queryDelimiter = ' ';
        IBinaryTokenizer queryTokenizer = new DelimitedUTF8StringBinaryTokenizer(queryDelimiter);

        queryTb.reset();
        UTF8StringSerializerDeserializer.INSTANCE.serialize(query, queryDos);
        queryTb.addFieldEndOffset();

        queryAppender.reset(frame, true);
        queryAppender.append(queryTb.getFieldEndOffsets(), queryTb.getByteArray(), 0, queryTb.getSize());
        queryTuple.reset(queryAccessor, 0);

        int numKeyFields = 1;
        int numValueFields = 1;
        ISerializerDeserializer[] resultSerde = new ISerializerDeserializer[numValueFields];
        for (int i = 0; i < numValueFields; i++) {
            resultSerde[i] = btreeSerde[numKeyFields + i];
        }
        RecordDescriptor resultRecDesc = new RecordDescriptor(resultSerde);
        FrameTupleAccessor resultAccessor = new FrameTupleAccessor(ctx.getFrameSize(), resultRecDesc);
        FrameTupleReference resultTuple = new FrameTupleReference();
        
        SimpleConjunctiveSearcher searcher = new SimpleConjunctiveSearcher(ctx, btree, btreeRecDesc, queryTokenizer,
                numKeyFields, numValueFields);

        long timeStart = System.currentTimeMillis();
        searcher.search(queryTuple, 0);
        long timeEnd = System.currentTimeMillis();
        System.out.println("SEARCH TIME: " + (timeEnd - timeStart) + "ms");

        // System.out.println("INTERSECTION RESULTS");
        IInvertedIndexResultCursor resultCursor = searcher.getResultCursor();
        while (resultCursor.hasNext()) {
            resultCursor.next();
            resultAccessor.reset(resultCursor.getBuffer());
            for (int i = 0; i < resultAccessor.getTupleCount(); i++) {
                resultTuple.reset(resultAccessor, i);
                for (int j = 0; j < resultTuple.getFieldCount(); j++) {
                    ByteArrayInputStream inStream = new ByteArrayInputStream(resultTuple.getFieldData(j), resultTuple
                            .getFieldStart(j), resultTuple.getFieldLength(j));
                    DataInput dataIn = new DataInputStream(inStream);
                    Object o = resultSerde[j].deserialize(dataIn);
                    System.out.print(o + " ");
                }
                System.out.println();
            }
        }

        /*
         * IBinaryComparator[] searchCmps = new IBinaryComparator[1];
         * searchCmps[0] =
         * UTF8StringBinaryComparatorFactory.INSTANCE.createBinaryComparator();
         * MultiComparator searchCmp = new MultiComparator(typeTraits,
         * searchCmps);
         * 
         * // ordered scan IBTreeCursor scanCursor = new
         * RangeSearchCursor(leafFrame); RangePredicate nullPred = new
         * RangePredicate(true, null, null, true, true, null); BTreeOpContext
         * searchOpCtx = btree.createOpContext(BTreeOp.BTO_SEARCH, leafFrame,
         * interiorFrame, metaFrame); btree.search(scanCursor, nullPred,
         * searchOpCtx);
         * 
         * try { while (scanCursor.hasNext()) { scanCursor.next();
         * ITupleReference frameTuple = scanCursor.getTuple(); String rec =
         * cmp.printTuple(frameTuple, btreeSerde); System.out.println(rec); } }
         * catch (Exception e) { e.printStackTrace(); } finally {
         * scanCursor.close(); }
         */

        btree.close();
        bufferCache.closeFile(fileId);
        bufferCache.close();
    }
}
