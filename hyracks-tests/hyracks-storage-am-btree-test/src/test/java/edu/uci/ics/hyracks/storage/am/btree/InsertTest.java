package edu.uci.ics.hyracks.storage.am.btree;

import java.util.Random;

import org.junit.Test;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.IntegerBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeOpContext;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.impls.TreeDiskOrderScanCursor;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;

public class InsertTest extends AbstractBTreeTest {
    
    @Test
    public void test() throws Exception {
        LOGGER.info("FIXED-LENGTH KEY TEST");

        // declare fields
        ISerializerDeserializer[] fieldSerdes = { IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
        int fieldCount = fieldSerdes.length;
        ITypeTrait[] typeTraits = BTreeTestUtils.serdesToTypeTraits(fieldSerdes);

        // declare keys
        int keyFieldCount = 1;
        IBinaryComparator[] cmps = new IBinaryComparator[keyFieldCount];
        cmps[0] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();

        BTree btree = BTreeTestUtils.createBTree(bufferCache, btreeFileId, typeTraits, cmps);
        
        IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) btree.getLeafFrameFactory().createFrame();
        IBTreeInteriorFrame interiorFrame = (IBTreeInteriorFrame) btree.getInteriorFrameFactory().createFrame();
        ITreeIndexMetaDataFrame metaFrame = btree.getFreePageManager().getMetaDataFrameFactory().createFrame();
        
        btree.create(btreeFileId, leafFrame, metaFrame);
        btree.open(btreeFileId);
        
        Random rnd = new Random();
        rnd.setSeed(50);

        long start = System.currentTimeMillis();

        LOGGER.info("INSERTING INTO TREE");

        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();

        BTreeOpContext insertOpCtx = btree.createOpContext(IndexOp.INSERT, leafFrame, interiorFrame, metaFrame);

        // 10000
        for (int i = 0; i < 10000; i++) {
            int f0 = rnd.nextInt() % 10000;
            int f1 = 5;

            BTreeTestUtils.createIntTuple(tupleBuilder, tuple, f0, f1);
            
            if (i % 1000 == 0) {
                long end = System.currentTimeMillis();
                LOGGER.info("INSERTING " + i + " : " + f0 + " " + f1 + " " + (end - start));
            }

            try {
                btree.insert(tuple, insertOpCtx);
            } catch (TreeIndexException e) {
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        // btree.printTree(leafFrame, interiorFrame);

        int maxPage = btree.getFreePageManager().getMaxPage(metaFrame);
        LOGGER.info("MAXPAGE: " + maxPage);
        LOGGER.info(btree.printStats());

        long end = System.currentTimeMillis();
        long duration = end - start;
        LOGGER.info("DURATION: " + duration);

        // ordered scan

        LOGGER.info("ORDERED SCAN:");
        ITreeIndexCursor scanCursor = new BTreeRangeSearchCursor(leafFrame);
        RangePredicate nullPred = new RangePredicate(true, null, null, true, true, null, null);
        BTreeOpContext searchOpCtx = btree.createOpContext(IndexOp.SEARCH, leafFrame, interiorFrame, null);
        btree.search(scanCursor, nullPred, searchOpCtx);
        try {
            while (scanCursor.hasNext()) {
                scanCursor.next();
                ITupleReference frameTuple = scanCursor.getTuple();
                String rec = btree.getMultiComparator().printTuple(frameTuple, fieldSerdes);
                LOGGER.info(rec);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            scanCursor.close();
        }

        // disk-order scan
        LOGGER.info("DISK-ORDER SCAN:");
        TreeDiskOrderScanCursor diskOrderCursor = new TreeDiskOrderScanCursor(leafFrame);
        BTreeOpContext diskOrderScanOpCtx = btree.createOpContext(IndexOp.DISKORDERSCAN, leafFrame, null, null);
        btree.diskOrderScan(diskOrderCursor, leafFrame, metaFrame, diskOrderScanOpCtx);
        try {
            while (diskOrderCursor.hasNext()) {
                diskOrderCursor.next();
                ITupleReference frameTuple = diskOrderCursor.getTuple();
                String rec = btree.getMultiComparator().printTuple(frameTuple, fieldSerdes);
                LOGGER.info(rec);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            diskOrderCursor.close();
        }

        /*
        // range search in [-1000, 1000]
        LOGGER.info("RANGE SEARCH:");

        ITreeIndexCursor rangeCursor = new BTreeRangeSearchCursor(leafFrame);

        // build low and high keys
        ArrayTupleBuilder ktb = new ArrayTupleBuilder(cmp.getKeyFieldCount());
        DataOutput kdos = ktb.getDataOutput();

        ISerializerDeserializer[] keyDescSers = { IntegerSerializerDeserializer.INSTANCE };
        RecordDescriptor keyDesc = new RecordDescriptor(keyDescSers);
        IFrameTupleAccessor keyAccessor = new FrameTupleAccessor(ctx.getFrameSize(), keyDesc);
        keyAccessor.reset(frame);

        appender.reset(frame, true);

        // build and append low key
        ktb.reset();
        IntegerSerializerDeserializer.INSTANCE.serialize(-1000, kdos);
        ktb.addFieldEndOffset();
        appender.append(ktb.getFieldEndOffsets(), ktb.getByteArray(), 0, ktb.getSize());

        // build and append high key
        ktb.reset();
        IntegerSerializerDeserializer.INSTANCE.serialize(1000, kdos);
        ktb.addFieldEndOffset();
        appender.append(ktb.getFieldEndOffsets(), ktb.getByteArray(), 0, ktb.getSize());

        // create tuplereferences for search keys
        FrameTupleReference lowKey = new FrameTupleReference();
        lowKey.reset(keyAccessor, 0);

        FrameTupleReference highKey = new FrameTupleReference();
        highKey.reset(keyAccessor, 1);

        IBinaryComparator[] searchCmps = new IBinaryComparator[1];
        searchCmps[0] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        MultiComparator searchCmp = new MultiComparator(typeTraits, searchCmps);

        RangePredicate rangePred = new RangePredicate(true, lowKey, highKey, true, true, searchCmp, searchCmp);
        btree.search(rangeCursor, rangePred, searchOpCtx);

        try {
            while (rangeCursor.hasNext()) {
                rangeCursor.next();
                ITupleReference frameTuple = rangeCursor.getTuple();
                String rec = cmp.printTuple(frameTuple, recDescSers);
                LOGGER.info(rec);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            rangeCursor.close();
        }

        btree.close();
        bufferCache.closeFile(fileId);
        bufferCache.close();
        */
        
        btree.close();
    }
    
    @Test
    public void test2() {
        System.out.println("Test 2");
    }       
}
