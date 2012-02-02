package edu.uci.ics.hyracks.storage.am.common.test;

import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.util.Collection;
import java.util.Iterator;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.util.TupleUtils;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoadContext;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;

@SuppressWarnings("rawtypes")
public abstract class TreeIndexTestUtils {
    private static final Logger LOGGER = Logger.getLogger(TreeIndexTestUtils.class.getName());

    protected abstract CheckTuple createCheckTuple(int numFields, int numKeyFields);

    protected abstract ISearchPredicate createNullSearchPredicate();

    public abstract void checkExpectedResults(ITreeIndexCursor cursor, Collection checkTuples,
            ISerializerDeserializer[] fieldSerdes, int keyFieldCount, Iterator<CheckTuple> checkIter) throws Exception;

    protected abstract CheckTuple createIntCheckTuple(int[] fieldValues, int numKeyFields);

    protected abstract void setIntKeyFields(int[] fieldValues, int numKeyFields, int maxValue, Random rnd);

    protected abstract boolean insertTuple(ITreeIndexTestContext ctx) throws Exception;

    protected abstract Collection createCheckTuplesCollection();

    @SuppressWarnings("unchecked")
    public static void createTupleFromCheckTuple(CheckTuple checkTuple, ArrayTupleBuilder tupleBuilder,
            ArrayTupleReference tuple, ISerializerDeserializer[] fieldSerdes) throws HyracksDataException {
        int fieldCount = tupleBuilder.getFieldEndOffsets().length;
        DataOutput dos = tupleBuilder.getDataOutput();
        tupleBuilder.reset();
        for (int i = 0; i < fieldCount; i++) {
            fieldSerdes[i].serialize(checkTuple.get(i), dos);
            tupleBuilder.addFieldEndOffset();
        }
        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
    }

    @SuppressWarnings("unchecked")
    public CheckTuple createCheckTupleFromTuple(ITupleReference tuple, ISerializerDeserializer[] fieldSerdes,
            int numKeys) throws HyracksDataException {
        CheckTuple checkTuple = createCheckTuple(fieldSerdes.length, numKeys);
        int fieldCount = Math.min(fieldSerdes.length, tuple.getFieldCount());
        for (int i = 0; i < fieldCount; i++) {
            ByteArrayInputStream inStream = new ByteArrayInputStream(tuple.getFieldData(i), tuple.getFieldStart(i),
                    tuple.getFieldLength(i));
            DataInput dataIn = new DataInputStream(inStream);
            Comparable fieldObj = (Comparable) fieldSerdes[i].deserialize(dataIn);
            checkTuple.add(fieldObj);
        }
        return checkTuple;
    }

    @SuppressWarnings("unchecked")
    public void checkScan(ITreeIndexTestContext ctx) throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Testing Scan.");
        }
        ITreeIndexCursor scanCursor = ctx.getIndexAccessor().createSearchCursor();
        ISearchPredicate nullPred = createNullSearchPredicate();
        ctx.getIndexAccessor().search(scanCursor, nullPred);
        Iterator<CheckTuple> checkIter = ctx.getCheckTuples().iterator();
        checkExpectedResults(scanCursor, ctx.getCheckTuples(), ctx.getFieldSerdes(), ctx.getKeyFieldCount(), checkIter);
    }

    public void checkDiskOrderScan(ITreeIndexTestContext ctx) throws Exception {
        try {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Testing Disk-Order Scan.");
            }
            ITreeIndexCursor diskOrderCursor = ctx.getIndexAccessor().createDiskOrderScanCursor();
            ctx.getIndexAccessor().diskOrderScan(diskOrderCursor);
            int actualCount = 0;
            try {
                while (diskOrderCursor.hasNext()) {
                    diskOrderCursor.next();
                    ITupleReference tuple = diskOrderCursor.getTuple();
                    CheckTuple checkTuple = createCheckTupleFromTuple(tuple, ctx.getFieldSerdes(),
                            ctx.getKeyFieldCount());
                    if (!ctx.getCheckTuples().contains(checkTuple)) {
                        fail("Disk-order scan returned unexpected answer: " + checkTuple.toString());
                    }
                    actualCount++;
                }
                if (actualCount < ctx.getCheckTuples().size()) {
                    fail("Disk-order scan returned fewer answers than expected.\nExpected: "
                            + ctx.getCheckTuples().size() + "\nActual  : " + actualCount);
                }
                if (actualCount > ctx.getCheckTuples().size()) {
                    fail("Disk-order scan returned more answers than expected.\nExpected: "
                            + ctx.getCheckTuples().size() + "\nActual  : " + actualCount);
                }
            } finally {
                diskOrderCursor.close();
            }
        } catch (UnsupportedOperationException e) {
            // Ignore exception because some indexes, e.g. the LSMTrees, don't
            // support disk-order scan.
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Ignoring disk-order scan since it's not supported.");
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void insertIntTuples(ITreeIndexTestContext ctx, int numTuples, Random rnd) throws Exception {
        int fieldCount = ctx.getFieldCount();
        int numKeyFields = ctx.getKeyFieldCount();
        int[] fieldValues = new int[ctx.getFieldCount()];
        // Scale range of values according to number of keys.
        // For example, for 2 keys we want the square root of numTuples, for 3
        // keys the cube root of numTuples, etc.
        int maxValue = (int) Math.ceil(Math.pow(numTuples, 1.0 / (double) numKeyFields));
        for (int i = 0; i < numTuples; i++) {
            // Set keys.
            setIntKeyFields(fieldValues, numKeyFields, maxValue, rnd);
            // Set values.
            for (int j = numKeyFields; j < fieldCount; j++) {
                fieldValues[j] = j;
            }
            TupleUtils.createIntegerTuple(ctx.getTupleBuilder(), ctx.getTuple(), fieldValues);
            if (LOGGER.isLoggable(Level.INFO)) {
                if ((i + 1) % (numTuples / Math.min(10, numTuples)) == 0) {
                    LOGGER.info("Inserting Tuple " + (i + 1) + "/" + numTuples);
                }
            }
            boolean succeeded = insertTuple(ctx);
            if (succeeded) {
                ctx.insertCheckTuple(createIntCheckTuple(fieldValues, ctx.getKeyFieldCount()), ctx.getCheckTuples());
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void bulkLoadIntTuples(ITreeIndexTestContext ctx, int numTuples, Random rnd) throws Exception {
        int fieldCount = ctx.getFieldCount();
        int numKeyFields = ctx.getKeyFieldCount();
        int[] fieldValues = new int[ctx.getFieldCount()];
        int maxValue = (int) Math.ceil(Math.pow(numTuples, 1.0 / (double) numKeyFields));
        Collection<CheckTuple> tmpCheckTuples = createCheckTuplesCollection();
        for (int i = 0; i < numTuples; i++) {
            // Set keys.
            setIntKeyFields(fieldValues, numKeyFields, maxValue, rnd);
            // Set values.
            for (int j = numKeyFields; j < fieldCount; j++) {
                fieldValues[j] = j;
            }

            // Set expected values. (We also use these as the pre-sorted stream
            // for ordered indexes bulk loading).
            ctx.insertCheckTuple(createIntCheckTuple(fieldValues, ctx.getKeyFieldCount()), tmpCheckTuples);
        }
        bulkLoadCheckTuples(ctx, tmpCheckTuples);

        // Add tmpCheckTuples to ctx check tuples for comparing searches.
        for (CheckTuple checkTuple : tmpCheckTuples) {
            ctx.insertCheckTuple(checkTuple, ctx.getCheckTuples());
        }
    }

    public static void bulkLoadCheckTuples(ITreeIndexTestContext ctx, Collection<CheckTuple> checkTuples)
            throws HyracksDataException, TreeIndexException {
        int fieldCount = ctx.getFieldCount();
        int numTuples = checkTuples.size();
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();
        // Perform bulk load.
        IIndexBulkLoadContext bulkLoadCtx = ctx.getIndex().beginBulkLoad(0.7f);
        int c = 1;
        for (CheckTuple checkTuple : checkTuples) {
            if (LOGGER.isLoggable(Level.INFO)) {
                if (c % (numTuples / 10) == 0) {
                    LOGGER.info("Bulk Loading Tuple " + c + "/" + numTuples);
                }
            }
            createTupleFromCheckTuple(checkTuple, tupleBuilder, tuple, ctx.getFieldSerdes());
            ctx.getIndex().bulkLoadAddTuple(tuple, bulkLoadCtx);
            c++;
        }
        ctx.getIndex().endBulkLoad(bulkLoadCtx);
    }

    @SuppressWarnings("unchecked")
    public void deleteTuples(ITreeIndexTestContext ctx, int numTuples, Random rnd) throws Exception {
        ArrayTupleBuilder deleteTupleBuilder = new ArrayTupleBuilder(ctx.getKeyFieldCount());
        ArrayTupleReference deleteTuple = new ArrayTupleReference();
        int numCheckTuples = ctx.getCheckTuples().size();
        // Copy CheckTuple references into array, so we can randomly pick from
        // there.
        CheckTuple[] checkTuples = new CheckTuple[numCheckTuples];
        int idx = 0;
        Iterator<CheckTuple> iter = ctx.getCheckTuples().iterator();
        while (iter.hasNext()) {
            CheckTuple checkTuple = iter.next();
            checkTuples[idx++] = checkTuple;
        }

        for (int i = 0; i < numTuples && numCheckTuples > 0; i++) {
            if (LOGGER.isLoggable(Level.INFO)) {
                if ((i + 1) % (numTuples / Math.min(10, numTuples)) == 0) {
                    LOGGER.info("Deleting Tuple " + (i + 1) + "/" + numTuples);
                }
            }
            int checkTupleIdx = Math.abs(rnd.nextInt() % numCheckTuples);
            CheckTuple checkTuple = checkTuples[checkTupleIdx];
            createTupleFromCheckTuple(checkTuple, deleteTupleBuilder, deleteTuple, ctx.getFieldSerdes());
            ctx.getIndexAccessor().delete(deleteTuple);

            // Remove check tuple from expected results.
            ctx.getCheckTuples().remove(checkTuple);

            // Swap with last "valid" CheckTuple.
            CheckTuple tmp = checkTuples[numCheckTuples - 1];
            checkTuples[numCheckTuples - 1] = checkTuple;
            checkTuples[checkTupleIdx] = tmp;
            numCheckTuples--;
        }
    }

}
