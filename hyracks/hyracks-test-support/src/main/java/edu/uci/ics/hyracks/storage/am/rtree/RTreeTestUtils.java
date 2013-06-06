/*
 * Copyright 2009-2013 by The Regents of the University of California
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

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.util.TupleUtils;
import edu.uci.ics.hyracks.storage.am.common.CheckTuple;
import edu.uci.ics.hyracks.storage.am.common.IIndexTestContext;
import edu.uci.ics.hyracks.storage.am.common.TreeIndexTestUtils;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.common.util.HashMultiSet;
import edu.uci.ics.hyracks.storage.am.rtree.impls.SearchPredicate;
import edu.uci.ics.hyracks.storage.am.rtree.util.RTreeUtils;

@SuppressWarnings("rawtypes")
public class RTreeTestUtils extends TreeIndexTestUtils {
    private static final Logger LOGGER = Logger.getLogger(RTreeTestUtils.class.getName());
    private int intPayloadValue = 0;
    private double doublePayloadValue = 0.0;

    @SuppressWarnings("unchecked")
    // Create a new ArrayList containing the elements satisfying the search key
    public HashMultiSet<RTreeCheckTuple> getRangeSearchExpectedResults(Collection<RTreeCheckTuple> checkTuples,
            RTreeCheckTuple key) {
        HashMultiSet<RTreeCheckTuple> expectedResult = new HashMultiSet<RTreeCheckTuple>();
        Iterator<RTreeCheckTuple> iter = checkTuples.iterator();
        while (iter.hasNext()) {
            RTreeCheckTuple t = iter.next();
            if (t.intersect(key)) {
                expectedResult.add(t);
            }
        }
        return expectedResult;
    }

    public void checkRangeSearch(IIndexTestContext ictx, ITupleReference key) throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Testing Range Search.");
        }
        AbstractRTreeTestContext ctx = (AbstractRTreeTestContext) ictx;
        MultiComparator cmp = RTreeUtils.getSearchMultiComparator(ctx.getComparatorFactories(), key);

        ITreeIndexCursor searchCursor = (ITreeIndexCursor) ctx.getIndexAccessor().createSearchCursor();
        SearchPredicate searchPred = new SearchPredicate(key, cmp);
        ctx.getIndexAccessor().search(searchCursor, searchPred);

        // Get the subset of elements from the expected set within given key
        // range.
        RTreeCheckTuple keyCheck = (RTreeCheckTuple) createCheckTupleFromTuple(key, ctx.getFieldSerdes(),
                cmp.getKeyFieldCount());

        HashMultiSet<RTreeCheckTuple> expectedResult = null;

        expectedResult = getRangeSearchExpectedResults(ctx.getCheckTuples(), keyCheck);
        checkExpectedResults(searchCursor, expectedResult, ctx.getFieldSerdes(), ctx.getKeyFieldCount(), null);
    }

    @SuppressWarnings("unchecked")
    public void insertDoubleTuples(IIndexTestContext ctx, int numTuples, Random rnd) throws Exception {
        int fieldCount = ctx.getFieldCount();
        int numKeyFields = ctx.getKeyFieldCount();
        double[] fieldValues = new double[ctx.getFieldCount()];
        // Scale range of values according to number of keys.
        // For example, for 2 keys we want the square root of numTuples, for 3
        // keys the cube root of numTuples, etc.
        double maxValue = Math.ceil(Math.pow(numTuples, 1.0 / (double) numKeyFields));
        for (int i = 0; i < numTuples; i++) {
            // Set keys.
            setDoubleKeyFields(fieldValues, numKeyFields, maxValue, rnd);
            // Set values.
            setDoublePayloadFields(fieldValues, numKeyFields, fieldCount);
            TupleUtils.createDoubleTuple(ctx.getTupleBuilder(), ctx.getTuple(), fieldValues);
            if (LOGGER.isLoggable(Level.INFO)) {
                if ((i + 1) % (numTuples / Math.min(10, numTuples)) == 0) {
                    LOGGER.info("Inserting Tuple " + (i + 1) + "/" + numTuples);
                }
            }
            try {
                ctx.getIndexAccessor().insert(ctx.getTuple());
                ctx.insertCheckTuple(createDoubleCheckTuple(fieldValues, ctx.getKeyFieldCount()), ctx.getCheckTuples());
            } catch (TreeIndexException e) {
                // We set expected values only after insertion succeeds because
                // we
                // ignore duplicate keys.
            }
        }
    }

    private void setDoubleKeyFields(double[] fieldValues, int numKeyFields, double maxValue, Random rnd) {
        int maxFieldPos = numKeyFields / 2;
        for (int j = 0; j < maxFieldPos; j++) {
            int k = maxFieldPos + j;
            double firstValue = rnd.nextDouble() % maxValue;
            double secondValue;
            do {
                secondValue = rnd.nextDouble() % maxValue;
            } while (secondValue < firstValue);
            fieldValues[j] = firstValue;
            fieldValues[k] = secondValue;
        }
    }

    private void setDoublePayloadFields(double[] fieldValues, int numKeyFields, int numFields) {
        for (int j = numKeyFields; j < numFields; j++) {
            fieldValues[j] = doublePayloadValue++;
        }
    }

    @SuppressWarnings("unchecked")
    protected CheckTuple createDoubleCheckTuple(double[] fieldValues, int numKeyFields) {
        RTreeCheckTuple<Double> checkTuple = new RTreeCheckTuple<Double>(fieldValues.length, numKeyFields);
        for (double v : fieldValues) {
            checkTuple.appendField(v);
        }
        return checkTuple;
    }

    @SuppressWarnings("unchecked")
    public void bulkLoadDoubleTuples(IIndexTestContext ctx, int numTuples, Random rnd) throws Exception {
        int fieldCount = ctx.getFieldCount();
        int numKeyFields = ctx.getKeyFieldCount();
        double[] fieldValues = new double[ctx.getFieldCount()];
        double maxValue = Math.ceil(Math.pow(numTuples, 1.0 / (double) numKeyFields));
        Collection<CheckTuple> tmpCheckTuples = createCheckTuplesCollection();
        for (int i = 0; i < numTuples; i++) {
            // Set keys.
            setDoubleKeyFields(fieldValues, numKeyFields, maxValue, rnd);
            // Set values.
            setDoublePayloadFields(fieldValues, numKeyFields, fieldCount);

            // Set expected values.
            ctx.insertCheckTuple(createDoubleCheckTuple(fieldValues, ctx.getKeyFieldCount()), tmpCheckTuples);
        }
        bulkLoadCheckTuples(ctx, tmpCheckTuples);

        // Add tmpCheckTuples to ctx check tuples for comparing searches.
        for (CheckTuple checkTuple : tmpCheckTuples) {
            ctx.insertCheckTuple(checkTuple, ctx.getCheckTuples());
        }
    }

    @Override
    public void checkExpectedResults(ITreeIndexCursor cursor, Collection checkTuples,
            ISerializerDeserializer[] fieldSerdes, int keyFieldCount, Iterator<CheckTuple> checkIter) throws Exception {
        int actualCount = 0;
        try {
            while (cursor.hasNext()) {
                cursor.next();
                ITupleReference tuple = cursor.getTuple();
                RTreeCheckTuple checkTuple = (RTreeCheckTuple) createCheckTupleFromTuple(tuple, fieldSerdes,
                        keyFieldCount);
                if (!checkTuples.contains(checkTuple)) {
                    fail("Scan or range search returned unexpected answer: " + checkTuple.toString());
                }
                actualCount++;
            }
            if (actualCount < checkTuples.size()) {
                fail("Scan or range search returned fewer answers than expected.\nExpected: " + checkTuples.size()
                        + "\nActual  : " + actualCount);
            }
            if (actualCount > checkTuples.size()) {
                fail("Scan or range search returned more answers than expected.\nExpected: " + checkTuples.size()
                        + "\nActual  : " + actualCount);
            }
        } finally {
            cursor.close();
        }
    }

    @Override
    protected CheckTuple createCheckTuple(int numFields, int numKeyFields) {
        return new RTreeCheckTuple(numFields, numKeyFields);
    }

    @Override
    protected ISearchPredicate createNullSearchPredicate() {
        return new SearchPredicate(null, null);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected CheckTuple createIntCheckTuple(int[] fieldValues, int numKeyFields) {
        RTreeCheckTuple<Integer> checkTuple = new RTreeCheckTuple<Integer>(fieldValues.length, numKeyFields);
        for (int v : fieldValues) {
            checkTuple.appendField(v);
        }
        return checkTuple;
    }

    @Override
    protected void setIntKeyFields(int[] fieldValues, int numKeyFields, int maxValue, Random rnd) {
        int maxFieldPos = numKeyFields / 2;
        for (int j = 0; j < maxFieldPos; j++) {
            int k = maxFieldPos + j;
            int firstValue = rnd.nextInt() % maxValue;
            int secondValue;
            do {
                secondValue = rnd.nextInt() % maxValue;
            } while (secondValue < firstValue);
            fieldValues[j] = firstValue;
            fieldValues[k] = secondValue;
        }
    }

    @Override
    protected void setIntPayloadFields(int[] fieldValues, int numKeyFields, int numFields) {
        for (int j = numKeyFields; j < numFields; j++) {
            fieldValues[j] = intPayloadValue++;
        }
    }

    @Override
    protected Collection createCheckTuplesCollection() {
        return new ArrayList<RTreeCheckTuple>();
    }

    @Override
    protected ArrayTupleBuilder createDeleteTupleBuilder(IIndexTestContext ctx) {
        return new ArrayTupleBuilder(ctx.getFieldCount());
    }

    @Override
    protected boolean checkDiskOrderScanResult(ITupleReference tuple, CheckTuple checkTuple, IIndexTestContext ctx)
            throws HyracksDataException {
        return ctx.getCheckTuples().contains(checkTuple);
    }
}
