/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.storage.am.btree;

import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.btree.util.BTreeUtils;
import org.apache.hyracks.storage.am.common.CheckTuple;
import org.apache.hyracks.storage.am.common.IIndexTestContext;
import org.apache.hyracks.storage.am.common.TreeIndexTestUtils;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@SuppressWarnings("rawtypes")
public class OrderedIndexTestUtils extends TreeIndexTestUtils {
    private static final Logger LOGGER = LogManager.getLogger();

    private static void compareActualAndExpected(ITupleReference actual, CheckTuple expected,
            ISerializerDeserializer[] fieldSerdes) throws HyracksDataException {
        for (int i = 0; i < fieldSerdes.length; i++) {
            ByteArrayInputStream inStream =
                    new ByteArrayInputStream(actual.getFieldData(i), actual.getFieldStart(i), actual.getFieldLength(i));
            DataInput dataIn = new DataInputStream(inStream);
            Object actualObj = fieldSerdes[i].deserialize(dataIn);
            if (!actualObj.equals(expected.getField(i))) {
                fail("Actual and expected fields do not match on field " + i + ".\nExpected: " + expected.getField(i)
                        + "\nActual  : " + actualObj);
            }
        }
    }

    @SuppressWarnings("unchecked")
    // Create a new TreeSet containing the elements satisfying the prefix search.
    // Implementing prefix search by changing compareTo() in CheckTuple does not
    // work.
    public static SortedSet<CheckTuple> getPrefixExpectedSubset(TreeSet<CheckTuple> checkTuples, CheckTuple lowKey,
            CheckTuple highKey) {
        lowKey.setIsHighKey(false);
        highKey.setIsHighKey(true);
        CheckTuple low = checkTuples.ceiling(lowKey);
        CheckTuple high = checkTuples.floor(highKey);
        if (low == null || high == null) {
            // Must be empty.
            return new TreeSet<>();
        }
        if (high.compareTo(low) < 0) {
            // Must be empty.
            return new TreeSet<>();
        }
        return checkTuples.subSet(low, true, high, true);
    }

    @SuppressWarnings("unchecked")
    public void checkRangeSearch(IIndexTestContext ctx, ITupleReference lowKey, ITupleReference highKey,
            boolean lowKeyInclusive, boolean highKeyInclusive) throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Testing Range Search.");
        }
        MultiComparator lowKeyCmp = BTreeUtils.getSearchMultiComparator(ctx.getComparatorFactories(), lowKey);
        MultiComparator highKeyCmp = BTreeUtils.getSearchMultiComparator(ctx.getComparatorFactories(), highKey);
        IIndexCursor searchCursor = ctx.getIndexAccessor().createSearchCursor(false);
        try {
            RangePredicate rangePred =
                    new RangePredicate(lowKey, highKey, lowKeyInclusive, highKeyInclusive, lowKeyCmp, highKeyCmp);
            int actualCount = 0;
            SortedSet<CheckTuple> expectedSubset = null;
            ctx.getIndexAccessor().search(searchCursor, rangePred);
            try {
                // Get the subset of elements from the expected set within given key
                // range.
                CheckTuple lowKeyCheck =
                        createCheckTupleFromTuple(lowKey, ctx.getFieldSerdes(), lowKeyCmp.getKeyFieldCount());
                CheckTuple highKeyCheck =
                        createCheckTupleFromTuple(highKey, ctx.getFieldSerdes(), highKeyCmp.getKeyFieldCount());
                if (lowKeyCmp.getKeyFieldCount() < ctx.getKeyFieldCount()
                        || highKeyCmp.getKeyFieldCount() < ctx.getKeyFieldCount()) {
                    // Searching on a key prefix (low key or high key or both).
                    expectedSubset = getPrefixExpectedSubset((TreeSet<CheckTuple>) ctx.getCheckTuples(), lowKeyCheck,
                            highKeyCheck);
                } else {
                    // Searching on all key fields.
                    expectedSubset = ((TreeSet<CheckTuple>) ctx.getCheckTuples()).subSet(lowKeyCheck, lowKeyInclusive,
                            highKeyCheck, highKeyInclusive);
                }
                Iterator<CheckTuple> checkIter = expectedSubset.iterator();
                while (searchCursor.hasNext()) {
                    if (!checkIter.hasNext()) {
                        fail("Range search returned more answers than expected.\nExpected: " + expectedSubset.size());
                    }
                    searchCursor.next();
                    CheckTuple expectedTuple = checkIter.next();
                    ITupleReference tuple = searchCursor.getTuple();
                    compareActualAndExpected(tuple, expectedTuple, ctx.getFieldSerdes());
                    actualCount++;
                }
            } finally {
                searchCursor.close();
            }
            if (actualCount < expectedSubset.size()) {
                fail("Range search returned fewer answers than expected.\nExpected: " + expectedSubset.size()
                        + "\nActual  : " + actualCount);
            }
        } finally {
            searchCursor.destroy();
        }
    }

    public void checkPointSearches(IIndexTestContext ictx) throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Testing Point Searches On All Expected Keys.");
        }
        OrderedIndexTestContext ctx = (OrderedIndexTestContext) ictx;
        IIndexCursor searchCursor = ctx.getIndexAccessor().createSearchCursor(false);
        try {
            ArrayTupleBuilder lowKeyBuilder = new ArrayTupleBuilder(ctx.getKeyFieldCount());
            ArrayTupleReference lowKey = new ArrayTupleReference();
            ArrayTupleBuilder highKeyBuilder = new ArrayTupleBuilder(ctx.getKeyFieldCount());
            ArrayTupleReference highKey = new ArrayTupleReference();
            RangePredicate rangePred = new RangePredicate(lowKey, highKey, true, true, null, null);

            // Iterate through expected tuples, and perform a point search in the
            // BTree to verify the tuple can be reached.
            for (CheckTuple checkTuple : ctx.getCheckTuples()) {
                createTupleFromCheckTuple(checkTuple, lowKeyBuilder, lowKey, ctx.getFieldSerdes());
                createTupleFromCheckTuple(checkTuple, highKeyBuilder, highKey, ctx.getFieldSerdes());
                MultiComparator lowKeyCmp = BTreeUtils.getSearchMultiComparator(ctx.getComparatorFactories(), lowKey);
                MultiComparator highKeyCmp = BTreeUtils.getSearchMultiComparator(ctx.getComparatorFactories(), highKey);

                rangePred.setLowKey(lowKey, true);
                rangePred.setHighKey(highKey, true);
                rangePred.setLowKeyComparator(lowKeyCmp);
                rangePred.setHighKeyComparator(highKeyCmp);

                ctx.getIndexAccessor().search(searchCursor, rangePred);
                try {
                    // We expect exactly one answer.
                    if (searchCursor.hasNext()) {
                        searchCursor.next();
                        ITupleReference tuple = searchCursor.getTuple();
                        compareActualAndExpected(tuple, checkTuple, ctx.getFieldSerdes());
                    }
                    if (searchCursor.hasNext()) {
                        fail("Point search returned more than one answer.");
                    }
                } finally {
                    searchCursor.close();
                }
            }
        } finally {
            searchCursor.destroy();
        }
    }

    @SuppressWarnings("unchecked")
    public void insertSortedIntTuples(IIndexTestContext ctx, int numTuples, Random rnd) throws Exception {
        int fieldCount = ctx.getFieldCount();
        int numKeyFields = ctx.getKeyFieldCount();
        int[] fieldValues = new int[ctx.getFieldCount()];
        int maxValue = (int) Math.ceil(Math.pow(numTuples, 1.0 / numKeyFields));
        Collection<CheckTuple> tmpCheckTuples = createCheckTuplesCollection();
        for (int i = 0; i < numTuples; i++) {
            // Set keys.
            setIntKeyFields(fieldValues, numKeyFields, maxValue, rnd);
            // Set values.
            setIntPayloadFields(fieldValues, numKeyFields, fieldCount);

            // Set expected values. (We also use these as the pre-sorted stream
            // for ordered indexes bulk loading).
            ctx.insertCheckTuple(createIntCheckTuple(fieldValues, ctx.getKeyFieldCount()), tmpCheckTuples);
        }
        insertCheckTuples(ctx, tmpCheckTuples);

        // Add tmpCheckTuples to ctx check tuples for comparing searches.
        for (CheckTuple checkTuple : tmpCheckTuples) {
            ctx.insertCheckTuple(checkTuple, ctx.getCheckTuples());
        }
    }

    @SuppressWarnings("unchecked")
    public void insertSortedStringTuples(IIndexTestContext ctx, int numTuples, Random rnd) throws Exception {
        int fieldCount = ctx.getFieldCount();
        int numKeyFields = ctx.getKeyFieldCount();
        String[] fieldValues = new String[fieldCount];
        TreeSet<CheckTuple> tmpCheckTuples = new TreeSet<>();
        for (int i = 0; i < numTuples; i++) {
            // Set keys.
            for (int j = 0; j < numKeyFields; j++) {
                int length = (Math.abs(rnd.nextInt()) % 10) + 1;
                fieldValues[j] = getRandomString(length, rnd);
            }
            // Set values.
            for (int j = numKeyFields; j < fieldCount; j++) {
                fieldValues[j] = getRandomString(5, rnd);
            }
            // Set expected values. We also use these as the pre-sorted stream
            // for bulk loading.
            ctx.insertCheckTuple(createStringCheckTuple(fieldValues, ctx.getKeyFieldCount()), tmpCheckTuples);
        }
        insertCheckTuples(ctx, tmpCheckTuples);

        // Add tmpCheckTuples to ctx check tuples for comparing searches.
        for (CheckTuple checkTuple : tmpCheckTuples) {
            ctx.insertCheckTuple(checkTuple, ctx.getCheckTuples());
        }
    }

    public static void insertCheckTuples(IIndexTestContext ctx, Collection<CheckTuple> checkTuples)
            throws HyracksDataException {
        insertCheckTuples(ctx, checkTuples, false);
    }

    public static void insertCheckTuples(IIndexTestContext ctx, Collection<CheckTuple> checkTuples, boolean filtered)
            throws HyracksDataException {
        int fieldCount = ctx.getFieldCount();
        int numTuples = checkTuples.size();
        ArrayTupleBuilder tupleBuilder =
                filtered ? new ArrayTupleBuilder(fieldCount + 1) : new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();

        int c = 1;
        for (CheckTuple checkTuple : checkTuples) {
            if (LOGGER.isInfoEnabled()) {
                if (c % (numTuples / 10) == 0) {
                    LOGGER.info("Inserting Tuple " + c + "/" + numTuples);
                }
            }
            createTupleFromCheckTuple(checkTuple, tupleBuilder, tuple, ctx.getFieldSerdes(), filtered);
            ctx.getIndexAccessor().insert(tuple);
            c++;
        }
    }

    public Pair<ITupleReference, ITupleReference> insertStringTuples(IIndexTestContext ctx, int numTuples, Random rnd)
            throws Exception {
        return insertStringTuples(ctx, numTuples, false, rnd);
    }

    @SuppressWarnings("unchecked")
    public Pair<ITupleReference, ITupleReference> insertStringTuples(IIndexTestContext ctx, int numTuples,
            boolean filtered, Random rnd) throws Exception {
        int fieldCount = ctx.getFieldCount();
        int numKeyFields = ctx.getKeyFieldCount();
        String[] fieldValues = new String[fieldCount];
        MutablePair<ITupleReference, ITupleReference> minMax = null;
        for (int i = 0; i < numTuples; i++) {
            if (LOGGER.isInfoEnabled()) {
                if ((i + 1) % (numTuples / Math.min(10, numTuples)) == 0) {
                    LOGGER.info("Inserting Tuple " + (i + 1) + "/" + numTuples);
                }
            }
            // Set keys.
            for (int j = 0; j < numKeyFields; j++) {
                int length = (Math.abs(rnd.nextInt()) % 10) + 1;
                fieldValues[j] = getRandomString(length, rnd);
            }
            // Set values.
            for (int j = numKeyFields; j < fieldCount; j++) {
                fieldValues[j] = getRandomString(5, rnd);
            }
            TupleUtils.createTuple(ctx.getTupleBuilder(), ctx.getTuple(), ctx.getFieldSerdes(), filtered,
                    (Object[]) fieldValues);
            try {
                ctx.getIndexAccessor().insert(ctx.getTuple());
                // Set expected values. Do this only after insertion succeeds
                // because we ignore duplicate keys.
                ctx.insertCheckTuple(createStringCheckTuple(fieldValues, ctx.getKeyFieldCount()), ctx.getCheckTuples());
                if (filtered) {
                    addFilterField(ctx, minMax);
                }
            } catch (HyracksDataException e) {
                // Ignore duplicate key insertions.
                if (e.getErrorCode() != ErrorCode.DUPLICATE_KEY) {
                    throw e;
                }
            }
        }
        return minMax;
    }

    public void upsertStringTuples(IIndexTestContext ictx, int numTuples, Random rnd) throws Exception {
        OrderedIndexTestContext ctx = (OrderedIndexTestContext) ictx;
        int fieldCount = ctx.getFieldCount();
        int numKeyFields = ctx.getKeyFieldCount();
        String[] fieldValues = new String[fieldCount];
        for (int i = 0; i < numTuples; i++) {
            if (LOGGER.isInfoEnabled()) {
                if ((i + 1) % (numTuples / Math.min(10, numTuples)) == 0) {
                    LOGGER.info("Inserting Tuple " + (i + 1) + "/" + numTuples);
                }
            }
            // Set keys.
            for (int j = 0; j < numKeyFields; j++) {
                int length = (Math.abs(rnd.nextInt()) % 10) + 1;
                fieldValues[j] = getRandomString(length, rnd);
            }
            // Set values.
            for (int j = numKeyFields; j < fieldCount; j++) {
                fieldValues[j] = getRandomString(5, rnd);
            }
            TupleUtils.createTuple(ctx.getTupleBuilder(), ctx.getTuple(), ctx.getFieldSerdes(), (Object[]) fieldValues);
            ctx.getIndexAccessor().upsert(ctx.getTuple());
            ctx.upsertCheckTuple(createStringCheckTuple(fieldValues, ctx.getKeyFieldCount()), ctx.getCheckTuples());
        }
    }

    public void bulkLoadStringTuples(IIndexTestContext ctx, int numTuples, Random rnd) throws Exception {
        bulkLoadStringTuples(ctx, numTuples, false, rnd);
    }

    @SuppressWarnings("unchecked")
    public void bulkLoadStringTuples(IIndexTestContext ctx, int numTuples, boolean filtered, Random rnd)
            throws Exception {
        int fieldCount = ctx.getFieldCount();
        int numKeyFields = ctx.getKeyFieldCount();
        String[] fieldValues = new String[fieldCount];
        TreeSet<CheckTuple> tmpCheckTuples = new TreeSet<>();
        for (int i = 0; i < numTuples; i++) {
            // Set keys.
            for (int j = 0; j < numKeyFields; j++) {
                int length = (Math.abs(rnd.nextInt()) % 10) + 1;
                fieldValues[j] = getRandomString(length, rnd);
            }
            // Set values.
            for (int j = numKeyFields; j < fieldCount; j++) {
                fieldValues[j] = getRandomString(5, rnd);
            }
            // Set expected values. We also use these as the pre-sorted stream
            // for bulk loading.
            ctx.insertCheckTuple(createStringCheckTuple(fieldValues, ctx.getKeyFieldCount()), tmpCheckTuples);
        }
        bulkLoadCheckTuples(ctx, tmpCheckTuples, filtered);

        // Add tmpCheckTuples to ctx check tuples for comparing searches.
        for (CheckTuple checkTuple : tmpCheckTuples) {
            ctx.insertCheckTuple(checkTuple, ctx.getCheckTuples());
        }
    }

    @Override
    public void upsertIntTuples(IIndexTestContext ictx, int numTuples, Random rnd) throws Exception {
        OrderedIndexTestContext ctx = (OrderedIndexTestContext) ictx;
        int fieldCount = ctx.getFieldCount();
        int numKeyFields = ctx.getKeyFieldCount();
        int[] fieldValues = new int[ctx.getFieldCount()];
        // Scale range of values according to number of keys.
        // For example, for 2 keys we want the square root of numTuples, for 3
        // keys the cube root of numTuples, etc.
        int maxValue = (int) Math.ceil(Math.pow(numTuples, 1.0 / numKeyFields));
        for (int i = 0; i < numTuples; i++) {
            // Set keys.
            setIntKeyFields(fieldValues, numKeyFields, maxValue, rnd);
            // Set values.
            setIntPayloadFields(fieldValues, numKeyFields, fieldCount);
            TupleUtils.createIntegerTuple(ctx.getTupleBuilder(), ctx.getTuple(), fieldValues);
            if (LOGGER.isInfoEnabled()) {
                if ((i + 1) % (numTuples / Math.min(10, numTuples)) == 0) {
                    LOGGER.info("Inserting Tuple " + (i + 1) + "/" + numTuples);
                }
            }
            ctx.getIndexAccessor().upsert(ctx.getTuple());
            ctx.upsertCheckTuple(createIntCheckTuple(fieldValues, ctx.getKeyFieldCount()), ctx.getCheckTuples());
        }
    }

    @SuppressWarnings("unchecked")
    public void updateTuples(IIndexTestContext ictx, int numTuples, Random rnd) throws Exception {
        OrderedIndexTestContext ctx = (OrderedIndexTestContext) ictx;
        int fieldCount = ctx.getFieldCount();
        int keyFieldCount = ctx.getKeyFieldCount();
        // This is a noop because we can only update non-key fields.
        if (fieldCount == keyFieldCount) {
            return;
        }
        ArrayTupleBuilder updateTupleBuilder = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference updateTuple = new ArrayTupleReference();
        int numCheckTuples = ctx.getCheckTuples().size();
        // Copy CheckTuple references into array, so we can randomly pick from
        // there.
        CheckTuple[] checkTuples = new CheckTuple[numCheckTuples];
        int idx = 0;
        for (CheckTuple checkTuple : ctx.getCheckTuples()) {
            checkTuples[idx++] = checkTuple;
        }
        for (int i = 0; i < numTuples && numCheckTuples > 0; i++) {
            if (LOGGER.isInfoEnabled()) {
                if ((i + 1) % (numTuples / Math.min(10, numTuples)) == 0) {
                    LOGGER.info("Updating Tuple " + (i + 1) + "/" + numTuples);
                }
            }
            int checkTupleIdx = Math.abs(rnd.nextInt() % numCheckTuples);
            CheckTuple checkTuple = checkTuples[checkTupleIdx];
            // Update check tuple's non-key fields.
            for (int j = keyFieldCount; j < fieldCount; j++) {
                Comparable newValue = getRandomUpdateValue(ctx.getFieldSerdes()[j], rnd);
                checkTuple.setField(j, newValue);
            }

            createTupleFromCheckTuple(checkTuple, updateTupleBuilder, updateTuple, ctx.getFieldSerdes());
            ctx.getIndexAccessor().update(updateTuple);

            // Swap with last "valid" CheckTuple.
            CheckTuple tmp = checkTuples[numCheckTuples - 1];
            checkTuples[numCheckTuples - 1] = checkTuple;
            checkTuples[checkTupleIdx] = tmp;
            numCheckTuples--;
        }
    }

    public CheckTuple createStringCheckTuple(String[] fieldValues, int numKeyFields) {
        CheckTuple<String> checkTuple = new CheckTuple<>(fieldValues.length, numKeyFields);
        for (String s : fieldValues) {
            checkTuple.appendField(s);
        }
        return checkTuple;
    }

    private static Comparable getRandomUpdateValue(ISerializerDeserializer serde, Random rnd) {
        if (serde instanceof IntegerSerializerDeserializer) {
            return Integer.valueOf(rnd.nextInt());
        } else if (serde instanceof UTF8StringSerializerDeserializer) {
            return getRandomString(10, rnd);
        }
        return null;
    }

    public static String getRandomString(int length, Random rnd) {
        String s = Long.toHexString(Double.doubleToLongBits(rnd.nextDouble()));
        StringBuilder strBuilder = new StringBuilder();
        for (int i = 0; i < s.length() && i < length; i++) {
            strBuilder.append(s.charAt(Math.abs(rnd.nextInt()) % s.length()));
        }
        return strBuilder.toString();
    }

    @Override
    protected CheckTuple createCheckTuple(int numFields, int numKeyFields) {
        return new CheckTuple(numFields, numKeyFields);
    }

    @Override
    protected ISearchPredicate createNullSearchPredicate() {
        return new RangePredicate(null, null, true, true, null, null);
    }

    @Override
    public void checkExpectedResults(IIndexCursor cursor, Collection checkTuples, ISerializerDeserializer[] fieldSerdes,
            int keyFieldCount, Iterator<CheckTuple> checkIter) throws Exception {
        int actualCount = 0;
        while (cursor.hasNext()) {
            if (!checkIter.hasNext()) {
                fail("Ordered scan returned more answers than expected.\nExpected: " + checkTuples.size());
            }
            cursor.next();
            CheckTuple expectedTuple = checkIter.next();
            ITupleReference tuple = cursor.getTuple();
            compareActualAndExpected(tuple, expectedTuple, fieldSerdes);
            actualCount++;
        }
        if (actualCount < checkTuples.size()) {
            fail("Ordered scan returned fewer answers than expected.\nExpected: " + checkTuples.size() + "\nActual  : "
                    + actualCount);
        }
    }

    @Override
    protected CheckTuple createIntCheckTuple(int[] fieldValues, int numKeyFields) {
        CheckTuple<Integer> checkTuple = new CheckTuple<>(fieldValues.length, numKeyFields);
        for (int v : fieldValues) {
            checkTuple.appendField(v);
        }
        return checkTuple;
    }

    @Override
    protected void setIntKeyFields(int[] fieldValues, int numKeyFields, int maxValue, Random rnd) {
        for (int j = 0; j < numKeyFields; j++) {
            fieldValues[j] = rnd.nextInt() % maxValue;
        }
    }

    @Override
    protected void setIntPayloadFields(int[] fieldValues, int numKeyFields, int numFields) {
        for (int j = numKeyFields; j < numFields; j++) {
            fieldValues[j] = j;
        }
    }

    @Override
    protected Collection createCheckTuplesCollection() {
        return new TreeSet<CheckTuple>();
    }

    @Override
    protected ArrayTupleBuilder createDeleteTupleBuilder(IIndexTestContext ctx) {
        return new ArrayTupleBuilder(ctx.getKeyFieldCount());
    }

    @Override
    protected boolean checkDiskOrderScanResult(ITupleReference tuple, CheckTuple checkTuple, IIndexTestContext ctx)
            throws HyracksDataException {
        @SuppressWarnings("unchecked")
        TreeSet<CheckTuple> checkTuples = (TreeSet<CheckTuple>) ctx.getCheckTuples();
        CheckTuple matchingCheckTuple = checkTuples.floor(checkTuple);
        if (matchingCheckTuple == null) {
            return false;
        }
        compareActualAndExpected(tuple, matchingCheckTuple, ctx.getFieldSerdes());
        return true;
    }
}
