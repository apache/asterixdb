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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util;

import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.util.GrowableArray;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.ShortSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.btree.OrderedIndexTestUtils;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.CheckTuple;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoader;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.datagen.DocumentStringFieldValueGenerator;
import edu.uci.ics.hyracks.storage.am.common.datagen.IFieldValueGenerator;
import edu.uci.ics.hyracks.storage.am.common.datagen.PersonNameFieldValueGenerator;
import edu.uci.ics.hyracks.storage.am.common.datagen.SortedIntegerFieldValueGenerator;
import edu.uci.ics.hyracks.storage.am.common.datagen.TupleGenerator;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.common.tuples.PermutingTupleReference;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexSearchModifier;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedListCursor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.common.LSMInvertedIndexTestHarness;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.exceptions.OccurrenceThresholdPanicException;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.search.InvertedIndexSearchPredicate;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.DelimitedUTF8StringBinaryTokenizerFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.HashedUTF8NGramTokenFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.HashedUTF8WordTokenFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizer;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IToken;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.ITokenFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.NGramUTF8StringBinaryTokenizerFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.UTF8NGramTokenFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.UTF8WordTokenFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util.LSMInvertedIndexTestContext.InvertedIndexType;

@SuppressWarnings("rawtypes")
public class LSMInvertedIndexTestUtils {

    public static final int TEST_GRAM_LENGTH = 3;

    public static TupleGenerator createStringDocumentTupleGen(Random rnd) throws IOException {
        IFieldValueGenerator[] fieldGens = new IFieldValueGenerator[2];
        fieldGens[0] = new DocumentStringFieldValueGenerator(2, 10, 10000, rnd);
        fieldGens[1] = new SortedIntegerFieldValueGenerator(0);
        ISerializerDeserializer[] fieldSerdes = new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
        TupleGenerator tupleGen = new TupleGenerator(fieldGens, fieldSerdes, 0);
        return tupleGen;
    }

    public static TupleGenerator createPersonNamesTupleGen(Random rnd) throws IOException {
        IFieldValueGenerator[] fieldGens = new IFieldValueGenerator[2];
        fieldGens[0] = new PersonNameFieldValueGenerator(rnd, 0.5f);
        fieldGens[1] = new SortedIntegerFieldValueGenerator(0);
        ISerializerDeserializer[] fieldSerdes = new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
        TupleGenerator tupleGen = new TupleGenerator(fieldGens, fieldSerdes, 0);
        return tupleGen;
    }

    private static ISerializerDeserializer[] getNonHashedIndexFieldSerdes(InvertedIndexType invIndexType)
            throws IndexException {
        ISerializerDeserializer[] fieldSerdes = null;
        switch (invIndexType) {
            case INMEMORY:
            case ONDISK:
            case LSM: {
                fieldSerdes = new ISerializerDeserializer[] { UTF8StringSerializerDeserializer.INSTANCE,
                        IntegerSerializerDeserializer.INSTANCE };
                break;
            }
            case PARTITIONED_INMEMORY:
            case PARTITIONED_ONDISK:
            case PARTITIONED_LSM: {
                // Such indexes also include the set-size for partitioning.
                fieldSerdes = new ISerializerDeserializer[] { UTF8StringSerializerDeserializer.INSTANCE,
                        ShortSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
                break;
            }
            default: {
                throw new IndexException("Unhandled inverted index type '" + invIndexType + "'.");
            }
        }
        return fieldSerdes;
    }

    private static ISerializerDeserializer[] getHashedIndexFieldSerdes(InvertedIndexType invIndexType)
            throws IndexException {
        ISerializerDeserializer[] fieldSerdes = null;
        switch (invIndexType) {
            case INMEMORY:
            case ONDISK:
            case LSM: {
                fieldSerdes = new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE,
                        IntegerSerializerDeserializer.INSTANCE };
                break;
            }
            case PARTITIONED_INMEMORY:
            case PARTITIONED_ONDISK:
            case PARTITIONED_LSM: {
                // Such indexes also include the set-size for partitioning.
                fieldSerdes = new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE,
                        ShortSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
                break;
            }
            default: {
                throw new IndexException("Unhandled inverted index type '" + invIndexType + "'.");
            }
        }
        return fieldSerdes;
    }

    public static LSMInvertedIndexTestContext createWordInvIndexTestContext(LSMInvertedIndexTestHarness harness,
            InvertedIndexType invIndexType) throws IOException, IndexException {
        ISerializerDeserializer[] fieldSerdes = getNonHashedIndexFieldSerdes(invIndexType);
        ITokenFactory tokenFactory = new UTF8WordTokenFactory();
        IBinaryTokenizerFactory tokenizerFactory = new DelimitedUTF8StringBinaryTokenizerFactory(true, false,
                tokenFactory);
        LSMInvertedIndexTestContext testCtx = LSMInvertedIndexTestContext.create(harness, fieldSerdes,
                fieldSerdes.length - 1, tokenizerFactory, invIndexType);
        return testCtx;
    }

    public static LSMInvertedIndexTestContext createHashedWordInvIndexTestContext(LSMInvertedIndexTestHarness harness,
            InvertedIndexType invIndexType) throws IOException, IndexException {
        ISerializerDeserializer[] fieldSerdes = getHashedIndexFieldSerdes(invIndexType);
        ITokenFactory tokenFactory = new HashedUTF8WordTokenFactory();
        IBinaryTokenizerFactory tokenizerFactory = new DelimitedUTF8StringBinaryTokenizerFactory(true, false,
                tokenFactory);
        LSMInvertedIndexTestContext testCtx = LSMInvertedIndexTestContext.create(harness, fieldSerdes,
                fieldSerdes.length - 1, tokenizerFactory, invIndexType);
        return testCtx;
    }

    public static LSMInvertedIndexTestContext createNGramInvIndexTestContext(LSMInvertedIndexTestHarness harness,
            InvertedIndexType invIndexType) throws IOException, IndexException {
        ISerializerDeserializer[] fieldSerdes = getNonHashedIndexFieldSerdes(invIndexType);
        ITokenFactory tokenFactory = new UTF8NGramTokenFactory();
        IBinaryTokenizerFactory tokenizerFactory = new NGramUTF8StringBinaryTokenizerFactory(TEST_GRAM_LENGTH, true,
                true, false, tokenFactory);
        LSMInvertedIndexTestContext testCtx = LSMInvertedIndexTestContext.create(harness, fieldSerdes,
                fieldSerdes.length - 1, tokenizerFactory, invIndexType);
        return testCtx;
    }

    public static LSMInvertedIndexTestContext createHashedNGramInvIndexTestContext(LSMInvertedIndexTestHarness harness,
            InvertedIndexType invIndexType) throws IOException, IndexException {
        ISerializerDeserializer[] fieldSerdes = getHashedIndexFieldSerdes(invIndexType);
        ITokenFactory tokenFactory = new HashedUTF8NGramTokenFactory();
        IBinaryTokenizerFactory tokenizerFactory = new NGramUTF8StringBinaryTokenizerFactory(TEST_GRAM_LENGTH, true,
                true, false, tokenFactory);
        LSMInvertedIndexTestContext testCtx = LSMInvertedIndexTestContext.create(harness, fieldSerdes,
                fieldSerdes.length - 1, tokenizerFactory, invIndexType);
        return testCtx;
    }

    public static void bulkLoadInvIndex(LSMInvertedIndexTestContext testCtx, TupleGenerator tupleGen, int numDocs)
            throws IndexException, IOException {
        SortedSet<CheckTuple> tmpMemIndex = new TreeSet<CheckTuple>();
        // First generate the expected index by inserting the documents one-by-one.
        for (int i = 0; i < numDocs; i++) {
            ITupleReference tuple = tupleGen.next();
            testCtx.insertCheckTuples(tuple, tmpMemIndex);
        }
        ISerializerDeserializer[] fieldSerdes = testCtx.getFieldSerdes();

        // Use the expected index to bulk-load the actual index.
        IIndexBulkLoader bulkLoader = testCtx.getIndex().createBulkLoader(1.0f, false, numDocs, true);
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(testCtx.getFieldSerdes().length);
        ArrayTupleReference tuple = new ArrayTupleReference();
        Iterator<CheckTuple> checkTupleIter = tmpMemIndex.iterator();
        while (checkTupleIter.hasNext()) {
            CheckTuple checkTuple = checkTupleIter.next();
            OrderedIndexTestUtils.createTupleFromCheckTuple(checkTuple, tupleBuilder, tuple, fieldSerdes);
            bulkLoader.add(tuple);
        }
        bulkLoader.end();

        // Add all check tuples from the temp index to the text context.
        testCtx.getCheckTuples().addAll(tmpMemIndex);
    }

    public static void insertIntoInvIndex(LSMInvertedIndexTestContext testCtx, TupleGenerator tupleGen, int numDocs)
            throws IOException, IndexException {
        // InMemoryInvertedIndex only supports insert.
        for (int i = 0; i < numDocs; i++) {
            ITupleReference tuple = tupleGen.next();
            testCtx.getIndexAccessor().insert(tuple);
            testCtx.insertCheckTuples(tuple, testCtx.getCheckTuples());
        }
    }

    public static void deleteFromInvIndex(LSMInvertedIndexTestContext testCtx, Random rnd, int numDocsToDelete)
            throws HyracksDataException, IndexException {
        List<ITupleReference> documentCorpus = testCtx.getDocumentCorpus();
        for (int i = 0; i < numDocsToDelete && !documentCorpus.isEmpty(); i++) {
            int size = documentCorpus.size();
            int tupleIndex = Math.abs(rnd.nextInt()) % size;
            ITupleReference deleteTuple = documentCorpus.get(tupleIndex);
            testCtx.getIndexAccessor().delete(deleteTuple);
            testCtx.deleteCheckTuples(deleteTuple, testCtx.getCheckTuples());
            // Swap tupleIndex with last element.
            documentCorpus.set(tupleIndex, documentCorpus.get(size - 1));
            documentCorpus.remove(size - 1);
        }
    }

    /**
     * Compares actual and expected indexes using the rangeSearch() method of the inverted-index accessor.
     */
    public static void compareActualAndExpectedIndexesRangeSearch(LSMInvertedIndexTestContext testCtx)
            throws HyracksDataException, IndexException {
        IInvertedIndex invIndex = (IInvertedIndex) testCtx.getIndex();
        int tokenFieldCount = invIndex.getTokenTypeTraits().length;
        int invListFieldCount = invIndex.getInvListTypeTraits().length;
        IInvertedIndexAccessor invIndexAccessor = (IInvertedIndexAccessor) invIndex.createAccessor(
                NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        IIndexCursor invIndexCursor = invIndexAccessor.createRangeSearchCursor();
        MultiComparator tokenCmp = MultiComparator.create(invIndex.getTokenCmpFactories());
        IBinaryComparatorFactory[] tupleCmpFactories = new IBinaryComparatorFactory[tokenFieldCount + invListFieldCount];
        for (int i = 0; i < tokenFieldCount; i++) {
            tupleCmpFactories[i] = invIndex.getTokenCmpFactories()[i];
        }
        for (int i = 0; i < invListFieldCount; i++) {
            tupleCmpFactories[tokenFieldCount + i] = invIndex.getInvListCmpFactories()[i];
        }
        MultiComparator tupleCmp = MultiComparator.create(tupleCmpFactories);
        RangePredicate nullPred = new RangePredicate(null, null, true, true, tokenCmp, tokenCmp);
        invIndexAccessor.rangeSearch(invIndexCursor, nullPred);

        // Helpers for generating a serialized inverted-list element from a CheckTuple from the expected index.
        ISerializerDeserializer[] fieldSerdes = testCtx.getFieldSerdes();
        ArrayTupleBuilder expectedBuilder = new ArrayTupleBuilder(fieldSerdes.length);
        ArrayTupleReference expectedTuple = new ArrayTupleReference();

        Iterator<CheckTuple> expectedIter = testCtx.getCheckTuples().iterator();

        // Compare index elements.
        try {
            while (invIndexCursor.hasNext() && expectedIter.hasNext()) {
                invIndexCursor.next();
                ITupleReference actualTuple = invIndexCursor.getTuple();
                CheckTuple expected = expectedIter.next();
                OrderedIndexTestUtils.createTupleFromCheckTuple(expected, expectedBuilder, expectedTuple, fieldSerdes);
                if (tupleCmp.compare(actualTuple, expectedTuple) != 0) {
                    fail("Index entries differ for token '" + expected.getField(0) + "'.");
                }
            }
            if (expectedIter.hasNext()) {
                fail("Indexes do not match. Actual index is missing entries.");
            }
            if (invIndexCursor.hasNext()) {
                fail("Indexes do not match. Actual index contains too many entries.");
            }
        } finally {
            invIndexCursor.close();
        }
    }

    /**
     * Compares actual and expected indexes by comparing their inverted-lists one by one. Exercises the openInvertedListCursor() method of the inverted-index accessor.
     */
    @SuppressWarnings("unchecked")
    public static void compareActualAndExpectedIndexes(LSMInvertedIndexTestContext testCtx)
            throws HyracksDataException, IndexException {
        IInvertedIndex invIndex = (IInvertedIndex) testCtx.getIndex();
        ISerializerDeserializer[] fieldSerdes = testCtx.getFieldSerdes();
        MultiComparator invListCmp = MultiComparator.create(invIndex.getInvListCmpFactories());
        IInvertedIndexAccessor invIndexAccessor = (IInvertedIndexAccessor) testCtx.getIndexAccessor();
        int tokenFieldCount = invIndex.getTokenTypeTraits().length;
        int invListFieldCount = invIndex.getInvListTypeTraits().length;
        // All tokens that were inserted into the indexes.
        Iterator<Comparable> tokensIter = testCtx.getAllTokens().iterator();

        // Search key for finding an inverted-list in the actual index.
        ArrayTupleBuilder searchKeyBuilder = new ArrayTupleBuilder(tokenFieldCount);
        ArrayTupleReference searchKey = new ArrayTupleReference();
        // Cursor over inverted list from actual index.
        IInvertedListCursor actualInvListCursor = invIndexAccessor.createInvertedListCursor();

        // Helpers for generating a serialized inverted-list element from a CheckTuple from the expected index.
        ArrayTupleBuilder expectedBuilder = new ArrayTupleBuilder(fieldSerdes.length);
        // Includes the token fields.
        ArrayTupleReference completeExpectedTuple = new ArrayTupleReference();
        // Field permutation and permuting tuple reference to strip away token fields from completeExpectedTuple.
        int[] fieldPermutation = new int[invListFieldCount];
        for (int i = 0; i < fieldPermutation.length; i++) {
            fieldPermutation[i] = tokenFieldCount + i;
        }
        PermutingTupleReference expectedTuple = new PermutingTupleReference(fieldPermutation);

        // Iterate over all tokens. Find the inverted-lists in actual and expected indexes. Compare the inverted lists,
        while (tokensIter.hasNext()) {
            Comparable token = tokensIter.next();

            // Position inverted-list iterator on expected index.
            CheckTuple checkLowKey = new CheckTuple(tokenFieldCount, tokenFieldCount);
            checkLowKey.appendField(token);
            CheckTuple checkHighKey = new CheckTuple(tokenFieldCount, tokenFieldCount);
            checkHighKey.appendField(token);

            SortedSet<CheckTuple> expectedInvList = OrderedIndexTestUtils.getPrefixExpectedSubset(
                    testCtx.getCheckTuples(), checkLowKey, checkHighKey);
            Iterator<CheckTuple> expectedInvListIter = expectedInvList.iterator();

            // Position inverted-list cursor in actual index.
            OrderedIndexTestUtils.createTupleFromCheckTuple(checkLowKey, searchKeyBuilder, searchKey, fieldSerdes);
            invIndexAccessor.openInvertedListCursor(actualInvListCursor, searchKey);

            if (actualInvListCursor.size() != expectedInvList.size()) {
                fail("Actual and expected inverted lists for token '" + token.toString()
                        + "' have different sizes. Actual size: " + actualInvListCursor.size() + ". Expected size: "
                        + expectedInvList.size() + ".");
            }
            // Compare inverted-list elements.
            int count = 0;
            actualInvListCursor.pinPages();
            try {
                while (actualInvListCursor.hasNext() && expectedInvListIter.hasNext()) {
                    actualInvListCursor.next();
                    ITupleReference actual = actualInvListCursor.getTuple();
                    CheckTuple expected = expectedInvListIter.next();
                    OrderedIndexTestUtils.createTupleFromCheckTuple(expected, expectedBuilder, completeExpectedTuple,
                            fieldSerdes);
                    expectedTuple.reset(completeExpectedTuple);
                    if (invListCmp.compare(actual, expectedTuple) != 0) {
                        fail("Inverted lists of token '" + token + "' differ at position " + count + ".");
                    }
                    count++;
                }
            } finally {
                actualInvListCursor.unpinPages();
            }
        }
    }

    /**
     * Determine the expected results with the simple ScanCount algorithm.
     */
    public static void getExpectedResults(int[] scanCountArray, TreeSet<CheckTuple> checkTuples,
            ITupleReference searchDocument, IBinaryTokenizer tokenizer, ISerializerDeserializer tokenSerde,
            IInvertedIndexSearchModifier searchModifier, List<Integer> expectedResults, InvertedIndexType invIndexType)
            throws IOException {
        boolean isPartitioned = false;
        switch (invIndexType) {
            case INMEMORY:
            case ONDISK:
            case LSM: {
                isPartitioned = false;
                break;
            }
            case PARTITIONED_INMEMORY:
            case PARTITIONED_ONDISK:
            case PARTITIONED_LSM: {
                isPartitioned = true;
                break;
            }
        }
        getExpectedResults(scanCountArray, checkTuples, searchDocument, tokenizer, tokenSerde, searchModifier,
                expectedResults, isPartitioned);
    }

    @SuppressWarnings("unchecked")
    public static void getExpectedResults(int[] scanCountArray, TreeSet<CheckTuple> checkTuples,
            ITupleReference searchDocument, IBinaryTokenizer tokenizer, ISerializerDeserializer tokenSerde,
            IInvertedIndexSearchModifier searchModifier, List<Integer> expectedResults, boolean isPartitioned)
            throws IOException {
        // Reset scan count array.
        Arrays.fill(scanCountArray, 0);
        expectedResults.clear();

        GrowableArray tokenData = new GrowableArray();
        tokenizer.reset(searchDocument.getFieldData(0), searchDocument.getFieldStart(0),
                searchDocument.getFieldLength(0));
        // Run though tokenizer to get number of tokens.
        int numQueryTokens = 0;
        while (tokenizer.hasNext()) {
            tokenizer.next();
            numQueryTokens++;
        }
        short numTokensLowerBound = -1;
        short numTokensUpperBound = -1;
        int invListElementField = 1;
        if (isPartitioned) {
            numTokensLowerBound = searchModifier.getNumTokensLowerBound((short) numQueryTokens);
            numTokensUpperBound = searchModifier.getNumTokensUpperBound((short) numQueryTokens);
            invListElementField = 2;
        }
        int occurrenceThreshold = searchModifier.getOccurrenceThreshold(numQueryTokens);
        tokenizer.reset(searchDocument.getFieldData(0), searchDocument.getFieldStart(0),
                searchDocument.getFieldLength(0));
        while (tokenizer.hasNext()) {
            tokenizer.next();
            IToken token = tokenizer.getToken();
            tokenData.reset();
            token.serializeToken(tokenData);
            ByteArrayInputStream inStream = new ByteArrayInputStream(tokenData.getByteArray(), 0, tokenData.getLength());
            DataInput dataIn = new DataInputStream(inStream);
            Comparable tokenObj = (Comparable) tokenSerde.deserialize(dataIn);
            CheckTuple lowKey;
            if (numTokensLowerBound < 0) {
                // Index is not partitioned, or no length filtering is possible for this search modifier.
                lowKey = new CheckTuple(1, 1);
                lowKey.appendField(tokenObj);
            } else {
                // Index is length partitioned, and search modifier supports length filtering.
                lowKey = new CheckTuple(2, 2);
                lowKey.appendField(tokenObj);
                lowKey.appendField(Short.valueOf(numTokensLowerBound));
            }
            CheckTuple highKey;
            if (numTokensUpperBound < 0) {
                // Index is not partitioned, or no length filtering is possible for this search modifier.
                highKey = new CheckTuple(1, 1);
                highKey.appendField(tokenObj);
            } else {
                // Index is length partitioned, and search modifier supports length filtering.
                highKey = new CheckTuple(2, 2);
                highKey.appendField(tokenObj);
                highKey.appendField(Short.valueOf(numTokensUpperBound));
            }

            // Get view over check tuples containing inverted-list corresponding to token. 
            SortedSet<CheckTuple> invList = OrderedIndexTestUtils.getPrefixExpectedSubset(checkTuples, lowKey, highKey);
            Iterator<CheckTuple> invListIter = invList.iterator();
            // Iterate over inverted list and update scan count array.
            while (invListIter.hasNext()) {
                CheckTuple checkTuple = invListIter.next();
                Integer element = (Integer) checkTuple.getField(invListElementField);
                scanCountArray[element]++;
            }
        }

        // Run through scan count array, and see whether elements satisfy the given occurrence threshold.
        expectedResults.clear();
        for (int i = 0; i < scanCountArray.length; i++) {
            if (scanCountArray[i] >= occurrenceThreshold) {
                expectedResults.add(i);
            }
        }
    }

    public static void testIndexSearch(LSMInvertedIndexTestContext testCtx, TupleGenerator tupleGen, Random rnd,
            int numDocQueries, int numRandomQueries, IInvertedIndexSearchModifier searchModifier, int[] scanCountArray)
            throws IOException, IndexException {
        IInvertedIndex invIndex = testCtx.invIndex;
        IInvertedIndexAccessor accessor = (IInvertedIndexAccessor) invIndex.createAccessor(
                NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        IBinaryTokenizer tokenizer = testCtx.getTokenizerFactory().createTokenizer();
        InvertedIndexSearchPredicate searchPred = new InvertedIndexSearchPredicate(tokenizer, searchModifier);
        List<ITupleReference> documentCorpus = testCtx.getDocumentCorpus();
        // Project away the primary-key field.
        int[] fieldPermutation = new int[] { 0 };
        PermutingTupleReference searchDocument = new PermutingTupleReference(fieldPermutation);

        int numQueries = numDocQueries + numRandomQueries;
        for (int i = 0; i < numQueries; i++) {
            // If number of documents in the corpus is less than numDocQueries, then replace the remaining ones with random queries.
            if (i >= numDocQueries || i >= documentCorpus.size()) {
                // Generate a random query.
                ITupleReference randomQuery = tupleGen.next();
                searchDocument.reset(randomQuery);
            } else {
                // Pick a random document from the corpus to use as the search query.
                int queryIndex = Math.abs(rnd.nextInt() % documentCorpus.size());
                searchDocument.reset(documentCorpus.get(queryIndex));
            }

            // Set query tuple in search predicate.
            searchPred.setQueryTuple(searchDocument);
            searchPred.setQueryFieldIndex(0);

            IIndexCursor resultCursor = accessor.createSearchCursor();
            boolean panic = false;
            try {
                accessor.search(resultCursor, searchPred);
            } catch (OccurrenceThresholdPanicException e) {
                // ignore panic queries.
                panic = true;
            }

            try {
                if (!panic) {
                    // Consume cursor and deserialize results so we can sort them. Some search cursors may not deliver the result sorted (e.g., LSM search cursor).
                    ArrayList<Integer> actualResults = new ArrayList<Integer>();
                    try {
                        while (resultCursor.hasNext()) {
                            resultCursor.next();
                            ITupleReference resultTuple = resultCursor.getTuple();
                            int actual = IntegerSerializerDeserializer.getInt(resultTuple.getFieldData(0),
                                    resultTuple.getFieldStart(0));
                            actualResults.add(Integer.valueOf(actual));
                        }
                    } catch (OccurrenceThresholdPanicException e) {
                        // Ignore panic queries.
                        continue;
                    }
                    Collections.sort(actualResults);

                    // Get expected results.
                    List<Integer> expectedResults = new ArrayList<Integer>();
                    LSMInvertedIndexTestUtils.getExpectedResults(scanCountArray, testCtx.getCheckTuples(),
                            searchDocument, tokenizer, testCtx.getFieldSerdes()[0], searchModifier, expectedResults,
                            testCtx.getInvertedIndexType());

                    Iterator<Integer> expectedIter = expectedResults.iterator();
                    Iterator<Integer> actualIter = actualResults.iterator();
                    while (expectedIter.hasNext() && actualIter.hasNext()) {
                        int expected = expectedIter.next();
                        int actual = actualIter.next();
                        if (actual != expected) {
                            fail("Query results do not match. Encountered: " + actual + ". Expected: " + expected + "");
                        }
                    }
                    if (expectedIter.hasNext()) {
                        fail("Query results do not match. Actual results missing.");
                    }
                    if (actualIter.hasNext()) {
                        fail("Query results do not match. Actual contains too many results.");
                    }
                }
            } finally {
                resultCursor.close();
            }
        }
    }
}
