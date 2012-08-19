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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util;

import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.btree.OrderedIndexTestUtils;
import edu.uci.ics.hyracks.storage.am.common.CheckTuple;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoader;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.datagen.DocumentStringFieldValueGenerator;
import edu.uci.ics.hyracks.storage.am.common.datagen.IFieldValueGenerator;
import edu.uci.ics.hyracks.storage.am.common.datagen.SortedIntegerFieldValueGenerator;
import edu.uci.ics.hyracks.storage.am.common.datagen.TupleGenerator;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.common.tuples.PermutingTupleReference;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexSearchModifier;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedListCursor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.common.LSMInvertedIndexTestHarness;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.DelimitedUTF8StringBinaryTokenizerFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizer;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IToken;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.ITokenFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.UTF8WordTokenFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexTestContext.InvertedIndexType;

@SuppressWarnings("rawtypes")
public class InvertedIndexTestUtils {

    public static TupleGenerator createStringDocumentTupleGen(Random rnd) throws IOException {
        IFieldValueGenerator[] fieldGens = new IFieldValueGenerator[2];
        fieldGens[0] = new DocumentStringFieldValueGenerator(2, 10, 10000, rnd);
        fieldGens[1] = new SortedIntegerFieldValueGenerator(0);
        ISerializerDeserializer[] fieldSerdes = new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
        TupleGenerator tupleGen = new TupleGenerator(fieldGens, fieldSerdes, 0);
        return tupleGen;
    }

    public static InvertedIndexTestContext createWordInvIndexTestContext(LSMInvertedIndexTestHarness harness,
            InvertedIndexType invIndexType) throws IOException, IndexException {
        ISerializerDeserializer[] fieldSerdes = new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
        ITokenFactory tokenFactory = new UTF8WordTokenFactory();
        IBinaryTokenizerFactory tokenizerFactory = new DelimitedUTF8StringBinaryTokenizerFactory(true, false,
                tokenFactory);
        InvertedIndexTestContext testCtx = InvertedIndexTestContext.create(harness, fieldSerdes, 1, tokenizerFactory,
                invIndexType);
        return testCtx;
    }

    public static void bulkLoadInvIndex(InvertedIndexTestContext testCtx, TupleGenerator tupleGen, int numDocs)
            throws IndexException, IOException {
        SortedSet<CheckTuple> tmpMemIndex = new TreeSet<CheckTuple>();;
        // First generate the expected index by inserting the documents one-by-one.
        for (int i = 0; i < numDocs; i++) {
            ITupleReference tuple = tupleGen.next();
            testCtx.insertCheckTuples(tuple, tmpMemIndex);
        }
        ISerializerDeserializer[] fieldSerdes = testCtx.getFieldSerdes();

        // Use the expected index to bulk-load the actual index.
        IIndexBulkLoader bulkLoader = testCtx.getIndex().createBulkLoader(1.0f, false);
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(testCtx.getFieldSerdes().length);
        ArrayTupleReference tuple = new ArrayTupleReference();
        Iterator<CheckTuple> checkTupleIter = testCtx.getCheckTuples().iterator();
        while (checkTupleIter.hasNext()) {
            CheckTuple checkTuple = checkTupleIter.next();
            OrderedIndexTestUtils.createTupleFromCheckTuple(checkTuple, tupleBuilder, tuple, fieldSerdes);
            bulkLoader.add(tuple);
        }
        bulkLoader.end();

        // Add all check tuples from the temp index to the text context.
        testCtx.getCheckTuples().addAll(tmpMemIndex);
    }

    public static void insertIntoInvIndex(InvertedIndexTestContext testCtx, TupleGenerator tupleGen, int numDocs)
            throws IOException, IndexException {
        // InMemoryInvertedIndex only supports insert.
        for (int i = 0; i < numDocs; i++) {
            ITupleReference tuple = tupleGen.next();
            testCtx.getIndexAccessor().insert(tuple);
            testCtx.insertCheckTuples(tuple, testCtx.getCheckTuples());
        }
    }

    @SuppressWarnings("unchecked")
    public static void compareActualAndExpectedIndexes(InvertedIndexTestContext testCtx) throws HyracksDataException,
            IndexException {
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
    @SuppressWarnings("unchecked")
    public static void getExpectedResults(int[] scanCountArray, TreeSet<CheckTuple> checkTuples,
            ITupleReference searchDocument, IBinaryTokenizer tokenizer, ISerializerDeserializer tokenSerde,
            IInvertedIndexSearchModifier searchModifier, List<Integer> expectedResults) throws IOException {
        // Reset scan count array.
        Arrays.fill(scanCountArray, 0);
        expectedResults.clear();

        ByteArrayAccessibleOutputStream baaos = new ByteArrayAccessibleOutputStream();
        tokenizer.reset(searchDocument.getFieldData(0), searchDocument.getFieldStart(0),
                searchDocument.getFieldLength(0));
        int numQueryTokens = 0;
        while (tokenizer.hasNext()) {
            tokenizer.next();
            IToken token = tokenizer.getToken();
            baaos.reset();
            DataOutput out = new DataOutputStream(baaos);
            token.serializeToken(out);
            ByteArrayInputStream inStream = new ByteArrayInputStream(baaos.getByteArray(), 0, baaos.size());
            DataInput dataIn = new DataInputStream(inStream);
            Comparable tokenObj = (Comparable) tokenSerde.deserialize(dataIn);
            CheckTuple lowKey = new CheckTuple(1, 1);
            lowKey.appendField(tokenObj);
            CheckTuple highKey = new CheckTuple(1, 1);
            highKey.appendField(tokenObj);

            // Get view over check tuples containing inverted-list corresponding to token. 
            SortedSet<CheckTuple> invList = OrderedIndexTestUtils.getPrefixExpectedSubset(checkTuples, lowKey, highKey);
            Iterator<CheckTuple> invListIter = invList.iterator();
            // Iterate over inverted list and update scan count array.
            while (invListIter.hasNext()) {
                CheckTuple checkTuple = invListIter.next();
                Integer element = (Integer) checkTuple.getField(1);
                scanCountArray[element]++;
            }
            numQueryTokens++;
        }

        int occurrenceThreshold = searchModifier.getOccurrenceThreshold(numQueryTokens);
        // Run through scan count array, and see whether elements satisfy the given occurrence threshold.
        expectedResults.clear();
        for (int i = 0; i < scanCountArray.length; i++) {
            if (scanCountArray[i] >= occurrenceThreshold) {
                expectedResults.add(i);
            }
        }
    }
    
    /*
    public static OnDiskInvertedIndex createTestInvertedIndex(LSMInvertedIndexTestHarness harness, IBinaryTokenizer tokenizer)
            throws HyracksDataException {
        ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();
        ITypeTraits[] btreeTypeTraits = new ITypeTraits[] { UTF8StringPointable.TYPE_TRAITS,
                IntegerPointable.TYPE_TRAITS, IntegerPointable.TYPE_TRAITS, IntegerPointable.TYPE_TRAITS,
                IntegerPointable.TYPE_TRAITS };
        ITreeIndexTupleWriterFactory tupleWriterFactory = new TypeAwareTupleWriterFactory(btreeTypeTraits);
        ITreeIndexFrameFactory leafFrameFactory = new BTreeNSMLeafFrameFactory(tupleWriterFactory);
        ITreeIndexFrameFactory interiorFrameFactory = new BTreeNSMInteriorFrameFactory(tupleWriterFactory);
        IFreePageManager freePageManager = new LinkedListFreePageManager(harness.getDiskBufferCache(), 0,
                metaFrameFactory);
        IBinaryComparatorFactory[] btreeCmpFactories = new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory
                .of(UTF8StringPointable.FACTORY) };
        return InvertedIndexUtils.createInvertedIndex(harness.getDiskBufferCache(), 
                harness.getInvListTypeTraits(), harness.getInvListCmpFactories(), tokenizer);
    }

    public static InMemoryInvertedIndex createInMemoryInvertedIndex(LSMInvertedIndexTestHarness harness,
            IBinaryTokenizer tokenizer) {
        return InvertedIndexUtils.createInMemoryBTreeInvertedindex(harness.getMemBufferCache(),
                harness.getMemFreePageManager(), harness.getTokenTypeTraits(), harness.getInvListTypeTraits(),
                harness.getTokenCmpFactories(), harness.getInvListCmpFactories(),
                tokenizer);
    }

    public static LSMInvertedIndex createLSMInvertedIndex(LSMInvertedIndexTestHarness harness,
            IBinaryTokenizer tokenizer) {
        return InvertedIndexUtils.createLSMInvertedIndex(harness.getMemBufferCache(),
                harness.getMemFreePageManager(), harness.getTokenTypeTraits(), harness.getInvListTypeTraits(),
                harness.getTokenCmpFactories(), harness.getInvListCmpFactories(),
                tokenizer, harness.getDiskBufferCache(),
                new LinkedListFreePageManagerFactory(harness.getDiskBufferCache(), new LIFOMetaDataFrameFactory()),
                harness.getIOManager(), harness.getOnDiskDir(), harness.getDiskFileMapProvider());
    }
    */
}
