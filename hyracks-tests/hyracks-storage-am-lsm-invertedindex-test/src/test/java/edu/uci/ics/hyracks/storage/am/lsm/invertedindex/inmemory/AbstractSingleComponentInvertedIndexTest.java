/*
 * Copyright 2009-2012 by The Regents of the University of California
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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.inmemory;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Iterator;
import java.util.SortedSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.btree.OrderedIndexTestUtils;
import edu.uci.ics.hyracks.storage.am.common.CheckTuple;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndex;
import edu.uci.ics.hyracks.storage.am.common.datagen.DocumentStringFieldValueGenerator;
import edu.uci.ics.hyracks.storage.am.common.datagen.IFieldValueGenerator;
import edu.uci.ics.hyracks.storage.am.common.datagen.SortedIntegerFieldValueGenerator;
import edu.uci.ics.hyracks.storage.am.common.datagen.TupleGenerator;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.common.tuples.PermutingTupleReference;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.LSMInvertedIndexTestHarness;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedListCursor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.DelimitedUTF8StringBinaryTokenizerFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.ITokenFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.UTF8WordTokenFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexTestContext;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexTestContext.InvertedIndexType;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;

@SuppressWarnings("rawtypes")
public abstract class AbstractSingleComponentInvertedIndexTest {
    protected final LSMInvertedIndexTestHarness harness = new LSMInvertedIndexTestHarness();

    protected int NUM_DOCS_TO_INSERT = 10000;

    protected final InvertedIndexType invIndexType;

    public AbstractSingleComponentInvertedIndexTest(InvertedIndexType invIndexType) {
        this.invIndexType = invIndexType;
    }

    @Before
    public void setUp() throws HyracksException {
        harness.setUp();
    }

    public abstract void loadIndex(TupleGenerator tupleGen, InvertedIndexTestContext testCtx) throws IOException,
            IndexException;

    public abstract IBufferCache getBufferCache();

    @Test
    public void stringDocumentTest() throws IOException, IndexException {
        ISerializerDeserializer[] fieldSerdes = new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
        IFieldValueGenerator[] fieldGens = new IFieldValueGenerator[2];
        fieldGens[0] = new DocumentStringFieldValueGenerator(2, 10, 10000, harness.getRandom());
        fieldGens[1] = new SortedIntegerFieldValueGenerator(0);
        TupleGenerator tupleGen = new TupleGenerator(fieldGens, fieldSerdes, 0);

        ITokenFactory tokenFactory = new UTF8WordTokenFactory();
        IBinaryTokenizerFactory tokenizerFactory = new DelimitedUTF8StringBinaryTokenizerFactory(true, false,
                tokenFactory);

        InvertedIndexTestContext testCtx = InvertedIndexTestContext.create(getBufferCache(),
                harness.getMemFreePageManager(), harness.getDiskFileMapProvider(), harness.getInvListsFileRef(),
                fieldSerdes, 1, tokenizerFactory, invIndexType);

        IIndex invIndex = testCtx.getIndex();
        invIndex.create();
        invIndex.activate();

        loadIndex(tupleGen, testCtx);

        // Validate index and compare against expected index.
        invIndex.validate();
        compareActualAndExpectedIndexes(testCtx);

        invIndex.deactivate();
        invIndex.destroy();
    }

    @SuppressWarnings("unchecked")
    protected void compareActualAndExpectedIndexes(InvertedIndexTestContext testCtx) throws HyracksDataException,
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

    @After
    public void tearDown() throws HyracksDataException {
        harness.tearDown();
    }
}
