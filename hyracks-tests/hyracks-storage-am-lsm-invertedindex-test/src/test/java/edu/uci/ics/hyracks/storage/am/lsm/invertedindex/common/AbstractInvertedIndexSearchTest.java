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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.common;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndex;
import edu.uci.ics.hyracks.storage.am.common.datagen.TupleGenerator;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.tuples.PermutingTupleReference;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexSearchModifier;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.exceptions.OccurrenceThresholdPanicException;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.search.ConjunctiveSearchModifier;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.search.InvertedIndexSearchPredicate;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizer;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexTestContext;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexTestContext.InvertedIndexType;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexTestUtils;

public abstract class AbstractInvertedIndexSearchTest extends AbstractInvertedIndexTest {

    protected int NUM_QUERIES = 10000;
    protected int[] scanCountArray = new int[NUM_DOCS_TO_INSERT];
    protected final boolean bulkLoad;

    // Probability that a randomly generated query is used, instead of a document from the corpus.
    protected final float randomQueryProb = 0.9f;

    public AbstractInvertedIndexSearchTest(InvertedIndexType invIndexType, boolean bulkLoad) {
        super(invIndexType);
        this.bulkLoad = bulkLoad;
    }

    protected void runTest(InvertedIndexTestContext testCtx, TupleGenerator tupleGen,
            IInvertedIndexSearchModifier searchModifier) throws IOException, IndexException {
        IIndex invIndex = testCtx.getIndex();
        invIndex.create();
        invIndex.activate();

        if (bulkLoad) {
            InvertedIndexTestUtils.bulkLoadInvIndex(testCtx, tupleGen, NUM_DOCS_TO_INSERT);
        } else {
            InvertedIndexTestUtils.insertIntoInvIndex(testCtx, tupleGen, NUM_DOCS_TO_INSERT);
        }
        invIndex.validate();

        IInvertedIndexAccessor accessor = (IInvertedIndexAccessor) invIndex.createAccessor(
                NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        IBinaryTokenizer tokenizer = testCtx.getTokenizerFactory().createTokenizer();
        InvertedIndexSearchPredicate searchPred = new InvertedIndexSearchPredicate(tokenizer, searchModifier);
        Random rnd = harness.getRandom();
        List<ITupleReference> documentCorpus = testCtx.getDocumentCorpus();
        // Project away the primary-key field.
        int[] fieldPermutation = new int[] { 0 };
        PermutingTupleReference searchDocument = new PermutingTupleReference(fieldPermutation);

        IIndexCursor resultCursor = accessor.createSearchCursor();
        for (int i = 0; i < NUM_QUERIES; i++) {
            if (rnd.nextFloat() <= randomQueryProb) {
                // Generate a random query.
                ITupleReference randomQuery = tupleGen.next();
                searchDocument.reset(randomQuery);
            } else {
                // Pick a random document from the corpus to use as the search query.
                int queryIndex = Math.abs(rnd.nextInt() % documentCorpus.size());
                searchDocument.reset(documentCorpus.get(queryIndex));
            }

            // DEBUG
            /*
            StringBuilder builder = new StringBuilder();
            UTF8StringPointable.toString(builder, searchDocument.getFieldData(0), searchDocument.getFieldStart(0));
            String query = builder.toString();
            
            System.out.println("QUERY: " + i + " " + query + " " + isRandom);
            if (query.equals("Patricia Mary")) {
                System.out.println("HERE WE GO, DEBUG IT!");
            }
            */

            // Set query tuple in search predicate.
            searchPred.setQueryTuple(searchDocument);
            searchPred.setQueryFieldIndex(0);

            resultCursor.reset();
            boolean panic = false;
            try {
                accessor.search(resultCursor, searchPred);
            } catch (OccurrenceThresholdPanicException e) {
                // ignore panic queries
                panic = true;
            }

            if (!panic) {
                // Consume cursor and deserialize results so we can sort them. Some search cursors may not deliver the result sorted (e.g., LSM search cursor).
                ArrayList<Integer> actualResults = new ArrayList<Integer>();
                while (resultCursor.hasNext()) {
                    resultCursor.next();
                    ITupleReference resultTuple = resultCursor.getTuple();
                    int actual = IntegerSerializerDeserializer.getInt(resultTuple.getFieldData(0),
                            resultTuple.getFieldStart(0));
                    actualResults.add(Integer.valueOf(actual));
                }
                Collections.sort(actualResults);

                // Get expected results.
                List<Integer> expectedResults = new ArrayList<Integer>();
                InvertedIndexTestUtils.getExpectedResults(scanCountArray, testCtx.getCheckTuples(), searchDocument,
                        tokenizer, testCtx.getFieldSerdes()[0], searchModifier, expectedResults);

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
        }

        invIndex.deactivate();
        invIndex.destroy();
    }

    @Test
    public void wordTokensInvIndexTest() throws IOException, IndexException {
        InvertedIndexTestContext testCtx = InvertedIndexTestUtils.createWordInvIndexTestContext(harness, invIndexType);
        TupleGenerator tupleGen = InvertedIndexTestUtils.createStringDocumentTupleGen(harness.getRandom());
        IInvertedIndexSearchModifier searchModifier = new ConjunctiveSearchModifier();
        runTest(testCtx, tupleGen, searchModifier);
    }

}
