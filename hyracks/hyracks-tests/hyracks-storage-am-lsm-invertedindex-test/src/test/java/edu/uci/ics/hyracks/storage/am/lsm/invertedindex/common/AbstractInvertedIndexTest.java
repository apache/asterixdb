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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.common;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.Before;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.storage.am.common.api.IIndex;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.datagen.TupleGenerator;
import edu.uci.ics.hyracks.storage.am.config.AccessMethodTestsConfig;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexSearchModifier;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.search.ConjunctiveSearchModifier;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.search.JaccardSearchModifier;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util.LSMInvertedIndexTestContext;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util.LSMInvertedIndexTestContext.InvertedIndexType;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util.LSMInvertedIndexTestUtils;

public abstract class AbstractInvertedIndexTest {
    protected final Logger LOGGER = Logger.getLogger(AbstractInvertedIndexTest.class.getName());

    protected final LSMInvertedIndexTestHarness harness = new LSMInvertedIndexTestHarness();

    protected final int NUM_DOCS_TO_INSERT = AccessMethodTestsConfig.LSM_INVINDEX_NUM_DOCS_TO_INSERT;
    protected final int[] SCAN_COUNT_ARRAY = new int[AccessMethodTestsConfig.LSM_INVINDEX_SCAN_COUNT_ARRAY_SIZE];

    protected final int TINY_WORKLOAD_NUM_DOC_QUERIES = AccessMethodTestsConfig.LSM_INVINDEX_TINY_NUM_DOC_QUERIES;
    protected final int TINY_WORKLOAD_NUM_RANDOM_QUERIES = AccessMethodTestsConfig.LSM_INVINDEX_TINY_NUM_RANDOM_QUERIES;

    // Note: The edit-distance search modifier is tested separately.
    protected final IInvertedIndexSearchModifier[] TEST_SEARCH_MODIFIERS = new IInvertedIndexSearchModifier[] {
            new ConjunctiveSearchModifier(), new JaccardSearchModifier(0.8f), new JaccardSearchModifier(0.5f) };

    protected final InvertedIndexType invIndexType;

    public AbstractInvertedIndexTest(InvertedIndexType invIndexType) {
        this.invIndexType = invIndexType;
    }

    @Before
    public void setUp() throws HyracksException {
        harness.setUp();
    }

    @After
    public void tearDown() throws HyracksDataException {
        harness.tearDown();
    }

    /**
     * Validates the index, and compares it against the expected index.
     * This test is only for verifying the integrity and correctness of the index,
     * it does not ensure the correctness of index searches.
     */
    protected void validateAndCheckIndex(LSMInvertedIndexTestContext testCtx) throws HyracksDataException, IndexException {
        IIndex invIndex = testCtx.getIndex();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Validating index: " + invIndex);
        }
        // Validate index and compare against expected index.
        invIndex.validate();
        if (invIndexType == InvertedIndexType.INMEMORY || invIndexType == InvertedIndexType.ONDISK) {
            // This comparison method exercises different features of these types of inverted indexes.
            LSMInvertedIndexTestUtils.compareActualAndExpectedIndexes(testCtx);
        }
        LSMInvertedIndexTestUtils.compareActualAndExpectedIndexesRangeSearch(testCtx);
    }

    /**
     * Runs a workload of queries using different search modifiers, and verifies the correctness of the results.
     */
    protected void runTinySearchWorkload(LSMInvertedIndexTestContext testCtx, TupleGenerator tupleGen) throws IOException,
            IndexException {
        for (IInvertedIndexSearchModifier searchModifier : TEST_SEARCH_MODIFIERS) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Running test workload with: " + searchModifier.toString());
            }
            LSMInvertedIndexTestUtils.testIndexSearch(testCtx, tupleGen, harness.getRandom(),
                    TINY_WORKLOAD_NUM_DOC_QUERIES, TINY_WORKLOAD_NUM_RANDOM_QUERIES, searchModifier, SCAN_COUNT_ARRAY);
        }
    }
}
