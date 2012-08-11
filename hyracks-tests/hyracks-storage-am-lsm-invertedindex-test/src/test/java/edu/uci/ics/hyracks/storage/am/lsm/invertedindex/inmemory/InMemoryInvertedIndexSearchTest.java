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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.inmemory;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Test;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.AbstractInvertedIndexTest;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexSearchModifier;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.search.ConjunctiveSearchModifier;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.search.EditDistanceSearchModifier;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.search.JaccardSearchModifier;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.ITokenFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.NGramUTF8StringBinaryTokenizer;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.UTF8NGramTokenFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexTestUtils;

public class InMemoryInvertedIndexSearchTest extends AbstractInvertedIndexTest {

    /**
     * Runs 5 random conjunctive search queries to test the
     * ConjunctiveSearchModifier.
     */
    @Test
    public void conjunctiveQueryTest() throws Exception {
        insertDocuments();
        IInvertedIndexSearchModifier searchModifier = new ConjunctiveSearchModifier();
        runQueries(searchModifier, 5);
    }

    /**
     * Runs 5 random jaccard-based search queries with thresholds 0.9, 0.8, 0.7.
     * Tests the JaccardSearchModifier.
     */
    @Test
    public void jaccardQueryTest() throws Exception {
        insertDocuments();
        JaccardSearchModifier searchModifier = new JaccardSearchModifier(1.0f);

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("JACCARD: " + 0.9f);
        }
        searchModifier.setJaccThresh(0.9f);
        runQueries(searchModifier, 5);

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("JACCARD: " + 0.8f);
        }
        searchModifier.setJaccThresh(0.8f);
        runQueries(searchModifier, 5);

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("JACCARD: " + 0.7f);
        }
        searchModifier.setJaccThresh(0.7f);
        runQueries(searchModifier, 5);
    }

    /**
     * Runs 5 random edit-distance based search queries with thresholds 1, 2, 3.
     * Tests the EditDistanceSearchModifier.
     */
    @Test
    public void editDistanceQueryTest() throws Exception {
        insertDocuments();
        EditDistanceSearchModifier searchModifier = new EditDistanceSearchModifier(3, 0);

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("EDIT DISTANCE: " + 1);
        }
        searchModifier.setEdThresh(1);
        runQueries(searchModifier, 5);

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("EDIT DISTANCE: " + 2);
        }
        searchModifier.setEdThresh(2);
        runQueries(searchModifier, 5);

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("EDIT DISTANCE: " + 3);
        }
        searchModifier.setEdThresh(3);
        runQueries(searchModifier, 5);
    }
    
    @Override
    protected void setTokenizer() {
        ITokenFactory tokenFactory = new UTF8NGramTokenFactory();
        tokenizer = new NGramUTF8StringBinaryTokenizer(3, false, true, false, tokenFactory);
    }

    @Override
    protected void setInvertedIndex() throws HyracksDataException {
        invertedIndex = InvertedIndexTestUtils.createInMemoryInvertedIndex(harness, tokenizer);
        invertedIndex.create(harness.getFileId());
        invertedIndex.open(harness.getFileId());
    }

    @Override
    protected void setLogger() {
        LOGGER = Logger.getLogger(InMemoryInvertedIndexSearchTest.class.getName());
    }

    @Override
    protected void setRandom() {
        random = false;
    }

}
