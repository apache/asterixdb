package edu.uci.ics.hyracks.storage.am.lsm.inverteredindex;

import java.io.IOException;

import org.junit.Test;

import edu.uci.ics.hyracks.storage.am.common.api.IndexException;

public abstract class AbstractInvertedIndexInsertTest extends AbstractInvertedIndexTest {

    /**
     * This test inserts documents into the inverted index, builds a baseline inverted index, and
     * verifies that each token and its associated inverted list is matching in both the baseline and the
     * inverted index to be tested.
     */
    @Test
    public void insertionTest() throws IndexException, IOException {
        insertDocuments();
        buildBaselineIndex();
        verifyAgainstBaseline();
    }

}
