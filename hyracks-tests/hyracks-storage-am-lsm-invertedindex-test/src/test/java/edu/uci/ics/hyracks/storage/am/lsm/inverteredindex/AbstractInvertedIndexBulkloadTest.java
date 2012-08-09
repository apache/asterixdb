package edu.uci.ics.hyracks.storage.am.lsm.inverteredindex;

import java.io.IOException;

import org.junit.Test;

import edu.uci.ics.hyracks.storage.am.common.api.IndexException;

public abstract class AbstractInvertedIndexBulkloadTest extends AbstractInvertedIndexTest {

    @Test
    public void bulkLoadTest() throws IndexException, IOException {
        bulkLoadDocuments();
        buildBaselineIndex();
        verifyAgainstBaseline();
    }
}
