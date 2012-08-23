package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndex;
import edu.uci.ics.hyracks.storage.am.common.datagen.TupleGenerator;
import edu.uci.ics.hyracks.storage.am.config.AccessMethodTestsConfig;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexTestContext;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexTestUtils;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexTestContext.InvertedIndexType;

public abstract class AbstractInvertedIndexDeleteTest extends AbstractInvertedIndexTest {
    
    protected final int numInsertRounds = AccessMethodTestsConfig.LSM_INVINDEX_NUM_INSERT_ROUNDS;
    protected final int numDeleteRounds = AccessMethodTestsConfig.LSM_INVINDEX_NUM_INSERT_ROUNDS;
    protected final boolean bulkLoad;
    
    public AbstractInvertedIndexDeleteTest(InvertedIndexType invIndexType, boolean bulkLoad) {
        super(invIndexType);
        this.bulkLoad = bulkLoad;
    }

    protected void runTest(InvertedIndexTestContext testCtx, TupleGenerator tupleGen) throws IOException,
            IndexException {
        IIndex invIndex = testCtx.getIndex();        
        invIndex.create();
        invIndex.activate();
        
        for (int i = 0; i < numInsertRounds; i++) {
            if (bulkLoad) {
                InvertedIndexTestUtils.bulkLoadInvIndex(testCtx, tupleGen, NUM_DOCS_TO_INSERT);
            } else {
                InvertedIndexTestUtils.insertIntoInvIndex(testCtx, tupleGen, NUM_DOCS_TO_INSERT);
            }
            validateAndCheckIndex(testCtx);

            List<ITupleReference> documentCorpus = new ArrayList<ITupleReference>(); 
            documentCorpus.addAll(testCtx.getDocumentCorpus());
            
            // Delete all documents in a couple of rounds.
            int numTuplesPerDeleteRound = (int) Math.ceil((float) testCtx.getDocumentCorpus().size() / (float) numDeleteRounds);
            for (int j = 0; j < numDeleteRounds; j++) {
                InvertedIndexTestUtils.deleteFromInvIndex(testCtx, harness.getRandom(), numTuplesPerDeleteRound);
                validateAndCheckIndex(testCtx);
            }                        
        }
        
        invIndex.deactivate();
        invIndex.destroy();
    }
    
    @Test
    public void wordTokensInvIndexTest() throws IOException, IndexException {
        InvertedIndexTestContext testCtx = InvertedIndexTestUtils.createWordInvIndexTestContext(harness, invIndexType);
        TupleGenerator tupleGen = InvertedIndexTestUtils.createStringDocumentTupleGen(harness.getRandom());
        runTest(testCtx, tupleGen);
    }
}
