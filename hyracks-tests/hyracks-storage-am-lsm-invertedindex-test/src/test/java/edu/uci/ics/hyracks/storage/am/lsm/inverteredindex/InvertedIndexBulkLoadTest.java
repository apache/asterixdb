package edu.uci.ics.hyracks.storage.am.lsm.inverteredindex;

import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.ITokenFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.NGramUTF8StringBinaryTokenizer;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.UTF8NGramTokenFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.utils.InvertedIndexTestUtils;

public class InvertedIndexBulkLoadTest extends AbstractInvertedIndexBulkloadTest {

    @Override
    protected void setTokenizer() {
        ITokenFactory tokenFactory = new UTF8NGramTokenFactory();
        tokenizer = new NGramUTF8StringBinaryTokenizer(3, false, true, false, tokenFactory);
    }

    @Override
    protected void setInvertedIndex() throws HyracksDataException {
        invertedIndex = InvertedIndexTestUtils.createTestInvertedIndex(harness, tokenizer);
        invertedIndex.create(harness.getDiskInvertedIndexFileId());
        invertedIndex.open(harness.getDiskInvertedIndexFileId());
    }

    @Override
    protected void setLogger() {
        LOGGER = Logger.getLogger(InvertedIndexBulkLoadTest.class.getName());

    }

    @Override
    protected void setRandom() {
        random = true;
    }

}
