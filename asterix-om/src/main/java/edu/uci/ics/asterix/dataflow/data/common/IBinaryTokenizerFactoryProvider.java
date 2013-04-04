package edu.uci.ics.asterix.dataflow.data.common;

import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;

public interface IBinaryTokenizerFactoryProvider {
    public IBinaryTokenizerFactory getWordTokenizerFactory(ATypeTag typeTag, boolean hashedTokens);

    public IBinaryTokenizerFactory getNGramTokenizerFactory(ATypeTag typeTag, int gramLength, boolean usePrePost,
            boolean hashedTokens);
}
