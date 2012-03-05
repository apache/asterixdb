package edu.uci.ics.asterix.dataflow.data.common;

import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IBinaryTokenizerFactory;

public interface IBinaryTokenizerFactoryProvider {
    public IBinaryTokenizerFactory getTokenizerFactory(Object type);
}
