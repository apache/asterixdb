package edu.uci.ics.asterix.formats.nontagged;

import edu.uci.ics.asterix.dataflow.data.common.IBinaryTokenizerFactoryProvider;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.DelimitedUTF8StringBinaryTokenizerFactory;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IBinaryTokenizerFactory;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.UTF8WordTokenFactory;

public class AqlBinaryTokenizerFactoryProvider implements IBinaryTokenizerFactoryProvider {

    public static final AqlBinaryTokenizerFactoryProvider INSTANCE = new AqlBinaryTokenizerFactoryProvider();

    private static final IBinaryTokenizerFactory aqlStringTokenizer = new DelimitedUTF8StringBinaryTokenizerFactory(
            true, true, new UTF8WordTokenFactory());

    @Override
    public IBinaryTokenizerFactory getTokenizerFactory(Object type) {
        IAType aqlType = (IAType) type;
        switch (aqlType.getTypeTag()) {
            case STRING: {
                return aqlStringTokenizer;
            }

            default: {
                return null;
            }
        }
    }

}
