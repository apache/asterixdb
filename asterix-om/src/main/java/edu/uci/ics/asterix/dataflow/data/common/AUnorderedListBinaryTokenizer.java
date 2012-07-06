package edu.uci.ics.asterix.dataflow.data.common;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AUnorderedListSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.ITokenFactory;

public class AUnorderedListBinaryTokenizer extends AOrderedListBinaryTokenizer {

    public AUnorderedListBinaryTokenizer(ITokenFactory tokenFactory) {
        super(tokenFactory);
    }
    
    @Override
    protected int getItemOffset(byte[] data, int start, int itemIndex) throws AsterixException {
        return AUnorderedListSerializerDeserializer.getItemOffset(data, start, itemIndex);
    }

    @Override
    protected int getNumberOfItems(byte[] data, int start) {
        return AUnorderedListSerializerDeserializer.getNumberOfItems(data, start);
    }
}
