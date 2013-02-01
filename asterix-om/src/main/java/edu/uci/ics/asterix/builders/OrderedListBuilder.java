package edu.uci.ics.asterix.builders;

import java.io.IOException;

import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IToken;

public class OrderedListBuilder extends AbstractListBuilder {

    public OrderedListBuilder() {
        super(ATypeTag.ORDEREDLIST);
    }

    public void addItem(IToken token) throws IOException {
        if (!fixedSize) {
            offsets.add((short) outputStorage.getLength());
        }
        numberOfItems++;
        token.serializeToken(outputStorage);
    }
}
