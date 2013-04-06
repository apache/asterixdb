package edu.uci.ics.asterix.dataflow.data.common;

import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IToken;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.ITokenFactory;

public class AListElementTokenFactory implements ITokenFactory {

    private static final long serialVersionUID = 1L;

    @Override
    public IToken createToken() {
        return new AListElementToken();
    }
}
