package edu.uci.ics.hyracks.storage.am.invertedindex.api;

import java.io.Serializable;

public interface IBinaryTokenizerFactory extends Serializable {
	public IBinaryTokenizer createBinaryTokenizer();
}
