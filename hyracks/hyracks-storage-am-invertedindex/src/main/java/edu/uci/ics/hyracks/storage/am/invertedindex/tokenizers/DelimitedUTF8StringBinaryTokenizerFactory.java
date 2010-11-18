package edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers;

import edu.uci.ics.hyracks.storage.am.invertedindex.api.IBinaryTokenizer;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IBinaryTokenizerFactory;

public class DelimitedUTF8StringBinaryTokenizerFactory implements IBinaryTokenizerFactory {
	
	private static final long serialVersionUID = 1L;
	private final char delimiter;
	
	public DelimitedUTF8StringBinaryTokenizerFactory(char delimiter) {
		this.delimiter = delimiter;
	}
	
	@Override
	public IBinaryTokenizer createBinaryTokenizer() {
		return new DelimitedUTF8StringBinaryTokenizer(delimiter);
	}	
}
