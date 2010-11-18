package edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers;

import edu.uci.ics.hyracks.storage.am.invertedindex.api.IBinaryTokenizer;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IBinaryTokenizerFactory;

public class HashedQGramUTF8StringBinaryTokenizerFactory implements IBinaryTokenizerFactory {
	
	private static final long serialVersionUID = 1L;
	private final int q;
	private final boolean prePost;
	
	public HashedQGramUTF8StringBinaryTokenizerFactory(int q, boolean prePost) {
		this.q = q;
		this.prePost = prePost;
	}
	
	@Override
	public IBinaryTokenizer createBinaryTokenizer() {
		return new HashedQGramUTF8StringBinaryTokenizer(q, prePost);
	}
}
