/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
