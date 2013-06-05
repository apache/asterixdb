/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.asterix.dataflow.data.common;

import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizer;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.ITokenFactory;

public class AOrderedListBinaryTokenizerFactory implements IBinaryTokenizerFactory {

    private static final long serialVersionUID = 1L;
    private final ITokenFactory tokenFactory;

    public AOrderedListBinaryTokenizerFactory(ITokenFactory tokenFactory) {
        this.tokenFactory = tokenFactory;
    }

    @Override
    public IBinaryTokenizer createTokenizer() {
        return new AOrderedListBinaryTokenizer(tokenFactory);
    }
}
