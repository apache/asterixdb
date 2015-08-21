/**
 * Copyright 2010-2011 The Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on
 * an "AS IS"; BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * Author: Alexander Behm <abehm (at) ics.uci.edu>
 */

package edu.uci.ics.asterix.fuzzyjoin.tokenizer;

import edu.uci.ics.asterix.fuzzyjoin.IntArray;

public abstract class AbstractUTF8StringBinaryTokenizer implements IBinaryTokenizer {

    protected byte[] data;
    protected int start;
    protected int length;
    protected int tokenLength;
    protected int index;
    protected int utf8Length;

    protected final IntArray tokensStart;
    protected final IntArray tokensLength;
    protected final IToken token;

    protected final boolean ignoreTokenCount;
    protected final boolean sourceHasTypeTag;

    public AbstractUTF8StringBinaryTokenizer(boolean ignoreTokenCount, boolean sourceHasTypeTag,
            ITokenFactory tokenFactory) {
        this.ignoreTokenCount = ignoreTokenCount;
        this.sourceHasTypeTag = sourceHasTypeTag;
        if (!ignoreTokenCount) {
            tokensStart = new IntArray();
            tokensLength = new IntArray();
        } else {
            tokensStart = null;
            tokensLength = null;
        }
        token = tokenFactory.createToken();
    }

    @Override
    public IToken getToken() {
        return token;
    }

    @Override
    public void reset(byte[] data, int start, int length) {
        this.start = start;
        index = this.start;
        if (sourceHasTypeTag) {
            index++; // skip type tag
        }
        utf8Length = StringUtils.getUTFLen(data, index);
        index += 2; // skip utf8 length indicator
        this.data = data;
        this.length = length + start;

        tokenLength = 0;
        if (!ignoreTokenCount) {
            tokensStart.reset();
            tokensLength.reset();
        }
    }
}
