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

import java.io.DataOutput;
import java.io.IOException;

public class UTF8NGramToken extends AbstractUTF8Token implements INGramToken {

    public final static char PRECHAR = '#';

    public final static char POSTCHAR = '$';

    protected int numPreChars;
    protected int numPostChars;

    public UTF8NGramToken(byte tokenTypeTag, byte countTypeTag) {
        super(tokenTypeTag, countTypeTag);
    }

    @Override
    public int getNumPostChars() {
        return numPreChars;
    }

    @Override
    public int getNumPreChars() {
        return numPostChars;
    }

    @Override
    public void serializeToken(DataOutput dos) throws IOException {
        handleTokenTypeTag(dos);

        // regular chars
        int numRegChars = tokenLength - numPreChars - numPostChars;

        // assuming pre and post char need 1-byte each in utf8
        int tokenUTF8Len = getLowerCaseUTF8Len(numRegChars) + numPreChars + numPostChars;

        // write utf8 length indicator
        StringUtils.writeUTF8Len(tokenUTF8Len, dos);

        // pre chars
        for (int i = 0; i < numPreChars; i++) {
            StringUtils.writeCharAsModifiedUTF8(PRECHAR, dos);
        }

        int pos = start;
        for (int i = 0; i < numRegChars; i++) {
            char c = StringUtils.toLowerCase(StringUtils.charAt(data, pos));
            StringUtils.writeCharAsModifiedUTF8(c, dos);
            pos += StringUtils.charSize(data, pos);
        }

        // post chars
        for (int i = 0; i < numPostChars; i++) {
            StringUtils.writeCharAsModifiedUTF8(POSTCHAR, dos);
        }
    }

    public void setNumPrePostChars(int numPreChars, int numPostChars) {
        this.numPreChars = numPreChars;
        this.numPostChars = numPostChars;
    }
}
