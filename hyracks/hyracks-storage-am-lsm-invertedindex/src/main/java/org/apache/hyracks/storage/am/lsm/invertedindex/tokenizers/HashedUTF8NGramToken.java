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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers;

import java.io.IOException;

import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.data.std.util.GrowableArray;

public class HashedUTF8NGramToken extends UTF8NGramToken {
    public HashedUTF8NGramToken(byte tokenTypeTag, byte countTypeTag) {
        super(tokenTypeTag, countTypeTag);
    }

    @Override
    public void serializeToken(GrowableArray out) throws IOException {
        handleTokenTypeTag(out.getDataOutput());

        int hash = GOLDEN_RATIO_32;

        // pre chars
        for (int i = 0; i < numPreChars; i++) {
            hash ^= PRECHAR;
            hash *= GOLDEN_RATIO_32;
        }

        // regular chars
        int numRegGrams = tokenLength - numPreChars - numPostChars;
        int pos = start;
        for (int i = 0; i < numRegGrams; i++) {
            hash ^= Character.toLowerCase(UTF8StringPointable.charAt(data, pos));
            hash *= GOLDEN_RATIO_32;
            pos += UTF8StringPointable.charSize(data, pos);
        }

        // post chars
        for (int i = 0; i < numPostChars; i++) {
            hash ^= POSTCHAR;
            hash *= GOLDEN_RATIO_32;
        }

        // token count
        hash += tokenCount;

        out.getDataOutput().writeInt(hash);
    }
}
