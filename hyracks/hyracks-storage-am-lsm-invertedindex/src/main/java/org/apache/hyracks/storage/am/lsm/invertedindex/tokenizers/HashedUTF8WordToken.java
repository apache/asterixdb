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

public class HashedUTF8WordToken extends UTF8WordToken {

    private int hash = 0;

    public HashedUTF8WordToken(byte tokenTypeTag, byte countTypeTag) {
        super(tokenTypeTag, countTypeTag);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (!(o instanceof IToken)) {
            return false;
        }
        IToken t = (IToken) o;
        if (t.getTokenLength() != tokenLength) {
            return false;
        }
        int offset = 0;
        for (int i = 0; i < tokenLength; i++) {
            if (UTF8StringPointable.charAt(t.getData(), t.getStart() + offset) != UTF8StringPointable.charAt(data,
                    start + offset)) {
                return false;
            }
            offset += UTF8StringPointable.charSize(data, start + offset);
        }
        return true;
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public void reset(byte[] data, int start, int length, int tokenLength, int tokenCount) {
        super.reset(data, start, length, tokenLength, tokenCount);

        // pre-compute hash value using JAQL-like string hashing
        int pos = start;
        hash = GOLDEN_RATIO_32;
        for (int i = 0; i < tokenLength; i++) {
            hash ^= Character.toLowerCase(UTF8StringPointable.charAt(data, pos));
            hash *= GOLDEN_RATIO_32;
            pos += UTF8StringPointable.charSize(data, pos);
        }
        hash += tokenCount;
    }

    @Override
    public void serializeToken(GrowableArray out) throws IOException {
        if (tokenTypeTag > 0) {
            out.getDataOutput().write(tokenTypeTag);
        }

        // serialize hash value
        out.getDataOutput().writeInt(hash);
    }
}
