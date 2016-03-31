/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers;

import java.io.IOException;

import org.apache.hyracks.data.std.util.GrowableArray;
import org.apache.hyracks.util.string.UTF8StringUtil;

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
            if (UTF8StringUtil.charAt(t.getData(), t.getStartOffset() + offset) != UTF8StringUtil.charAt(data,
                    startOffset + offset)) {
                return false;
            }
            offset += UTF8StringUtil.charSize(data, startOffset + offset);
        }
        return true;
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public void reset(byte[] data, int startOffset, int endOffset, int tokenLength, int tokenCount) {
        super.reset(data, startOffset, endOffset, tokenLength, tokenCount);

        // pre-compute hash value using JAQL-like string hashing
        int pos = startOffset;
        hash = GOLDEN_RATIO_32;
        for (int i = 0; i < tokenLength; i++) {
            hash ^= Character.toLowerCase(UTF8StringUtil.charAt(data, pos));
            hash *= GOLDEN_RATIO_32;
            pos += UTF8StringUtil.charSize(data, pos);
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
