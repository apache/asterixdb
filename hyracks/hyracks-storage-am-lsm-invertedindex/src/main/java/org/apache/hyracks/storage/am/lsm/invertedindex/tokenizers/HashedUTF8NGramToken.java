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
        int pos = startOffset;
        for (int i = 0; i < numRegGrams; i++) {
            hash ^= Character.toLowerCase(UTF8StringUtil.charAt(data, pos));
            hash *= GOLDEN_RATIO_32;
            pos += UTF8StringUtil.charSize(data, pos);
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
