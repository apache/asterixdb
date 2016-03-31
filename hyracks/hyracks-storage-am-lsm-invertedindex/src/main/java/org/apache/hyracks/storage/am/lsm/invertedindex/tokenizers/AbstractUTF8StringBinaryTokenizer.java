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

import org.apache.hyracks.util.string.UTF8StringUtil;

public abstract class AbstractUTF8StringBinaryTokenizer implements IBinaryTokenizer {

    protected byte[] sentenceBytes;
    protected int sentenceStartOffset;
    protected int sentenceEndOffset;
    protected int sentenceUtf8Length;

    protected int byteIndex;

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

    //TODO: This UTF8Tokenizer strongly relies on the Asterix data format,
    // i.e. the TypeTag and the byteIndex increasing both assume the given byte[] sentence
    // is an AString object. A better way (if we want to keep the byte[] interface) would be
    // giving this tokenizer the pure UTF8 character sequence whose {@code start} is the start
    // of the first character, and move the shift offset to the caller.
    @Override
    public void reset(byte[] sentenceData, int start, int length) {
        this.sentenceBytes = sentenceData;
        this.sentenceStartOffset = start;
        this.sentenceEndOffset = length + start;

        byteIndex = this.sentenceStartOffset;
        if (sourceHasTypeTag) {
            byteIndex++; // skip type tag
        }
        sentenceUtf8Length = UTF8StringUtil.getUTFLength(sentenceData, byteIndex);
        byteIndex += UTF8StringUtil.getNumBytesToStoreLength(sentenceUtf8Length); // skip utf8 length indicator

        if (!ignoreTokenCount) {
            tokensStart.reset();
            tokensLength.reset();
        }
    }
}
