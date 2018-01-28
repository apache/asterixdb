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

import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.TokenizerInfo.TokenizerType;
import org.apache.hyracks.util.string.UTF8StringUtil;

public class NGramUTF8StringBinaryTokenizer extends AbstractUTF8StringBinaryTokenizer {

    private int gramLength;
    private boolean usePrePost;

    private int gramNum;
    private int totalGrams;

    private final INGramToken concreteToken;

    public NGramUTF8StringBinaryTokenizer(int gramLength, boolean usePrePost, boolean ignoreTokenCount,
            boolean sourceHasTypeTag, ITokenFactory tokenFactory) {
        super(ignoreTokenCount, sourceHasTypeTag, tokenFactory);
        this.gramLength = gramLength;
        this.usePrePost = usePrePost;
        concreteToken = (INGramToken) token;
    }

    @Override
    public boolean hasNext() {
        if (gramNum < totalGrams) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void next() {
        int currentTokenStart = byteIndex;
        int tokenCount = 1;
        int numPreChars = 0;
        int numPostChars = 0;
        if (usePrePost) {
            numPreChars = Math.max(gramLength - gramNum - 1, 0);
            numPostChars = (gramNum > totalGrams - gramLength) ? gramLength - totalGrams + gramNum : 0;
        }
        gramNum++;

        concreteToken.setNumPrePostChars(numPreChars, numPostChars);
        if (numPreChars == 0) {
            byteIndex += UTF8StringUtil.charSize(sentenceBytes, byteIndex);
        }

        // compute token count
        // ignore pre and post grams for duplicate detection
        if (!ignoreTokenCount && numPreChars == 0 && numPostChars == 0) {
            int tmpIndex = sentenceStartOffset;
            if (sourceHasTypeTag) {
                tmpIndex++; // skip type tag
            }
            int utfLength = UTF8StringUtil.getUTFLength(sentenceBytes, tmpIndex);
            tmpIndex += UTF8StringUtil.getNumBytesToStoreLength(utfLength); // skip utf8 length indicator
            while (tmpIndex < currentTokenStart) {
                tokenCount++; // assume found
                int offset = 0;
                for (int j = 0; j < gramLength; j++) {
                    if (Character
                            .toLowerCase(UTF8StringUtil.charAt(sentenceBytes, currentTokenStart + offset)) != Character
                                    .toLowerCase(UTF8StringUtil.charAt(sentenceBytes, tmpIndex + offset))) {
                        tokenCount--;
                        break;
                    }
                    offset += UTF8StringUtil.charSize(sentenceBytes, tmpIndex + offset);
                }
                tmpIndex += UTF8StringUtil.charSize(sentenceBytes, tmpIndex);
            }
        }

        // set token
        token.reset(sentenceBytes, currentTokenStart, sentenceEndOffset, gramLength, tokenCount);
    }

    @Override
    public void reset(byte[] sentenceData, int start, int length) {
        super.reset(sentenceData, start, length);
        gramNum = 0;

        int numChars = 0;
        int pos = byteIndex;
        int end = pos + sentenceUtf8Length;
        while (pos < end) {
            numChars++;
            pos += UTF8StringUtil.charSize(sentenceData, pos);
        }

        if (usePrePost) {
            totalGrams = numChars + gramLength - 1;
        } else {
            if (numChars >= gramLength) {
                totalGrams = numChars - gramLength + 1;
            } else {
                totalGrams = 0;
            }
        }
    }

    public void setGramlength(int gramLength) {
        this.gramLength = gramLength;
    }

    public void setPrePost(boolean usePrePost) {
        this.usePrePost = usePrePost;
    }

    @Override
    public short getTokensCount() {
        return (short) totalGrams;
    }

    @Override
    public TokenizerType getTokenizerType() {
        return TokenizerType.STRING;
    }
}
