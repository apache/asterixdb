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

public class DelimitedUTF8StringBinaryTokenizer extends AbstractUTF8StringBinaryTokenizer {

    protected short tokenCount;
    private boolean tokenCountCalculated;
    private int originalIndex;

    public DelimitedUTF8StringBinaryTokenizer(boolean ignoreTokenCount, boolean sourceHasTypeTag,
            ITokenFactory tokenFactory) {
        super(ignoreTokenCount, sourceHasTypeTag, tokenFactory);
    }

    @Override
    public void reset(byte[] sentenceData, int start, int length) {
        super.reset(sentenceData, start, length);
        // Needed for calculating the number of tokens
        tokenCount = 0;
        tokenCountCalculated = false;
        originalIndex = byteIndex;
    }

    @Override
    public boolean hasNext() {
        // skip delimiters
        while (byteIndex < sentenceEndOffset && isSeparator(UTF8StringUtil.charAt(sentenceBytes, byteIndex))) {
            byteIndex += UTF8StringUtil.charSize(sentenceBytes, byteIndex);
        }
        return byteIndex < sentenceEndOffset;
    }

    public static boolean isSeparator(char c) {
        return !(Character.isLetterOrDigit(c) || Character.getType(c) == Character.OTHER_LETTER
                || Character.getType(c) == Character.OTHER_NUMBER);
    }

    @Override
    public void next() {
        int tokenLength = 0;
        int currentTokenStart = byteIndex;
        while (byteIndex < sentenceEndOffset && !isSeparator(UTF8StringUtil.charAt(sentenceBytes, byteIndex))) {
            byteIndex += UTF8StringUtil.charSize(sentenceBytes, byteIndex);
            tokenLength++;
        }
        int curTokenCount = 1;
        if (tokenLength > 0 && !ignoreTokenCount) {
            // search if we got the same token before
            for (int i = 0; i < tokensStart.length(); ++i) {
                if (tokenLength == tokensLength.get(i)) {
                    int tokenStart = tokensStart.get(i);
                    curTokenCount++; // assume we found it
                    int offset = 0;
                    for (int charPos = 0; charPos < tokenLength; charPos++) {
                        // case insensitive comparison
                        if (Character.toLowerCase(
                                UTF8StringUtil.charAt(sentenceBytes, currentTokenStart + offset)) != Character
                                        .toLowerCase(UTF8StringUtil.charAt(sentenceBytes, tokenStart + offset))) {
                            curTokenCount--;
                            break;
                        }
                        offset += UTF8StringUtil.charSize(sentenceBytes, currentTokenStart + offset);
                    }
                }
            }
            // add the new token to the list of seen tokens
            tokensStart.add(currentTokenStart);
            tokensLength.add(tokenLength);
        }

        // set token
        token.reset(sentenceBytes, currentTokenStart, byteIndex, tokenLength, curTokenCount);
        tokenCount++;
    }

    // TODO Why we bother to get the tokenCount in advance? It seems a caller's problem.
    @Override
    public short getTokensCount() {
        if (!tokenCountCalculated) {
            tokenCount = 0;
            boolean previousCharIsSeparator = true;
            while (originalIndex < sentenceEndOffset) {
                if (isSeparator(UTF8StringUtil.charAt(sentenceBytes, originalIndex))) {
                    previousCharIsSeparator = true;
                } else {
                    if (previousCharIsSeparator) {
                        tokenCount++;
                        previousCharIsSeparator = false;
                    }
                }
                originalIndex += UTF8StringUtil.charSize(sentenceBytes, originalIndex);
            }
        }
        return tokenCount;
    }

    @Override
    public TokenizerType getTokenizerType() {
        return TokenizerType.STRING;
    }
}
