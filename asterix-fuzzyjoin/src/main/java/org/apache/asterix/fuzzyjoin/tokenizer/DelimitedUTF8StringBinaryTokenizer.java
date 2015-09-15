/**
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

package org.apache.asterix.fuzzyjoin.tokenizer;

public class DelimitedUTF8StringBinaryTokenizer extends AbstractUTF8StringBinaryTokenizer {

    public DelimitedUTF8StringBinaryTokenizer(boolean ignoreTokenCount, boolean sourceHasTypeTag,
            ITokenFactory tokenFactory) {
        super(ignoreTokenCount, sourceHasTypeTag, tokenFactory);
    }

    @Override
    public boolean hasNext() {
        // skip delimiters
        while (index < length && isSeparator(StringUtils.charAt(data, index))) {
            index += StringUtils.charSize(data, index);
        }
        return index < length;
    }

    private boolean isSeparator(char c) {
        return !(Character.isLetterOrDigit(c) || Character.getType(c) == Character.OTHER_LETTER || Character.getType(c) == Character.OTHER_NUMBER);
    }

    @Override
    public void next() {
        tokenLength = 0;
        int currentTokenStart = index;
        while (index < length && !isSeparator(StringUtils.charAt(data, index))) {
            index += StringUtils.charSize(data, index);
            tokenLength++;
        }
        int tokenCount = 1;
        if (tokenLength > 0 && !ignoreTokenCount) {
            // search if we got the same token before
            for (int i = 0; i < tokensStart.length(); ++i) {
                if (tokenLength == tokensLength.get(i)) {
                    int tokenStart = tokensStart.get(i);
                    tokenCount++; // assume we found it
                    int offset = 0;
                    int currLength = 0;
                    while (currLength < tokenLength) {
                        // case insensitive comparison
                        if (StringUtils.toLowerCase(StringUtils.charAt(data, currentTokenStart + offset)) != StringUtils
                                .toLowerCase(StringUtils.charAt(data, tokenStart + offset))) {
                            tokenCount--;
                            break;
                        }
                        offset += StringUtils.charSize(data, currentTokenStart + offset);
                        currLength++;
                    }
                }
            }
            // add the new token to the list of seen tokens
            tokensStart.add(currentTokenStart);
            tokensLength.add(tokenLength);
        }

        // set token
        token.reset(data, currentTokenStart, index, tokenLength, tokenCount);
    }
}
