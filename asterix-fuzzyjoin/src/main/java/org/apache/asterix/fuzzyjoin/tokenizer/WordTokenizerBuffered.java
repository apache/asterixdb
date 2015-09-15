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

import org.apache.asterix.fuzzyjoin.IntArray;

public class WordTokenizerBuffered implements TokenizerBuffered {

    private final StringBuilder buffer;
    private int index;
    private final Token token;

    private final IntArray tokensStart, tokensLength;

    public WordTokenizerBuffered(StringBuilder buffer) {
        this.buffer = buffer;
        token = new Token();
        tokensStart = new IntArray();
        tokensLength = new IntArray();
        reset();
    }

    @Override
    public void advance() {
        while (index < buffer.length() && TokenizerBufferedFactory.isSeparator(buffer.charAt(index))) {
            index++;
        }
        int start = index;
        while (index < buffer.length() && !TokenizerBufferedFactory.isSeparator(buffer.charAt(index))) {
            buffer.setCharAt(index, Character.toLowerCase(buffer.charAt(index)));
            index++;
        }
        int length = index - start;
        int count = 1;
        if (length > 0) {
            // search if we got the same token before
            for (int i = 0; i < tokensStart.length(); ++i) {
                if (length == tokensLength.get(i)) {
                    int tokenStart = tokensStart.get(i);
                    count++; // assume we found it
                    for (int j = 0; j < length; ++j) {
                        if (buffer.charAt(start + j) != buffer.charAt(tokenStart + j)) {
                            count--; // token not found
                            break;
                        }
                    }
                }
            }
            // add the new token to the list of seen tokens
            tokensStart.add(start);
            tokensLength.add(length);
        }
        // set token
        token.set(buffer, start, length, count);
    }

    @Override
    public boolean end() {
        return token.length() <= 0;
    }

    @Override
    public Token getToken() {
        return token;
    }

    @Override
    public void reset() {
        index = 0;
        tokensStart.reset();
        tokensLength.reset();
        advance();
    }

}
