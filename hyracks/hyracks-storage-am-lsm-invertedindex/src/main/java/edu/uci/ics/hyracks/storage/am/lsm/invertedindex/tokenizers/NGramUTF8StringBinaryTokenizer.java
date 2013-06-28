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

import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;

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
        int currentTokenStart = index;
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
            index += UTF8StringPointable.charSize(data, index);
        }

        // compute token count
        // ignore pre and post grams for duplicate detection
        if (!ignoreTokenCount && numPreChars == 0 && numPostChars == 0) {
            int tmpIndex = start;
            while (tmpIndex < currentTokenStart) {
                tokenCount++; // assume found
                int offset = 0;
                for (int j = 0; j < gramLength; j++) {
                    if (Character.toLowerCase(UTF8StringPointable.charAt(data, currentTokenStart + offset)) != Character
                            .toLowerCase(UTF8StringPointable.charAt(data, tmpIndex + offset))) {
                        tokenCount--;
                        break;
                    }
                    offset += UTF8StringPointable.charSize(data, tmpIndex + offset);
                }
                tmpIndex += UTF8StringPointable.charSize(data, tmpIndex);
            }
        }

        // set token
        token.reset(data, currentTokenStart, length, gramLength, tokenCount);
    }

    @Override
    public void reset(byte[] data, int start, int length) {
        super.reset(data, start, length);
        gramNum = 0;

        int numChars = 0;
        int pos = index;
        int end = pos + utf8Length;
        while (pos < end) {
            numChars++;
            pos += UTF8StringPointable.charSize(data, pos);
        }

        if (usePrePost) {
            totalGrams = numChars + gramLength - 1;
        } else {
            totalGrams = numChars - gramLength + 1;
        }
    }

    public void setGramlength(int gramLength) {
        this.gramLength = gramLength;
    }

    public void setPrePost(boolean usePrePost) {
        this.usePrePost = usePrePost;
    }
}
