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

package org.apache.asterix.fuzzyjoin.tokenorder;

import java.util.Collection;
import java.util.HashMap;

import org.apache.asterix.fuzzyjoin.tokenizer.Token;

public class TokenRankBufferedFrequency implements TokenRank {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    private final HashMap<Token, Integer> ranksMap = new HashMap<Token, Integer>();
    private int crtRank = 0;

    public int add(String stringWithCount) {
        int end = stringWithCount.lastIndexOf('_');
        int count = 0;
        for (int i = end + 1; i < stringWithCount.length(); ++i) {
            count = count * 10 + (stringWithCount.charAt(i) - '0');
        }
        return add(stringWithCount.substring(0, end), count);
    }

    public int add(String string, int count) {
        Token token = new Token(string, 0, string.length(), count);
        return add(token);
    }

    public int add(Token token) {
        int prevRank = crtRank;
        ranksMap.put(token, prevRank);
        crtRank++;
        return prevRank;
    }

    @Override
    public Integer getRank(String token) {
        throw new UnsupportedOperationException();
    }

    public Integer getRank(Token token) {
        return ranksMap.get(token);
    }

    @Override
    public Collection<Integer> getTokenRanks(Iterable<String> tokens) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return "[" + crtRank + ",\n " + ranksMap + "\n]";
    }
}
