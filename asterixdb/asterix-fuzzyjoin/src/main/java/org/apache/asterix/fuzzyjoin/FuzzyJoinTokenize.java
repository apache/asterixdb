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

package org.apache.asterix.fuzzyjoin;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;

import org.apache.asterix.fuzzyjoin.tokenizer.Tokenizer;
import org.apache.asterix.fuzzyjoin.tokenizer.TokenizerFactory;
import org.apache.asterix.fuzzyjoin.tokenorder.TokenLoad;
import org.apache.asterix.fuzzyjoin.tokenorder.TokenRank;
import org.apache.asterix.fuzzyjoin.tokenorder.TokenRankFrequency;

public class FuzzyJoinTokenize {
    public static class TokenCount implements Comparable<Object> {
        public String token;
        public MutableInteger count;

        public TokenCount(String token, MutableInteger count) {
            this.token = token;
            this.count = count;
        }

        @Override
        public int compareTo(Object o) {
            TokenCount tc = (TokenCount) o;
            return count.compareTo(tc.count);
        }

        public String getToken() {
            return token;
        }

        @Override
        public String toString() {
            return token + " " + count;
        }
    }

    public static void main(String args[]) throws IOException {
        final String inputFileName = args[0];
        final String tokensFileName = args[1];
        final String tokenizedFileName = args[2];

        BufferedReader input = new BufferedReader(new FileReader(inputFileName));

        Tokenizer tokenizer = TokenizerFactory.getTokenizer(FuzzyJoinConfig.TOKENIZER_VALUE,
                FuzzyJoinConfig.WORD_SEPARATOR_REGEX, FuzzyJoinConfig.TOKEN_SEPARATOR);

        int[] dataColumns = FuzzyJoinUtil.getDataColumns("2,3");

        String line;
        HashMap<String, MutableInteger> tokenCount = new HashMap<String, MutableInteger>();
        while ((line = input.readLine()) != null) {
            Collection<String> tokens =
                    tokenizer.tokenize(FuzzyJoinUtil.getData(line.split(FuzzyJoinConfig.RECORD_SEPARATOR_REGEX),
                            dataColumns, FuzzyJoinConfig.TOKEN_SEPARATOR));

            for (String token : tokens) {
                MutableInteger count = tokenCount.get(token);
                if (count == null) {
                    tokenCount.put(token, new MutableInteger(1));
                } else {
                    count.inc();
                }
            }
        }

        input.close();

        ArrayList<TokenCount> tokenCounts = new ArrayList<TokenCount>();
        tokenCount.forEach((key, value) -> tokenCounts.add(new TokenCount(key, value)));
        Collections.sort(tokenCounts);

        BufferedWriter outputTokens = new BufferedWriter(new FileWriter(tokensFileName));
        for (TokenCount tc : tokenCounts) {
            outputTokens.write(tc.getToken() + "\n");
        }
        outputTokens.close();

        TokenRank tokenRank = new TokenRankFrequency();
        TokenLoad tokenLoad = new TokenLoad(tokensFileName, tokenRank);
        tokenLoad.loadTokenRank();

        input = new BufferedReader(new FileReader(inputFileName));
        LittleEndianIntOutputStream outputTokenized =
                new LittleEndianIntOutputStream(new BufferedOutputStream(new FileOutputStream(tokenizedFileName)));
        while ((line = input.readLine()) != null) {
            String splits[] = line.split(FuzzyJoinConfig.RECORD_SEPARATOR_REGEX);
            int rid = Integer.parseInt(splits[FuzzyJoinConfig.RECORD_KEY]);
            outputTokenized.writeInt(rid);
            Collection<String> tokens =
                    tokenizer.tokenize(FuzzyJoinUtil.getData(splits, dataColumns, FuzzyJoinConfig.TOKEN_SEPARATOR));
            Collection<Integer> tokensRanked = tokenRank.getTokenRanks(tokens);
            outputTokenized.writeInt(tokensRanked.size());
            for (Integer token : tokensRanked) {
                outputTokenized.writeInt(token);
            }
            // for (int i = 0; i < tokens.size() - tokensRanked.size(); i++) {
            // outputTokenized.writeInt(Integer.MAX_VALUE);
            // }
        }

        input.close();
        outputTokenized.close();
    }
}
