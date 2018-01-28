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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;

import org.apache.asterix.fuzzyjoin.tokenizer.Tokenizer;
import org.apache.asterix.fuzzyjoin.tokenizer.TokenizerFactory;

public class FuzzyJoinAppendLength {
    public static void main(String args[]) throws IOException {
        final String inputFileName = args[0];
        final String outputFileName = args[1];

        BufferedReader input = new BufferedReader(new FileReader(inputFileName));
        BufferedWriter output = new BufferedWriter(new FileWriter(outputFileName));

        Tokenizer tokenizer = TokenizerFactory.getTokenizer(FuzzyJoinConfig.TOKENIZER_VALUE,
                FuzzyJoinConfig.WORD_SEPARATOR_REGEX, FuzzyJoinConfig.TOKEN_SEPARATOR);

        int[] dataColumns = FuzzyJoinUtil.getDataColumns("2,3");

        String line;
        while ((line = input.readLine()) != null) {
            String[] splits = line.split(FuzzyJoinConfig.RECORD_SEPARATOR_REGEX);
            Collection<String> tokens =
                    tokenizer.tokenize(FuzzyJoinUtil.getData(splits, dataColumns, FuzzyJoinConfig.TOKEN_SEPARATOR));
            output.write(splits[0] + FuzzyJoinConfig.RECORD_SEPARATOR + splits[1] + FuzzyJoinConfig.RECORD_SEPARATOR
                    + splits[2] + FuzzyJoinConfig.RECORD_SEPARATOR + splits[3] + FuzzyJoinConfig.RECORD_SEPARATOR
                    + tokens.size() + "\n");
        }

        input.close();
        output.close();
    }
}
