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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class WordTokenizer implements Tokenizer {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    public static void main(String args[]) {
        Tokenizer tokenizer = new WordTokenizer("_", '_');
        String a = "hadoop_rocks_in_java";
        System.out.println(a + ":" + tokenizer.tokenize(a));
    }

    private final String wordSeparator;
    private final char tokenSeparator;

    public WordTokenizer() {
        this(" ", '_');
    }

    public WordTokenizer(String wordSeparator, char tokenSeparator) {
        this.wordSeparator = wordSeparator;
        this.tokenSeparator = tokenSeparator;
    }

    public List<String> tokenize(String input) {
        final ArrayList<String> returnVect = new ArrayList<String>();
        final HashMap<String, Integer> tokens = new HashMap<String, Integer>();
        for (String term : input.split(wordSeparator)) {
            if (term.length() == 0) {
                continue;
            }
            Integer count = tokens.get(term);
            if (count == null) {
                count = 0;
            }
            count++;
            tokens.put(term, count);
            returnVect.add(term + tokenSeparator + count);
        }
        return returnVect;
    }
}
