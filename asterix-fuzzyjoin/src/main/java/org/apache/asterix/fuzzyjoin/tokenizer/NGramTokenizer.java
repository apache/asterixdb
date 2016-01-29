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

public class NGramTokenizer implements Tokenizer {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    public static void main(String args[]) {
        Tokenizer tokenizer = new NGramTokenizer();
        String a = "hadoopoop";
        System.out.println(a + ":" + tokenizer.tokenize(a));
    }

    private final int gramLength;

    /**
     * padding used in q gram calculation.
     */
    private final char QGRAMENDPADDING = '$';

    /**
     * padding used in q gram calculation.
     */
    private final char QGRAMSTARTPADDING = '$';

    public NGramTokenizer() {
        gramLength = 3;
    }

    public NGramTokenizer(int gramLength) {
        this.gramLength = gramLength;
    }

    private StringBuffer getAdjustedString(String input) {
        final StringBuffer adjustedString = new StringBuffer();
        for (int i = 0; i < gramLength - 1; i++) {
            adjustedString.append(QGRAMSTARTPADDING);
        }
        adjustedString.append(input);
        for (int i = 0; i < gramLength - 1; i++) {
            adjustedString.append(QGRAMENDPADDING);
        }
        return adjustedString;
    }

    public List<String> tokenize(String input) {
        final ArrayList<String> returnVect = new ArrayList<String>();
        final StringBuffer adjustedString = getAdjustedString(input);
        int curPos = 0;
        final int length = adjustedString.length() - (gramLength - 1);
        final HashMap<String, Integer> grams = new HashMap<String, Integer>();
        while (curPos < length) {
            final String term = adjustedString.substring(curPos, curPos + gramLength);
            Integer count = grams.get(term);
            if (count == null) {
                count = new Integer(0);
            }
            count++;
            grams.put(term, count);
            returnVect.add(term + count);
            curPos++;
        }
        return returnVect;
    }
}
