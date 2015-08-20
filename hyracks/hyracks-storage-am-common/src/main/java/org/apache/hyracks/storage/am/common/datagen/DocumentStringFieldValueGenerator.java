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

package edu.uci.ics.hyracks.storage.am.common.datagen;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class DocumentStringFieldValueGenerator implements IFieldValueGenerator<String> {
    private final String FIRST_NAMES_FILE = "dist.all.first.cleaned";
    private final String LAST_NAMES_FILE = "dist.all.last.cleaned";

    private final int docMinWords;
    private final int docMaxWords;
    private final int maxDictionarySize;
    private final Random rnd;
    private int[] cumulIntRanges;

    private List<String> tokenDict = new ArrayList<String>();

    public DocumentStringFieldValueGenerator(int docMinWords, int docMaxWords, int maxDictionarySize, Random rnd)
            throws IOException {
        this.docMinWords = docMinWords;
        this.docMaxWords = docMaxWords;
        this.maxDictionarySize = maxDictionarySize;
        this.rnd = rnd;
        initDictionary();
        double[] zipfProbDist = ProbabilityHelper.getZipfProbDist(tokenDict.size(), 1);
        cumulIntRanges = ProbabilityHelper.getCumulIntRanges(zipfProbDist);
    }

    private void initDictionary() throws IOException {
        String line;
        int count = 0;

        // Read first names from data file.
        InputStream firstNamesIn = this.getClass().getClassLoader().getResourceAsStream(FIRST_NAMES_FILE);
        BufferedReader firstNamesReader = new BufferedReader(new InputStreamReader(firstNamesIn));
        try {
            while (count < maxDictionarySize && (line = firstNamesReader.readLine()) != null) {
                tokenDict.add(line.trim());
                count++;
            }
        } finally {
            firstNamesReader.close();
        }

        // Read last names from data file.
        InputStream lastNamesIn = this.getClass().getClassLoader().getResourceAsStream(LAST_NAMES_FILE);
        BufferedReader lastNamesReader = new BufferedReader(new InputStreamReader(lastNamesIn));
        try {
            while (count < maxDictionarySize && (line = lastNamesReader.readLine()) != null) {
                tokenDict.add(line.trim());
                count++;
            }
        } finally {
            lastNamesReader.close();
        }
    }

    @Override
    public String next() {
        StringBuilder strBuilder = new StringBuilder();
        int numWords = Math.abs(rnd.nextInt()) % (docMaxWords - docMinWords + 1) + docMinWords;
        for (int i = 0; i < numWords; i++) {
            int ix = ProbabilityHelper.choose(cumulIntRanges, rnd.nextInt());
            strBuilder.append(tokenDict.get(ix));
            if (i != numWords - 1) {
                strBuilder.append(" ");
            }
        }
        return strBuilder.toString();
    }

    public List<String> getTokenDictionary() {
        return tokenDict;
    }

    @Override
    public void reset() {
    }
}
