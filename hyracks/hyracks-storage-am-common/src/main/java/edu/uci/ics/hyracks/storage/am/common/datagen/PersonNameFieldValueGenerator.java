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

public class PersonNameFieldValueGenerator implements IFieldValueGenerator<String> {
    private final String FIRST_NAMES_FILE = "dist.all.first.cleaned";
    private final String LAST_NAMES_FILE = "dist.all.last.cleaned";

    private final Random rnd;
    private final double middleInitialProb;
    private final String letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    private List<String> firstNames = new ArrayList<String>();
    private List<String> lastNames = new ArrayList<String>();

    public PersonNameFieldValueGenerator(Random rnd, double middleInitialProb)
            throws IOException {
        this.rnd = rnd;
        this.middleInitialProb = middleInitialProb;
        initNames();
    }

    private void initNames() throws IOException {
        String line;

        // Read first names from data file.
        InputStream firstNamesIn = this.getClass().getClassLoader().getResourceAsStream(FIRST_NAMES_FILE);
        BufferedReader firstNamesReader = new BufferedReader(new InputStreamReader(firstNamesIn));
        try {
            while ((line = firstNamesReader.readLine()) != null) {
                firstNames.add(line.trim());
            }
        } finally {
            firstNamesReader.close();
        }

        // Read last names from data file.
        InputStream lastNamesIn = this.getClass().getClassLoader().getResourceAsStream(LAST_NAMES_FILE);
        BufferedReader lastNamesReader = new BufferedReader(new InputStreamReader(lastNamesIn));
        try {
            while ((line = lastNamesReader.readLine()) != null) {
                lastNames.add(line.trim());
            }
        } finally {
            lastNamesReader.close();
        }
    }

    @Override
    public String next() {
        StringBuilder strBuilder = new StringBuilder();

        // First name.
        int fix = Math.abs(rnd.nextInt()) % firstNames.size();
        strBuilder.append(firstNames.get(fix));
        strBuilder.append(" ");
        
        // Optional middle initial.
        double d = Math.abs(rnd.nextDouble());
        if (d <= middleInitialProb) {
            int mix = Math.abs(rnd.nextInt()) % letters.length();
            strBuilder.append(letters.charAt(mix));
            strBuilder.append(". ");
        }
        
        // Last name.
        int lix = Math.abs(rnd.nextInt()) % lastNames.size();
        strBuilder.append(lastNames.get(lix));
        
        return strBuilder.toString();
    }

    @Override
    public void reset() {
    }
}
