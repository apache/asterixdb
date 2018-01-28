/*
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

package org.apache.hyracks.storage.am.common.datagen;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hyracks.util.MathUtil;

public class PersonNameFieldValueGenerator implements IFieldValueGenerator<String> {
    private static final String FIRST_NAMES_FILE = "dist.all.first.cleaned";
    private static final String LAST_NAMES_FILE = "dist.all.last.cleaned";
    private static final String LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    private final Random rnd;
    private final double middleInitialProb;

    private List<String> firstNames = new ArrayList<>();
    private List<String> lastNames = new ArrayList<>();

    public PersonNameFieldValueGenerator(Random rnd, double middleInitialProb) throws IOException {
        this.rnd = rnd;
        this.middleInitialProb = middleInitialProb;
        initNames();
    }

    private void initNames() throws IOException {
        String line;

        // Read first names from data file.
        InputStream firstNamesIn = this.getClass().getClassLoader().getResourceAsStream(FIRST_NAMES_FILE);
        try (BufferedReader firstNamesReader = new BufferedReader(new InputStreamReader(firstNamesIn))) {
            while ((line = firstNamesReader.readLine()) != null) {
                if (!line.startsWith(";")) {
                    firstNames.add(line.trim());
                }
            }
        }

        // Read last names from data file.
        InputStream lastNamesIn = this.getClass().getClassLoader().getResourceAsStream(LAST_NAMES_FILE);
        try (BufferedReader lastNamesReader = new BufferedReader(new InputStreamReader(lastNamesIn))) {
            while ((line = lastNamesReader.readLine()) != null) {
                if (!line.startsWith(";")) {
                    lastNames.add(line.trim());
                }
            }
        }
    }

    @Override
    public String next() {
        StringBuilder strBuilder = new StringBuilder();

        // First name.
        int fix = MathUtil.stripSignBit(rnd.nextInt()) % firstNames.size();
        strBuilder.append(firstNames.get(fix));
        strBuilder.append(" ");

        // Optional middle initial.
        double d = Math.abs(rnd.nextDouble());
        if (d <= middleInitialProb) {
            int mix = MathUtil.stripSignBit(rnd.nextInt()) % LETTERS.length();
            strBuilder.append(LETTERS.charAt(mix));
            strBuilder.append(". ");
        }

        // Last name.
        int lix = MathUtil.stripSignBit(rnd.nextInt()) % lastNames.size();
        strBuilder.append(lastNames.get(lix));

        return strBuilder.toString();
    }

    @Override
    public void reset() {
    }
}
