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

import java.util.regex.Pattern;

public class FuzzyJoinUtil {
    private static final Pattern rePunctuation = Pattern.compile("[^\\p{L}\\p{N}]"); // L:Letter, N:Number
    private static final Pattern reSpaceOrAnderscore = Pattern.compile("(_|\\s)+");

    public static String clean(String in) {
        /*
         * - remove punctuation
         *
         * - normalize case
         *
         * - remove extra spaces
         *
         * - repalce space with FuzzyJoinDriver.TOKEN_SEPARATOR
         */

        in = rePunctuation.matcher(in).replaceAll(" ");
        in = reSpaceOrAnderscore.matcher(in).replaceAll(" ");
        in = in.trim();
        in = in.replace(' ', '_');
        in = in.toLowerCase();
        return in;
    }

    /**
     * @param splits
     *            splitted record
     * @param dataColumns
     *            column index of data columns
     * @param tokenSeparator
     *            TODO
     * @return concatenation of data column values
     */
    public static String getData(Object[] splits, int[] dataColumns, char tokenSeparator) {
        String data = null;
        for (int dataColumn : dataColumns) {
            if (data != null) {
                data += tokenSeparator;
            }
            if (splits.length > dataColumn) {
                if (data == null) {
                    data = "";
                }
                // data += splits[dataColumns[i]];
                data += clean(splits[dataColumn].toString());
            }
        }
        return data;
    }

    /**
     * @param columnsString
     *            string containing the indexes of the columns containing data
     * @return array of data columns indexes
     */
    public static int[] getDataColumns(String columnsString) {
        String[] splits = columnsString.split(FuzzyJoinConfig.RECORD_DATA_VALUE_SEPARATOR_REGEX);
        int[] columns = new int[splits.length];
        for (int i = 0; i < splits.length; i++) {
            columns[i] = Integer.parseInt(splits[i]);
        }
        return columns;
    }

}
