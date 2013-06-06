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
package edu.uci.ics.pregelix.example.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class TestUtils {

    public static void compareWithResultDir(File expectedFileDir, File actualFileDir) throws Exception {
        String[] fileNames = expectedFileDir.list();
        for (String fileName : fileNames) {
            compareWithResult(new File(expectedFileDir, fileName), new File(actualFileDir, fileName));
        }
    }

    public static void compareWithResult(File expectedFile, File actualFile) throws Exception {
        BufferedReader readerExpected = new BufferedReader(new FileReader(expectedFile));
        BufferedReader readerActual = new BufferedReader(new FileReader(actualFile));
        String lineExpected, lineActual;
        int num = 1;
        try {
            while ((lineExpected = readerExpected.readLine()) != null) {
                lineActual = readerActual.readLine();
                if (lineActual == null) {
                    throw new Exception("Actual result changed at line " + num + ":\n< " + lineExpected + "\n> ");
                }
                if (!equalStrings(lineExpected, lineActual)) {
                    throw new Exception("Result for changed at line " + num + ":\n< " + lineExpected + "\n> "
                            + lineActual);
                }
                ++num;
            }
            lineActual = readerActual.readLine();
            if (lineActual != null) {
                throw new Exception("Actual result changed at line " + num + ":\n< \n> " + lineActual);
            }
        } finally {
            readerExpected.close();
            readerActual.close();
        }
    }

    private static boolean equalStrings(String s1, String s2) {
        String[] rowsOne = s1.split("\n");
        String[] rowsTwo = s2.split("\n");

        if (rowsOne.length != rowsTwo.length)
            return false;

        for (int i = 0; i < rowsOne.length; i++) {
            String row1 = rowsOne[i];
            String row2 = rowsTwo[i];

            if (row1.equals(row2))
                continue;

            boolean spaceOrTab = false;
            spaceOrTab = row1.contains(" ");
            String[] fields1 = spaceOrTab ? row1.split(" ") : row1.split("\t");
            String[] fields2 = spaceOrTab ? row2.split(" ") : row2.split("\t");

            for (int j = 0; j < fields1.length; j++) {
                if (fields1[j].equals(fields2[j])) {
                    continue;
                } else if (fields1[j].indexOf('.') < 0) {
                    return false;
                } else {
                    Double double1 = Double.parseDouble(fields1[j]);
                    Double double2 = Double.parseDouble(fields2[j]);
                    float float1 = (float) double1.doubleValue();
                    float float2 = (float) double2.doubleValue();

                    if (Math.abs(float1 - float2) < 1.0e-7)
                        continue;
                    else {
                        return false;
                    }
                }
            }
        }
        return true;
    }

}
