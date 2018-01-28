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
package org.apache.asterix.tools.tbltoadm;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

public class TblToAdm {

    private static final String SEPARATOR_STRING = "\\|\\s*";
    private static final char SEPARATOR_CHAR = '|';

    private static void convertFile(String inputFileName, String outputFileName, String scmFileName)
            throws IOException {
        File scmFile = new File(scmFileName);
        File inFile = new File(inputFileName);
        File outFile = new File(outputFileName);

        BufferedReader scmReader = new BufferedReader(new FileReader(scmFile));
        String scmStr = scmReader.readLine();
        String[] fieldInfo = scmStr.split(SEPARATOR_STRING);
        String[] fieldNames = new String[fieldInfo.length];
        boolean[] fieldIsString = new boolean[fieldInfo.length];
        for (int i = 0; i < fieldInfo.length; i++) {
            String[] f = fieldInfo[i].split(":\\s*");
            fieldNames[i] = f[0];
            fieldIsString[i] = f[1].equals("string");
        }
        scmReader.close();

        BufferedReader reader = new BufferedReader(new FileReader(inFile));
        PrintWriter writer = new PrintWriter(outFile);
        String row;
        while ((row = reader.readLine()) != null) {
            // String[] fields = row.split(SEPARATOR);
            if (row.length() == 0 || row.charAt(0) == ' ' || row.charAt(0) == '\n') {
                continue;
            }
            writer.write("{  ");
            int pos = 0;
            int strlen = row.length();
            for (int i = 0; i < fieldNames.length; i++) {
                if (i > 0) {
                    writer.write(", ");
                }
                writer.print('"');
                writer.write(fieldNames[i]);
                writer.write("\": ");
                if (fieldIsString[i]) {
                    writer.print('"');
                }
                for (; pos < strlen; pos++) {
                    char c = row.charAt(pos);
                    if (c != SEPARATOR_CHAR) {
                        writer.print(c);

                    } else {
                        ++pos;
                        break;
                    }
                }
                if (fieldIsString[i]) {
                    writer.print('"');
                }
            }
            writer.write("  }\n");
        }
        writer.close();
        reader.close();
    }

    public static void main(String args[]) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: bash tbl2adm <srcfile.tbl> <destfile.adm> <schema-file>");
            return;
        }
        convertFile(args[0], args[1], args[2]);
    }
}
