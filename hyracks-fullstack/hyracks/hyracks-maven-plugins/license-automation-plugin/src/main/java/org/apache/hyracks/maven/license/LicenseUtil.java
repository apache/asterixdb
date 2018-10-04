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
package org.apache.hyracks.maven.license;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.maven.project.MavenProject;

public class LicenseUtil {

    private static int wrapLength = 80;
    private static int wrapThreshold = 100;

    private LicenseUtil() {
    }

    public static void setWrapLength(int wrapLength) {
        LicenseUtil.wrapLength = wrapLength;
    }

    public static void setWrapThreshold(int wrapThreshold) {
        LicenseUtil.wrapThreshold = wrapThreshold;
    }

    public static String trim(String input) throws IOException {
        try (BufferedReader reader = new BufferedReader(new StringReader(input))) {
            reader.mark(input.length() + 1);
            StringWriter sw = new StringWriter();
            trim(sw, reader);
            return sw.toString();
        }
    }

    public static String process(String input, boolean unpad, boolean wrap, boolean strict) throws IOException {
        try (BufferedReader reader = new BufferedReader(new StringReader(input))) {
            reader.mark(input.length() + 1);
            StringWriter sw = new StringWriter();
            trim(sw, reader, unpad, wrap, strict);
            sw.append('\n');
            return sw.toString();
        }
    }

    public static void readAndTrim(Writer out, File file) throws IOException {
        try (FileInputStream fis = new FileInputStream(file);
                BufferedReader reader = new BufferedReader(new InputStreamReader(fis, StandardCharsets.UTF_8))) {
            reader.mark((int) file.length() * 2);
            trim(out, reader);
        }
    }

    private static void trim(Writer out, BufferedReader reader) throws IOException {
        trim(out, reader, true, true, false);
    }

    private static void trim(Writer out, BufferedReader reader, boolean unpad, boolean wrap, boolean strict)
            throws IOException {
        Pair<Integer, Integer> result = null;
        if (unpad || wrap) {
            result = analyze(reader);
            reader.reset();
        }
        doTrim(out, reader, unpad ? result.getLeft() : 0,
                wrap && (result.getRight() > wrapThreshold) ? wrapLength : Integer.MAX_VALUE, strict);
    }

    private static void doTrim(Writer out, BufferedReader reader, int extraPadding, int wrapLength, boolean strict)
            throws IOException {
        boolean head = true;
        int empty = 0;
        for (String line = reader.readLine(); line != null; line = reader.readLine()) {
            if ("".equals(line.trim())) {
                if (!head) {
                    empty++;
                }
            } else {
                head = false;
                for (; empty > 0; empty--) {
                    out.append('\n');
                }
                String trimmed = line.substring(extraPadding);
                int leadingWS = trimmed.length() - trimmed.trim().length();
                while (trimmed.length() > wrapLength) {
                    int cut = trimmed.lastIndexOf(' ', wrapLength);
                    cut = Math.max(cut, trimmed.lastIndexOf('\t', wrapLength));
                    if (cut != -1 && cut > leadingWS) {
                        out.append(trimmed.substring(0, cut));
                        out.append('\n');
                        trimmed = trimmed.substring(cut + 1);
                    } else if (!strict) {
                        break;
                    } else {
                        out.append(trimmed.substring(0, wrapLength));
                        out.append('\n');
                        trimmed = trimmed.substring(wrapLength);
                    }
                    for (int i = 0; i < leadingWS; i++) {
                        trimmed = ' ' + trimmed;
                    }
                }
                out.append(trimmed);
                empty++;
            }
        }
    }

    private static Pair<Integer, Integer> analyze(BufferedReader reader) throws IOException {
        int freeSpaces = Integer.MAX_VALUE;
        int maxLineLength = 0;
        for (String line = reader.readLine(); line != null; line = reader.readLine()) {
            // trim trailing WS
            String rightTrimmed = line.replaceFirst("\\s*$", "");
            if ("".equals(rightTrimmed)) {
                // ignore empty lines
                continue;
            }
            String fullyTrimmed = line.trim();
            freeSpaces = Math.min(freeSpaces, rightTrimmed.length() - fullyTrimmed.length());
            maxLineLength = Math.max(maxLineLength, fullyTrimmed.length());
        }
        return new ImmutablePair<>(freeSpaces, maxLineLength);
    }

    static String toGav(MavenProject dep) {
        return dep.getGroupId() + ":" + dep.getArtifactId() + ":" + dep.getVersion();
    }
}
