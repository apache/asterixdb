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
package org.apache.asterix.testframework.template;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.asterix.testframework.datagen.LoremIpsumReplacement;
import org.apache.asterix.testframework.datagen.TemplateReplacement;

public class TemplateHelper {

    private final Map<String, TemplateReplacement> replacements = new HashMap<>();
    private final Pattern replacementPattern;

    public static final TemplateHelper INSTANCE = new TemplateHelper();

    private TemplateHelper() {
        registerReplacement(LoremIpsumReplacement.INSTANCE);
        StringBuilder pattern = new StringBuilder();
        replacements.forEach((key, value) -> {
            if (pattern.length() == 0) {
                pattern.append("%(");
            } else {
                pattern.append("|");
            }
            pattern.append(key);
        });
        pattern.append(")[^%]*%");
        replacementPattern = Pattern.compile(pattern.toString());
    }

    private void registerReplacement(TemplateReplacement replacement) {
        replacements.put(replacement.tag(), replacement);
    }

    public File resolveTemplateFile(File inputFile) throws IOException {
        File outputFile = File.createTempFile("template.",
                "." + inputFile.getName().substring(0, inputFile.getName().lastIndexOf(".template")));
        outputFile.deleteOnExit();
        processFile(inputFile, outputFile);
        return outputFile;
    }

    public void processFile(File inputFile, File outputFile) throws IOException {
        synchronized (this) {
            outputFile.getParentFile().mkdirs();
        }
        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile));
                BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                Matcher m = replacementPattern.matcher(line);
                if (m.find()) {
                    writer.write(line, 0, m.start());
                    replacements.get(m.group(1)).appendReplacement(m.group(0), writer);
                    writer.write(line, m.end(), line.length() - m.end());
                    writer.newLine();
                } else {
                    writer.write(line + "\n");
                }
            }
        }
    }
}
