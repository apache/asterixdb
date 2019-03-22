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
package org.apache.asterix.lexergenerator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;

/**
 * @goal generate-lexer
 * @phase generate-sources
 * @requiresDependencyResolution compile
 */
public class LexerGeneratorMojo extends AbstractMojo {
    /**
     * parameter injected from pom.xml
     *
     * @parameter
     * @required
     */
    private File grammarFile;

    /**
     * parameter injected from pom.xml
     *
     * @parameter
     * @required
     */
    private File outputDir;

    public void execute() throws MojoExecutionException {
        LexerGenerator lexer = new LexerGenerator(getLog());
        HashMap<String, String> config = new HashMap<String, String>();
        getLog().info("--- Lexer Generator Maven Plugin - started with grammarFile: " + grammarFile.toString());
        try {
            String input = readFile(grammarFile);
            config.put("OUTPUT_DIR", outputDir.toString());
            boolean tokens = false;
            for (String line : input.split("\r?\n")) {
                line = line.trim();
                if (line.length() == 0 || line.charAt(0) == '#')
                    continue;
                if (tokens == false && !line.equals("TOKENS:")) {
                    config.put(line.split("\\s*:\\s*")[0], line.split("\\s*:\\s*")[1]);
                } else if (line.equals("TOKENS:")) {
                    tokens = true;
                } else {
                    lexer.addToken(line);
                }
            }
            lexer.generateLexer(config);
        } catch (Throwable e) {
            throw new MojoExecutionException("Error while generating lexer", e);
        }
        String parsedGrammar = lexer.printParsedGrammar();
        getLog().info("--- Generated grammar:\n" + parsedGrammar);
    }

    private String readFile(File file) throws FileNotFoundException, IOException {
        StringBuffer fileData = new StringBuffer(1000);
        BufferedReader reader = new BufferedReader(new FileReader(file));
        char[] buf = new char[1024];
        int numRead = 0;
        while ((numRead = reader.read(buf)) != -1) {
            String readData = String.valueOf(buf, 0, numRead);
            fileData.append(readData);
            buf = new char[1024];
        }
        reader.close();
        return fileData.toString();
    }

}
