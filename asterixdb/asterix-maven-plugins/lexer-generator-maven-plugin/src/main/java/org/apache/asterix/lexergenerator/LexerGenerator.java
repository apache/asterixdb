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
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.maven.plugin.logging.Log;

public class LexerGenerator {
    private LinkedHashMap<String, Token> tokens = new LinkedHashMap<String, Token>();
    private Log logger;

    public LexerGenerator() {
    }

    public LexerGenerator(Log logger) {
        this.logger = logger;
    }

    private void log(String info) {
        if (logger == null) {
            System.out.println(info);
        } else {
            logger.info(info);
        }
    }

    public void addToken(String rule) throws Exception {
        Token newToken;
        if (rule.charAt(0) == '@') {
            newToken = new TokenAux(rule, tokens);
        } else {
            newToken = new Token(rule, tokens);
        }
        Token existingToken = tokens.get(newToken.getName());
        if (existingToken == null) {
            tokens.put(newToken.getName(), newToken);
        } else {
            existingToken.merge(newToken);
        }
    }

    public void generateLexer(HashMap<String, String> config) throws Exception {
        LexerNode main = this.compile();
        config.put("TOKENS_CONSTANTS", this.tokensConstants());
        config.put("TOKENS_IMAGES", this.tokensImages());
        config.put("LEXER_LOGIC", main.toJava());
        config.put("LEXER_AUXFUNCTIONS", replaceParams(this.auxiliaryFunctions(main), config));
        String[] files = { "/Lexer.java", "/LexerException.java" };
        String outputDir = config.get("OUTPUT_DIR");
        (new File(outputDir)).mkdirs();
        for (String file : files) {
            String input = readFile(LexerGenerator.class.getResourceAsStream(file));
            String fileOut = file.replace("Lexer", config.get("LEXER_NAME"));
            String output = replaceParams(input, config);
            log("Generating: " + file + "\t>>\t" + fileOut);
            FileWriter out = new FileWriter((new File(outputDir, fileOut)).toString());
            out.write(output);
            out.close();
            log(" [done]\n");
        }
    }

    public String printParsedGrammar() {
        StringBuilder result = new StringBuilder();
        for (Token token : tokens.values()) {
            result.append(token.toString()).append("\n");
        }
        return result.toString();
    }

    private LexerNode compile() throws Exception {
        LexerNode main = new LexerNode();
        for (Token token : tokens.values()) {
            if (token instanceof TokenAux)
                continue;
            main.merge(token.getNode());
        }
        return main;
    }

    private String tokensImages() {
        StringBuilder result = new StringBuilder();
        Set<String> uniqueTokens = tokens.keySet();
        for (String token : uniqueTokens) {
            result.append(", \"<").append(token).append(">\" ");
        }
        return result.toString();
    }

    private String tokensConstants() {
        StringBuilder result = new StringBuilder();
        Set<String> uniqueTokens = tokens.keySet();
        int i = 2;
        for (String token : uniqueTokens) {
            result.append(", TOKEN_").append(token).append("=").append(i).append(" ");
            i++;
        }
        return result.toString();
    }

    private String auxiliaryFunctions(LexerNode main) {
        StringBuilder result = new StringBuilder();
        Set<String> functions = main.neededAuxFunctions();
        for (String token : functions) {
            result.append("private int parse_" + token + "(char currentChar) throws IOException {\n");
            result.append(tokens.get(token).getNode().toJavaAuxFunction());
            result.append("\n}\n\n");
        }
        return result.toString();
    }

    private static String readFile(Reader input) throws IOException {
        StringBuffer fileData = new StringBuffer(1000);
        BufferedReader reader = new BufferedReader(input);
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

    private static String readFile(InputStream input) throws IOException {
        if (input == null) {
            throw new FileNotFoundException();
        }
        return readFile(new InputStreamReader(input));
    }

    private static String readFile(String fileName) throws IOException {
        return readFile(new FileReader(fileName));
    }

    private static String replaceParams(String input, HashMap<String, String> config) {
        for (Entry<String, String> param : config.entrySet()) {
            String key = "\\[" + param.getKey() + "\\]";
            String value = param.getValue();
            input = input.replaceAll(key, value);
        }
        return input;
    }

    public static void main(String args[]) throws Exception {
        if (args.length == 0 || args[0] == "--help" || args[0] == "-h") {
            System.out.println("LexerGenerator\nusage: java LexerGenerator <configuration file>");
            return;
        }

        LexerGenerator lexer = new LexerGenerator();
        HashMap<String, String> config = new HashMap<String, String>();

        System.out.println("Config file:\t" + args[0]);
        String input = readFile(args[0]);
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

        String parsedGrammar = lexer.printParsedGrammar();
        lexer.generateLexer(config);
        System.out.println("\nGenerated grammar:");
        System.out.println(parsedGrammar);
    }

}
