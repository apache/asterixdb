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
package org.apache.asterix.extension.grammar;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.asterix.external.input.record.CharArrayRecord;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.commons.lang3.StringUtils;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

/**
 * a Mojo for creating a grammar extension
 */
@Mojo(name = "grammarix")
public class GrammarExtensionMojo extends AbstractMojo {

    private static final String PARSER_BEGIN = "PARSER_BEGIN";
    private static final String PARSER_END = "PARSER_END";
    private static final char OPEN_BRACE = '{';
    private static final char CLOSE_BRACE = '}';
    private static final char OPEN_ANGULAR = '<';
    private static final char CLOSE_ANGULAR = '>';
    private static final char OPEN_PAREN = '(';
    private static final char CLOSE_PAREN = ')';
    private static final char SEMICOLON = ';';
    private static final List<Character> SIG_SPECIAL_CHARS =
            Arrays.asList(new Character[] { '(', ')', ':', '<', '>', ';', '.' });
    private static final String KWCLASS = "class";
    private static final String KWIMPORT = "import";
    private static final String KWUNIMPORT = "unimport";
    private static final String KWPACKAGE = "package";
    private static final String NEWPRODUCTION = "@new";
    // Adds a construct at the end of the file.
    private static final String NEW_AT_THE_END_PRODUCTION = "@new_at_the_end";
    // Adds a method at the end of the class definition.
    private static final String NEW_AT_THE_END_CLASS_DEFINITION = "@new_at_the_class_def";
    private static final String MERGEPRODUCTION = "@merge";
    private static final String OVERRIDEPRODUCTION = "@override";
    private static final String BEFORE = "before:";
    private static final String AFTER = "after:";
    private static final String REPLACE = "replace";
    private static final String WITH = "with";
    private static final String OPTION_TRUE = "true";
    private static final String OPTION_FALSE = "false";
    private static final List<String> KEYWORDS =
            Arrays.asList(new String[] { KWCLASS, KWIMPORT, KWPACKAGE, PARSER_BEGIN, PARSER_END });
    private static final List<String> EXTENSIONKEYWORDS =
            Arrays.asList(new String[] { KWIMPORT, KWUNIMPORT, NEWPRODUCTION, NEW_AT_THE_END_PRODUCTION,
                    NEW_AT_THE_END_CLASS_DEFINITION, OVERRIDEPRODUCTION, MERGEPRODUCTION });
    private static final String REGEX_WS_DOT_SEMICOLON = "\\s|[.]|[;]";
    private static final String REGEX_WS_PAREN = "\\s|[(]|[)]";
    private static final String OPTIONS = "options";
    private CharArrayRecord record = new CharArrayRecord();
    private Position position = new Position();
    private Map<String, Pair<String, String>> extensibles = new HashMap<>();
    private Map<String, String[]> mergeElements = new HashMap<>();
    private List<Pair<String, String>> baseFinals = new ArrayList<>();
    private List<Pair<String, String>> extensionFinals = new ArrayList<>();
    private List<Pair<String, String>> extensionFinalsAtTheEnd = new ArrayList<>();
    private List<Pair<String, String>> extensionMethodsAtTheClassDef = new ArrayList<>();
    private List<List<String>> imports = new ArrayList<>();
    private String baseClassName;
    private String baseClassDef;
    private String optionsBlock;
    private boolean read = false;
    private boolean shouldReplace = false;

    // Used in @merge replace. If set to true, applies each block's changes and stores them.
    private boolean shouldApplyEachBlockChange = true;

    private String oldPhrase = null;
    private String newPhrase = null;

    @Parameter(property = "grammarix.base")
    private String base;

    @Parameter(property = "grammarix.gbase")
    private String gbase;

    @Parameter(property = "grammarix.gextension")
    private String gextension;

    @Parameter(property = "grammarix.output")
    private String output;

    @Parameter(property = "grammarix.packageName")
    private String packageName;

    @Parameter(property = "grammarix.parserClassName")
    private String parserClassName;
    private String lastIdentifier;

    @Parameter(property = "grammarix.parserClassModifier")
    private String parserClassModifier;

    @Override
    public void execute() throws MojoExecutionException {
        base = new File(base).getAbsolutePath();
        gbase = resolvePath(gbase);
        gextension = resolvePath(gextension);
        output = resolvePath(output);
        getLog().info("Base dir: " + base);
        getLog().info("Grammar-base: " + gbase);
        getLog().info("Grammar-extension: " + gextension);
        getLog().info("Output: " + output);
        processBase();
        processExtension();
        generateOutput();
    }

    private String resolvePath(String path) {
        return new File(path).isAbsolute() ? path : new File(base, path).getAbsolutePath();
    }

    private void generateOutput() throws MojoExecutionException {
        File outputFile = prepareOutputFile();
        try (BufferedWriter writer = Files.newBufferedWriter(outputFile.toPath(), StandardCharsets.UTF_8)) {
            // Options
            if (optionsBlock != null) {
                writer.write(OPTIONS);
                writer.write(optionsBlock);
            }
            writer.newLine();

            // Parser Begin
            writer.write(PARSER_BEGIN);
            writer.write(OPEN_PAREN);
            writer.write(parserClassName);
            writer.write(CLOSE_PAREN);
            writer.newLine();
            writer.newLine();

            // Package
            writer.write(KWPACKAGE);
            writer.write(" ");
            writer.write(packageName);
            writer.write(SEMICOLON);
            writer.newLine();
            writer.newLine();

            // Imports
            List<String> importList = new ArrayList<>();
            for (List<String> importTokens : imports) {
                importList.add(importToString(importTokens));
            }
            Collections.sort(importList);
            for (String importStatement : importList) {
                writer.write(importStatement);
                writer.newLine();
            }

            writer.newLine();

            // Class definition
            String classDef = baseClassDef.replaceAll(baseClassName, parserClassName);
            if (parserClassModifier != null && parserClassModifier.length() > 0) {
                // Adds a class modifier if any.
                classDef = classDef.replaceFirst(KWCLASS, parserClassModifier + " " + KWCLASS);
            }

            // Process extensions at the end of the class definition
            if (!extensionMethodsAtTheClassDef.isEmpty()) {
                int index = classDef.lastIndexOf(CLOSE_BRACE);
                if (index != -1) {
                    String classDefExtension = "";
                    for (Pair<String, String> element : extensionMethodsAtTheClassDef) {
                        classDefExtension += toOutput(element.first);
                        classDefExtension += "\n";
                        classDefExtension += element.second;
                        classDefExtension += "\n";
                    }
                    classDef =
                            classDef.substring(0, index) + "\n" + classDefExtension + "\n" + classDef.substring(index);
                }
            }

            writer.write(classDef);
            writer.newLine();

            // Parser End
            writer.write(PARSER_END);
            writer.write(OPEN_PAREN);
            writer.write(parserClassName);
            writer.write(CLOSE_PAREN);
            writer.newLine();
            writer.newLine();

            // Extensibles
            for (Entry<String, Pair<String, String>> entry : extensibles.entrySet()) {
                writer.newLine();
                String signature = entry.getKey();
                if (mergeElements.containsKey(signature)) {
                    writer.write("// Merged Non-terminal");
                    writer.newLine();
                }
                writer.write(extensibleSignatureToOutput(signature));
                writer.newLine();
                if (mergeElements.containsKey(signature)) {
                    merge(writer, entry.getValue(), mergeElements.get(signature));
                } else {
                    writer.write(entry.getValue().first);
                    writer.newLine();
                    if (entry.getValue().second != null) {
                        writer.write(entry.getValue().second);
                        writer.newLine();
                    }
                }
            }

            for (Pair<String, String> element : extensionFinals) {
                writer.write(toOutput(element.first));
                writer.newLine();
                writer.write(element.second);
                writer.newLine();
            }

            for (Pair<String, String> element : baseFinals) {
                writer.write(toOutput(element.first));
                writer.newLine();
                writer.write(element.second);
                writer.newLine();
            }

            for (Pair<String, String> element : extensionFinalsAtTheEnd) {
                writer.write(toOutput(element.first));
                writer.newLine();
                writer.write(element.second);
                writer.newLine();
            }

        } catch (Exception e) {
            getLog().error(e);
            throw new MojoExecutionException(e.getMessage(), e);
        }
    }

    private String extensibleSignatureToOutput(String signature) {
        StringBuilder aString = new StringBuilder();
        String[] tokens = signature.split(" ");
        aString.append(tokens[0]);
        for (int i = 1; i < tokens.length; i++) {
            if (tokens[i - 1].charAt(tokens[i - 1].length() - 1) == CLOSE_PAREN
                    || (!SIG_SPECIAL_CHARS.contains(tokens[i].charAt(0))
                            && !SIG_SPECIAL_CHARS.contains(tokens[i - 1].charAt(tokens[i - 1].length() - 1)))) {
                aString.append(" ");
            }
            aString.append(tokens[i]);
        }
        return aString.toString();
    }

    private String toOutput(String signature) {
        if (signature.indexOf(OPEN_ANGULAR) == 0) {
            // a final
            StringBuilder aString = new StringBuilder();
            aString.append(signature.substring(0, signature.indexOf(CLOSE_ANGULAR) + 1));
            aString.append('\n');
            aString.append(signature.substring(signature.indexOf(CLOSE_ANGULAR) + 1));
            return aString.toString();
        } else {
            return signature;
        }
    }

    private void merge(BufferedWriter writer, Pair<String, String> baseBlocks, String[] extensions)
            throws IOException, MojoExecutionException {
        String errorMessage = "Merged base node doesn't conform to expected mergable node structure";
        int block1Open = baseBlocks.first.indexOf(OPEN_BRACE);
        int block1Close = baseBlocks.first.lastIndexOf(CLOSE_BRACE);
        // first block
        writer.write(OPEN_BRACE);
        if (extensions[0] != null) {
            writer.write(extensions[0]);
        }
        writer.write(baseBlocks.first.substring(block1Open + 1, block1Close));
        if (extensions[1] != null) {
            writer.write(extensions[1]);
        }
        writer.write(CLOSE_BRACE);
        writer.newLine();
        // second block
        writer.write(OPEN_BRACE);
        writer.newLine();
        if (extensions[2] != null) {
            writer.write(extensions[2]);
        }
        String innerBlock2String = null;
        if (baseBlocks.second != null) {
            BufferedReader blockReader = stringToReader(baseBlocks.second);
            Position blockPosition = new Position();
            blockPosition.index = 0;
            blockPosition.line = blockReader.readLine();
            while (blockPosition.line != null
                    && (blockPosition.line.trim().length() == 0 || blockPosition.line.indexOf(OPEN_BRACE) < 0)) {
                if (blockPosition.line.trim().length() > 0) {
                    writer.write(blockPosition.line);
                    writer.newLine();
                }
                blockPosition.line = blockReader.readLine();
            }
            if (blockPosition.line == null) {
                throw new MojoExecutionException(errorMessage);
            }
            int block2Open = blockPosition.line.indexOf(OPEN_BRACE);
            blockPosition.line = blockPosition.line.substring(block2Open + 1);
            while (blockPosition.line != null
                    && (blockPosition.line.trim().length() == 0 || blockPosition.line.indexOf(OPEN_PAREN) < 0)) {
                if (blockPosition.line.trim().length() > 0) {
                    writer.write(blockPosition.line);
                    writer.newLine();
                }
                blockPosition.line = blockReader.readLine();
            }
            if (blockPosition.line == null) {
                throw new MojoExecutionException(errorMessage);
            }
            int innerBlock1Open = blockPosition.line.indexOf(OPEN_PAREN);
            writer.write("  ");
            writer.write(OPEN_PAREN);
            blockPosition.index = innerBlock1Open;
            readBlock(blockReader, OPEN_PAREN, CLOSE_PAREN, blockPosition);
            String innerBlock1String = record.toString();
            writer.newLine();
            writer.write("    ");
            writer.write(innerBlock1String
                    .substring(innerBlock1String.indexOf(OPEN_PAREN) + 1, innerBlock1String.lastIndexOf(CLOSE_PAREN))
                    .trim());
            writer.newLine();
            record.reset();
            // read second inner block
            blockPosition.line = blockReader.readLine();
            while (blockPosition.line != null && blockPosition.line.trim().length() == 0) {
                blockPosition.line = blockReader.readLine();
            }
            int innerBlock2Open = blockPosition.line.indexOf(OPEN_BRACE);
            if (innerBlock2Open < 0) {
                throw new MojoExecutionException(errorMessage);
            }
            blockPosition.index = innerBlock2Open;
            readBlock(blockReader, OPEN_BRACE, CLOSE_BRACE, blockPosition);
            innerBlock2String = record.toString();
            record.reset();
        }
        if (extensions[3] != null) {
            writer.write(extensions[3]);
        }
        writer.newLine();
        writer.write("  ");
        writer.write(CLOSE_PAREN);
        writer.newLine();
        writer.write("  ");
        writer.write(OPEN_BRACE);
        if (extensions[4] != null) {
            writer.write(extensions[4]);
        }
        if (innerBlock2String != null) {
            writer.newLine();
            writer.write("  ");
            writer.write(innerBlock2String
                    .substring(innerBlock2String.indexOf(OPEN_BRACE) + 1, innerBlock2String.lastIndexOf(CLOSE_BRACE))
                    .trim());
            writer.newLine();
        }
        if (extensions[5] != null) {
            writer.write(extensions[5]);
        }
        // Close inner second block
        writer.write("  ");
        writer.write(CLOSE_BRACE);
        writer.newLine();
        // Close second block
        writer.write(CLOSE_BRACE);
        writer.newLine();
    }

    class Position {
        String line;
        int index;
    }

    private void readBlock(BufferedReader reader, char start, char end) throws IOException, MojoExecutionException {
        readBlock(reader, start, end, position);
    }

    private void readBlock(BufferedReader reader, char start, char end, Position position)
            throws IOException, MojoExecutionException {
        record.reset();
        char[] chars = position.line.toCharArray();
        if (chars[position.index] != start) {
            throw new MojoExecutionException(
                    "attempt to read a non terminal that doesn't start with a brace. @Line: " + position.line);
        }
        boolean prevCharEscape = false;
        boolean inString = false;
        boolean hasFinished = false;
        int depth = 0;
        int bufferPosn = position.index;
        int bufferLength = chars.length;
        do {
            int startPosn = bufferPosn;
            if (bufferPosn >= bufferLength) {
                startPosn = bufferPosn = 0;
                record.append("\n".toCharArray());
                position.line = reader.readLine();
                chars = position.line.toCharArray();
                bufferLength = chars.length;
            }
            for (; bufferPosn < bufferLength; ++bufferPosn) {
                if (inString) {
                    // we are in a string, we only care about the string end
                    if (chars[bufferPosn] == ExternalDataConstants.QUOTE && !prevCharEscape) {
                        inString = false;
                    }
                    if (prevCharEscape) {
                        prevCharEscape = false;
                    } else {
                        prevCharEscape = chars[bufferPosn] == ExternalDataConstants.ESCAPE;
                    }
                } else {
                    if (chars[bufferPosn] == ExternalDataConstants.QUOTE) {
                        inString = true;
                    } else if (chars[bufferPosn] == start) {
                        depth += 1;
                    } else if (chars[bufferPosn] == end) {
                        depth -= 1;
                        if (depth == 0) {
                            hasFinished = true;
                            bufferPosn++;
                            position.index = bufferPosn;
                            break;
                        }
                    }
                }
            }
            int appendLength = bufferPosn - startPosn;
            if (appendLength > 0) {
                record.append(chars, startPosn, appendLength);
            }
        } while (!hasFinished);
        record.endRecord();
    }

    private void processBase() throws MojoExecutionException {
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(gbase), StandardCharsets.UTF_8)) {
            StringBuilder identifier = new StringBuilder();
            while ((position.line = reader.readLine()) != null) {
                if (position.line.trim().startsWith("//")) {
                    // skip comments
                    continue;
                }
                String[] tokens = position.line.split(REGEX_WS_PAREN);
                position.index = 0;
                int openBraceIndex = position.line.indexOf(OPEN_BRACE);
                int openAngularIndex = position.line.trim().indexOf(OPEN_ANGULAR);
                if (tokens.length > 0 && identifier.length() == 0 && KEYWORDS.contains(tokens[0])) {
                    handleSpecialToken(tokens[0], reader);
                } else if (openBraceIndex >= 0 && openAngularIndex < 0) {
                    String beforeBrace = position.line.substring(0, openBraceIndex);
                    if (beforeBrace.trim().length() > 0) {
                        identifier.append(beforeBrace);
                    } else if (identifier.length() == 0) {
                        identifier.append(lastIdentifier);
                    }
                    position.index = openBraceIndex;
                    readBlock(reader, OPEN_BRACE, CLOSE_BRACE);
                    // if next non-white space character is an open brace, then  we need to append
                    addExtensibleProduction(identifier);
                } else if (openAngularIndex == 0) {
                    position.index = position.line.indexOf(OPEN_ANGULAR);
                    readFinalProduction(identifier, reader);
                    addFinalProduction(identifier, baseFinals);
                } else if (identifier.length() > 0 || position.line.trim().length() > 0) {
                    identifier.append(position.line);
                    identifier.append('\n');
                }
            }
        } catch (Exception e) {
            getLog().error(e);
            throw new MojoExecutionException(e.getMessage(), e);
        }
    }

    private void handleSpecialToken(String token, BufferedReader reader) throws IOException, MojoExecutionException {
        switch (token) {
            case PARSER_BEGIN:
                // parser begin. duh!
                parserBegin(reader);
                break;
            case PARSER_END:
                // parser end
                parserEnd(reader);
                break;
            case KWCLASS:
                // class declaration
                handleClassDeclaration(reader);
                break;
            case KWPACKAGE:
                // package declaration
                skipPackageDeclaration(reader);
                break;
            case KWIMPORT:
                handleImport(reader);
                // import statement
                break;
            default:
                break;
        }
    }

    private void addFinalProduction(StringBuilder identifier, List<Pair<String, String>> finals) {
        String sig = toSignature(identifier.toString());
        finals.add(new Pair<String, String>(sig, record.toString()));
        record.reset();
        identifier.setLength(0);
        lastIdentifier = null;
    }

    private void handleImport(BufferedReader reader) throws IOException {
        // will not work on two imports on a single line
        ArrayList<String> importList = new ArrayList<>();
        String[] tokens = position.line.split(REGEX_WS_DOT_SEMICOLON);
        importList.addAll(Arrays.asList(tokens));
        while (position.line.indexOf(SEMICOLON) < 0) {
            position.line = reader.readLine();
            tokens = position.line.split(REGEX_WS_DOT_SEMICOLON);
            importList.addAll(Arrays.asList(tokens));
        }
        imports.add(importList);
    }

    private void handleUnImport(BufferedReader reader) throws IOException {
        ArrayList<String> importList = new ArrayList<>();
        String[] tokens = position.line.split(REGEX_WS_DOT_SEMICOLON);
        importList.addAll(Arrays.asList(tokens));
        while (position.line.indexOf(SEMICOLON) < 0) {
            position.line = reader.readLine();
            tokens = position.line.split(REGEX_WS_DOT_SEMICOLON);
            importList.addAll(Arrays.asList(tokens));
        }
        // remove from imports
        Iterator<List<String>> it = imports.iterator();
        while (it.hasNext()) {
            List<String> anImport = it.next();
            if (anImport.size() == importList.size()) {
                boolean equals = true;
                for (int i = 1; i < anImport.size(); i++) {
                    if (!anImport.get(i).equals(importList.get(i))) {
                        equals = false;
                        break;
                    }
                }
                if (equals) {
                    it.remove();
                }
            }
        }
    }

    private String importToString(List<String> importTokens) {
        return "import " + StringUtils.join(importTokens.subList(1, importTokens.size()), '.') + ";";
    }

    private void skipPackageDeclaration(BufferedReader reader) throws IOException {
        while (position.line.indexOf(SEMICOLON) < 0) {
            position.line = reader.readLine();
        }
    }

    private void handleClassDeclaration(BufferedReader reader) throws IOException, MojoExecutionException {
        StringBuilder parserDef = new StringBuilder();
        int classPosition = position.line.indexOf(KWCLASS);
        int startIndex = position.line.indexOf(OPEN_BRACE, classPosition);
        if (startIndex < 0) {
            position.line = position.line.substring(classPosition).replace(baseClassName, parserClassName);
            while (startIndex < 0) {
                parserDef.append(position.line);
                parserDef.append('\n');
                position.line = reader.readLine();
                startIndex = position.line.indexOf(OPEN_BRACE);
            }
            parserDef.append(position.line, 0, startIndex);
        } else {
            parserDef.append(position.line, classPosition, startIndex);
        }
        position.index = startIndex;
        readBlock(reader, OPEN_BRACE, CLOSE_BRACE);
        String classBody = record.toString();
        parserDef.append(classBody);
        parserDef.append('\n');
        baseClassDef = parserDef.toString();
    }

    private void parserEnd(BufferedReader reader) throws IOException {
        int endIndex = position.line.indexOf(CLOSE_PAREN, position.index);
        while (endIndex < 0) {
            position.line = reader.readLine();
            endIndex = position.line.indexOf(CLOSE_PAREN);
        }
        position.index = endIndex;
    }

    private void parserBegin(BufferedReader reader) throws IOException {
        StringBuilder aStringBuilder = new StringBuilder();
        int startIndex = position.line.indexOf(OPEN_PAREN, position.index);
        while (startIndex < 0) {
            position.line = reader.readLine();
            startIndex = position.line.indexOf(OPEN_PAREN);
        }
        int endIndex = position.line.indexOf(CLOSE_PAREN, startIndex);
        if (endIndex < 0) {
            // start and end on different lines
            position.line = position.line.substring(startIndex + 1);
            while (endIndex < 0) {
                aStringBuilder.append(position.line);
                position.line = reader.readLine();
                endIndex = position.line.indexOf(CLOSE_PAREN);
            }
            aStringBuilder.append(position.line, 0, endIndex);
        } else {
            // start and end on the same line
            aStringBuilder.append(position.line.substring(startIndex + 1, endIndex));
        }
        position.index = endIndex;
        baseClassName = aStringBuilder.toString().trim();
    }

    private void readFinalProduction(StringBuilder identifier, BufferedReader reader)
            throws IOException, MojoExecutionException {
        int blockStart = position.line.indexOf(OPEN_BRACE);
        if (blockStart < 0) {
            position.line = position.line.substring(position.index);
            while (blockStart < 0) {
                identifier.append(position.line);
                identifier.append('\n');
                position.line = reader.readLine();
                blockStart = position.line.indexOf(OPEN_BRACE);
            }
            identifier.append(position.line.substring(0, blockStart));
        } else {
            identifier.append(position.line.substring(position.index, blockStart));
        }
        position.index = blockStart;
        readBlock(reader, OPEN_BRACE, CLOSE_BRACE);
    }

    private void addExtensibleProduction(StringBuilder identifier) {
        if (identifier.toString().trim().equals(OPTIONS)) {
            optionsBlock = record.toString();
        } else {
            String sig = toSignature(identifier.toString());
            Pair<String, String> pair = extensibles.get(sig);
            if (pair == null) {
                pair = new Pair<>(record.toString(), null);
                extensibles.put(sig, pair);
            } else {
                pair.second = record.toString();
            }
            lastIdentifier = identifier.toString();
        }
        record.reset();
        identifier.setLength(0);
    }

    private void processExtension() throws MojoExecutionException {
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(gextension), StandardCharsets.UTF_8)) {
            StringBuilder identifier = new StringBuilder();
            String nextOperation = OVERRIDEPRODUCTION;
            while (read || (position.line = reader.readLine()) != null) {
                read = false;
                if (position.line.trim().startsWith("//")) {
                    // skip comments
                    continue;
                }
                String[] tokens = position.line.split(REGEX_WS_PAREN);
                position.index = 0;
                int openBraceIndex = position.line.indexOf(OPEN_BRACE);
                int openAngularIndex = position.line.trim().indexOf(OPEN_ANGULAR);
                if (tokens.length > 0 && identifier.length() == 0 && EXTENSIONKEYWORDS.contains(tokens[0])) {
                    switch (tokens[0]) {
                        case KWIMPORT:
                            handleImport(reader);
                            break;
                        case KWUNIMPORT:
                            handleUnImport(reader);
                            break;
                        case NEWPRODUCTION:
                            nextOperation = NEWPRODUCTION;
                            break;
                        case NEW_AT_THE_END_PRODUCTION:
                            nextOperation = NEW_AT_THE_END_PRODUCTION;
                            break;
                        case NEW_AT_THE_END_CLASS_DEFINITION:
                            nextOperation = NEW_AT_THE_END_CLASS_DEFINITION;
                            break;
                        case MERGEPRODUCTION:
                            nextOperation = MERGEPRODUCTION;
                            shouldReplace = shouldReplace(tokens, position.line);
                            break;
                        case OVERRIDEPRODUCTION:
                            nextOperation = OVERRIDEPRODUCTION;
                            break;
                        default:
                            break;
                    }
                } else if (openBraceIndex >= 0 && openAngularIndex < 0) {
                    String beforeBrace = position.line.substring(0, openBraceIndex);
                    if (beforeBrace.trim().length() > 0) {
                        identifier.append(beforeBrace);
                    } else if (identifier.length() == 0) {
                        identifier.append(lastIdentifier);
                    }
                    position.index = openBraceIndex;
                    switch (nextOperation) {
                        case NEWPRODUCTION:
                            handleNew(identifier, reader);
                            break;
                        case NEW_AT_THE_END_CLASS_DEFINITION:
                            readFinalProduction(identifier, reader);
                            addFinalProduction(identifier, extensionMethodsAtTheClassDef);
                            break;
                        case OVERRIDEPRODUCTION:
                            handleOverride(identifier, reader);
                            break;
                        case MERGEPRODUCTION:
                            handleMerge(identifier, reader);
                            break;
                        default:
                            throw new MojoExecutionException("Malformed extention file");
                    }
                    nextOperation = NEWPRODUCTION;
                } else if (openAngularIndex == 0) {
                    if (nextOperation != NEWPRODUCTION && nextOperation != NEW_AT_THE_END_PRODUCTION) {
                        throw new MojoExecutionException("Can only add new REGEX production kind");
                    }
                    position.index = position.line.indexOf(OPEN_ANGULAR);
                    readFinalProduction(identifier, reader);
                    if (nextOperation == NEWPRODUCTION) {
                        addFinalProduction(identifier, extensionFinals);
                    } else if (nextOperation == NEW_AT_THE_END_PRODUCTION) {
                        addFinalProduction(identifier, extensionFinalsAtTheEnd);
                    }
                } else if (identifier.length() > 0 || position.line.trim().length() > 0) {
                    identifier.append(position.line);
                    identifier.append('\n');
                }
            }
        } catch (Exception e) {
            getLog().error(e);
            throw new MojoExecutionException(e.getMessage(), e);
        }
    }

    private boolean shouldReplace(String[] tokens, String currentLine) throws MojoExecutionException {
        boolean replace = false;
        String errMessage = "Allowed syntax after @merge: <REPLACE> \"oldPhrase\" <WITH> \"newPhrase\" <TRUE|FALSE>.";
        String errMessage1 = "The old phrase should be place between two quotes. (E.g., \"old\")";
        String errMessage2 = "The new phrase should be place between two quotes. (E.g., \"new\")";
        // @merge replace "oldphrase" with "newphrase" proceedBlock:true/false
        if (tokens.length >= 6) {
            // Checks whether "replace" exists.
            if (!tokens[1].equalsIgnoreCase(REPLACE)) {
                throw new MojoExecutionException(errMessage);
            }

            // Checks whether "with" exists.
            boolean withFound = false;
            for (int i = 3; i < tokens.length; i++) {
                if (tokens[i].equalsIgnoreCase(WITH)) {
                    withFound = true;
                    break;
                }
            }
            if (!withFound) {
                throw new MojoExecutionException(errMessage);
            }

            // Check whether the last parameter is true/false.
            // If this is true, then we check all blocks and process before: and after:.
            // If not, we don't process all blocks and replace "old" with "new" for all instances for
            // the entire part of the given method.
            if (tokens[tokens.length - 1].equalsIgnoreCase(OPTION_TRUE)) {
                shouldApplyEachBlockChange = true;
            } else if (tokens[tokens.length - 1].equalsIgnoreCase(OPTION_FALSE)) {
                shouldApplyEachBlockChange = false;
            } else {
                throw new MojoExecutionException(errMessage);
            }

            // Gets the old phrase.
            int oldStart = findQuotePos(currentLine, 0);
            if (oldStart < 0) {
                throw new MojoExecutionException(errMessage1);
            }
            int oldEnd = findQuotePos(currentLine, oldStart + 1);
            if (oldEnd < 0) {
                throw new MojoExecutionException(errMessage1);
            }
            oldPhrase = currentLine.substring(oldStart + 1, oldEnd);

            // Gets the new phrase.
            int newStart = findQuotePos(currentLine, oldEnd + 1);
            if (newStart < 0) {
                throw new MojoExecutionException(errMessage2);
            }
            int newEnd = findQuotePos(currentLine, newStart + 1);
            if (newEnd < 0) {
                throw new MojoExecutionException(errMessage2);
            }
            newPhrase = currentLine.substring(newStart + 1, newEnd);
            replace = true;
        }
        return replace;
    }

    private void handleOverride(StringBuilder identifier, BufferedReader reader)
            throws MojoExecutionException, IOException {
        readBlock(reader, OPEN_BRACE, CLOSE_BRACE);
        Pair<String, String> pair = new Pair<>(record.toString(), null);
        String sig = toSignature(identifier.toString());
        extensibles.put(sig, pair);
        record.reset();
        identifier.setLength(0);
        // will read ahead of loop cycle
        read = true;
        position.index = 0;
        position.line = reader.readLine();
        while (position.line != null && position.line.trim().length() == 0) {
            position.line = reader.readLine();
        }
        int openBraceIndex = position.line.indexOf(OPEN_BRACE);
        if (openBraceIndex > -1) {
            // consume
            read = false;
            position.index = openBraceIndex;
            readBlock(reader, OPEN_BRACE, CLOSE_BRACE);
            pair.second = record.toString();
            record.reset();
        }
    }

    private void handleNew(StringBuilder identifier, BufferedReader reader) throws MojoExecutionException, IOException {
        String sig = toSignature(identifier.toString());
        if (extensibles.containsKey(sig)) {
            throw new MojoExecutionException(identifier.toString() + " already exists in base grammar");
        }
        handleOverride(identifier, reader);
    }

    private void handleMerge(StringBuilder identifier, BufferedReader reader)
            throws MojoExecutionException, IOException {
        String sig = toSignature(identifier.toString());
        if (!extensibles.containsKey(sig)) {
            throw new MojoExecutionException(identifier.toString() + " doesn't exist in base grammar");
        } else if (shouldReplace) {
            Pair<String, String> baseMethods = extensibles.get(sig);
            // Literally replaces the old phrase with the new phrase.
            baseMethods.first = stringReplaceAll(baseMethods.first, oldPhrase, newPhrase);
            baseMethods.second = stringReplaceAll(baseMethods.second, oldPhrase, newPhrase);
            shouldReplace = false;
        }
        String[] amendments = new String[6];
        if (shouldApplyEachBlockChange) {
            // Applies and stores each block's change only if shouldApplyEachBlockChange is set to true.
            mergeElements.put(sig, amendments);
        } else {
            // If shouldApplyEachBlockChange is false, no result from each block's change are not stored.
            shouldApplyEachBlockChange = true;
        }
        // we don't need the identifier anymore
        identifier.setLength(0);
        // The first block
        readBlock(reader, OPEN_BRACE, CLOSE_BRACE);
        String block = record.toString();
        extractBeforeAndAfter(block, amendments, 0, 1);
        record.reset();
        position.index = 0;
        position.line = reader.readLine();
        // Skips empty lines.
        while (position.line != null && position.line.trim().length() == 0) {
            position.line = reader.readLine();
        }
        // The second block
        int openBraceIndex = position.line.indexOf(OPEN_BRACE);
        if (openBraceIndex > -1) {
            position.index = openBraceIndex;
            // Reads the entire part of the second block - two OPEN_BRACE and two CLOSE_BRACE.
            readBlock(reader, OPEN_BRACE, CLOSE_BRACE);
        } else {
            throw new MojoExecutionException("merge element doesn't have a second block");
        }
        block = record.toString();
        BufferedReader blockReader = stringToReader(block);
        String line = blockReader.readLine();
        while (line != null && line.indexOf(OPEN_BRACE) < 0) {
            line = blockReader.readLine();
        }
        if (line == null) {
            throw new MojoExecutionException("merge element doesn't have a correct second block");
        }
        line = line.substring(line.indexOf(OPEN_BRACE) + 1);
        while (line != null && line.trim().length() == 0) {
            line = blockReader.readLine();
        }
        if (line == null) {
            throw new MojoExecutionException("merge element doesn't have a correct second block");
        }
        int openParenIndex = line.indexOf(OPEN_PAREN);
        if (openParenIndex < 0) {
            throw new MojoExecutionException("second block in merge element doesn't have a correct () block");
        }
        Position blockPosition = new Position();
        blockPosition.line = line;
        blockPosition.index = openParenIndex;
        readBlock(blockReader, OPEN_PAREN, CLOSE_PAREN, blockPosition);
        extractBeforeAndAfter(record.toString(), amendments, 2, 3);
        record.reset();
        // process third block:
        blockPosition.index = 0;
        blockPosition.line = blockReader.readLine();
        while (blockPosition.line != null && blockPosition.line.trim().length() == 0) {
            blockPosition.line = blockReader.readLine();
        }
        openBraceIndex = blockPosition.line.indexOf(OPEN_BRACE);
        if (openBraceIndex > -1) {
            blockPosition.index = openBraceIndex;
            readBlock(blockReader, OPEN_BRACE, CLOSE_BRACE, blockPosition);
        } else {
            throw new MojoExecutionException("merge element doesn't have a second block");
        }
        extractBeforeAndAfter(record.toString(), amendments, 4, 5);
        record.reset();
    }

    private void extractBeforeAndAfter(String block, String[] amendments, int beforeIndex, int afterIndex) {
        int before = block.indexOf(BEFORE);
        int after = block.indexOf(AFTER);
        if (before >= 0) {
            // before exists
            amendments[beforeIndex] =
                    block.substring(before + BEFORE.length(), (after >= 0) ? after : block.length() - 1);
            if (amendments[beforeIndex].trim().length() == 0) {
                amendments[beforeIndex] = null;
            }
        }
        if (after >= 0) {
            // after exists
            amendments[afterIndex] = block.substring(after + AFTER.length(), block.length() - 1);
            if (amendments[afterIndex].trim().length() == 0) {
                amendments[afterIndex] = null;
            }
        }
    }

    private BufferedReader stringToReader(String aString) {
        InputStream is = new ByteArrayInputStream(aString.getBytes(StandardCharsets.UTF_8));
        return new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
    }

    private File prepareOutputFile() throws MojoExecutionException {
        // write output
        File outputFile = new File(output);
        if (outputFile.exists() && (!outputFile.delete())) {
            throw new MojoExecutionException("Unable to delete file " + output);
        }
        try {
            outputFile.getParentFile().mkdirs();
            if (!outputFile.createNewFile()) {
                throw new MojoExecutionException("Unable to create file " + output);
            }
        } catch (IOException ioe) {
            throw new MojoExecutionException("Unable to create file " + output, ioe);
        }
        return outputFile;
    }

    private String toSignature(String identifier) {
        StringBuilder aString = new StringBuilder();
        char[] chars = identifier.toCharArray();
        int index = 0;
        boolean first = true;
        while (index < chars.length) {
            int begin;
            while (Character.isWhitespace(chars[index])) {
                index++;
                if (index == chars.length) {
                    break;
                }
            }
            if (index == chars.length) {
                break;
            }
            begin = index;
            while (!Character.isWhitespace(chars[index])) {
                index++;
                if ((index == chars.length) || SIG_SPECIAL_CHARS.contains(chars[index - 1])
                        || ((index < chars.length - 1) && SIG_SPECIAL_CHARS.contains(chars[index]))) {
                    break;
                }
            }
            if (!first) {
                aString.append(' ');
            }
            aString.append(new String(chars, begin, index - begin));
            first = false;
        }
        return aString.toString();
    }

    /**
     * Literally replaces the given string.
     */
    private String stringReplaceAll(String baseStr, String oldPhrase, String newPhrase) {
        String resultStr = baseStr;
        String tempStr;
        int index = resultStr.indexOf(oldPhrase);
        while (index < resultStr.length()) {
            if (index < 0) {
                return resultStr;
            }
            tempStr = resultStr.substring(index, index + oldPhrase.length());
            if (tempStr.equals(oldPhrase)) {
                resultStr = resultStr.substring(0, index) + newPhrase + resultStr.substring(index + oldPhrase.length());
            }
            index = resultStr.indexOf(oldPhrase, index + 1);
        }
        return resultStr;
    }

    /**
     * Finds and returns the first occurrence index of a quote from startingIndex.
     *
     * @param baseStr
     * @param startingIndex
     * @return the substring if found. If not, this returns null.
     */
    private int findQuotePos(String baseStr, int startingIndex) {
        return baseStr.indexOf(ExternalDataConstants.QUOTE, startingIndex);
    }

}
