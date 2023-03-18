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
package org.apache.asterix.column.common.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.asterix.external.parser.JSONDataParser;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.IoUtil;

import com.fasterxml.jackson.core.JsonFactory;

public abstract class TestBase {
    public static final File OUTPUT_PATH;
    public static final File DATA_PATH;

    private static final File TESTS;
    private static final File RESULT_PATH;
    private static final Map<Class<?>, TestPath> TEST_PATH_MAP;

    protected final TestCase testCase;
    protected final JSONDataParser parser;

    static {
        TEST_PATH_MAP = new HashMap<>();

        ClassLoader classLoader = TestBase.class.getClassLoader();
        OUTPUT_PATH = new File("target", "result");
        TESTS = new File(Objects.requireNonNull(classLoader.getResource("only.txt")).getPath());
        DATA_PATH = new File(Objects.requireNonNull(classLoader.getResource("data")).getPath());
        RESULT_PATH = new File(Objects.requireNonNull(classLoader.getResource("result")).getPath());
    }

    protected TestBase(TestCase testCase) throws HyracksDataException {
        this.testCase = testCase;
        JsonFactory jsonFactory = new JsonFactory();
        parser = new JSONDataParser(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE, jsonFactory);
    }

    protected void prepareParser(File testFile) throws IOException {
        //Prepare parser
        FileInputStream inputStream = new FileInputStream(testFile);
        parser.reset(inputStream);
    }

    protected static void setup(Class<?> clazz) throws IOException {
        TestPath path = TEST_PATH_MAP.get(clazz);
        if (!OUTPUT_PATH.exists()) {
            Files.createDirectory(Paths.get(OUTPUT_PATH.toURI()));
        }
        if (path.outputPath.exists()) {
            IoUtil.delete(path.outputPath);
        }
        Files.createDirectory(Paths.get(path.outputPath.toURI()));
    }

    protected static Collection<Object[]> initTests(Class<?> clazz, String testName) throws Exception {
        TestPath path = TEST_PATH_MAP.computeIfAbsent(clazz,
                k -> new TestPath(new File(OUTPUT_PATH, testName), new File(RESULT_PATH, testName)));
        Set<String> only = getOnly();
        List<File> testFiles = listFiles(DATA_PATH, only);
        List<File> resultFiles = listFiles(path.resultPath, only);

        List<Object[]> testCases = new ArrayList<>();
        for (int i = 0; i < testFiles.size(); i++) {
            Object[] testCase = { new TestCase(testFiles.get(i), resultFiles.get(i), path.outputPath) };
            testCases.add(testCase);
        }
        return testCases;
    }

    public static List<File> listFiles(File path) throws IOException {
        return listFiles(path, Collections.emptySet());
    }

    private static List<File> listFiles(File path, Set<String> only) throws IOException {
        Predicate<File> predicate = f -> only.isEmpty() || only.contains(f.getName().split("\\.")[0]);
        return Files.list(Paths.get(path.toURI())).map(Path::toFile).filter(predicate).sorted(File::compareTo)
                .collect(Collectors.toList());
    }

    private static Set<String> getOnly() throws FileNotFoundException {
        BufferedReader reader = new BufferedReader(new FileReader(TESTS));
        return reader.lines().filter(l -> !l.trim().isEmpty() && l.charAt(0) != '#').collect(Collectors.toSet());
    }

    private static class TestPath {
        private final File outputPath;
        private final File resultPath;

        TestPath(File outputPath, File resultPath) {
            this.outputPath = outputPath;
            this.resultPath = resultPath;
        }
    }
}
