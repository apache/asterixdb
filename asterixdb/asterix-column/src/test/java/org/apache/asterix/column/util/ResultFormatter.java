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
package org.apache.asterix.column.util;

import static org.apache.asterix.column.common.test.TestBase.DATA_PATH;
import static org.apache.asterix.column.common.test.TestBase.OUTPUT_PATH;
import static org.apache.asterix.column.common.test.TestBase.listFiles;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.asterix.external.parser.JSONDataParser;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.pointables.printer.json.clean.APrintVisitor;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.util.IoUtil;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

import com.fasterxml.jackson.core.JsonFactory;

/**
 * A simple tool that helps to format manually written result JSON files
 * The formatted result files are required to when matching expected results with actual results
 * For example:
 * Input:
 * {"b":[[1,2,3],[4, 5,6]]}
 * Output:
 * {"b": [[1, 2, 3], [4, 5, 6]]}
 */
public class ResultFormatter {
    private final APrintVisitor printVisitor;
    private final ARecordVisitablePointable recordPointable;
    private final JSONDataParser parser;
    private final ArrayBackedValueStorage storage;

    private ResultFormatter() {
        printVisitor = new APrintVisitor();
        recordPointable = new ARecordVisitablePointable(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
        JsonFactory jsonFactory = new JsonFactory();
        parser = new JSONDataParser(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE, jsonFactory);
        storage = new ArrayBackedValueStorage();
    }

    private void prepareParser(File testFile) throws IOException {
        //Prepare parser
        FileInputStream inputStream = new FileInputStream(testFile);
        parser.reset(inputStream);
        storage.reset();
    }

    private void format(File dataPath, File resultPath) throws IOException {
        prepareParser(dataPath);
        try (PrintStream ps = new PrintStream(new FileOutputStream(resultPath))) {
            Pair<PrintStream, ATypeTag> pair = new Pair<>(ps, ATypeTag.OBJECT);
            while (parser.parse(storage.getDataOutput())) {
                recordPointable.set(storage);
                recordPointable.accept(printVisitor, pair);
                ps.println();
                storage.reset();
            }
        }

    }

    private static void setUp(File path) throws IOException {
        if (!OUTPUT_PATH.exists()) {
            Files.createDirectory(Paths.get(OUTPUT_PATH.toURI()));
        }
        if (path.exists()) {
            IoUtil.delete(path);
        }
        Files.createDirectory(Paths.get(path.toURI()));
    }

    public static void main(String[] args) throws IOException {
        ResultFormatter formatter = new ResultFormatter();
        File path = new File(OUTPUT_PATH, "formatter");
        setUp(path);
        for (File file : listFiles(DATA_PATH)) {
            formatter.format(file, new File(OUTPUT_PATH, file.getName()));
        }
    }
}
