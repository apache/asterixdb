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
package org.apache.asterix.test.external_dataset;

import static org.apache.asterix.test.external_dataset.aws.AwsS3ExternalDatasetTest.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ExternalDatasetTestUtils {

    protected static final Logger LOGGER = LogManager.getLogger();

    // Base directory paths for data files
    private static String JSON_DATA_PATH;
    private static String CSV_DATA_PATH;
    private static String TSV_DATA_PATH;

    // IMPORTANT: The following values must be used in the AWS S3 test case
    // Region, container and definitions
    public static final String JSON_DEFINITION = "json-data/reviews/";
    public static final String CSV_DEFINITION = "csv-data/reviews/";
    public static final String TSV_DEFINITION = "tsv-data/reviews/";
    public static final String MIXED_DEFINITION = "mixed-data/reviews/";

    // This is used for a test to generate over 1000 number of files
    public static final String OVER_1000_OBJECTS_PATH = "over-1000-objects";
    public static final int OVER_1000_OBJECTS_COUNT = 2999;

    private static Uploader playgroundDataLoader;
    private static Uploader fixedDataLoader;
    private static Uploader mixedDataLoader;

    protected TestCaseContext tcCtx;

    public interface Uploader {
        default void upload(String key, String content) {
            upload(key, content, false, false);
        }

        void upload(String key, String content, boolean fromFile, boolean gzipped);
    }

    public ExternalDatasetTestUtils(TestCaseContext tcCtx) {
        this.tcCtx = tcCtx;
    }

    public static void setDataPaths(String jsonDataPath, String csvDataPath, String tsvDataPath) {
        JSON_DATA_PATH = jsonDataPath;
        CSV_DATA_PATH = csvDataPath;
        TSV_DATA_PATH = tsvDataPath;
    }

    public static void setUploaders(Uploader playgroundDataLoader, Uploader fixedDataLoader, Uploader mixedDataLoader) {
        ExternalDatasetTestUtils.playgroundDataLoader = playgroundDataLoader;
        ExternalDatasetTestUtils.fixedDataLoader = fixedDataLoader;
        ExternalDatasetTestUtils.mixedDataLoader = mixedDataLoader;
    }

    /**
     * Creates a bucket and fills it with some files for testing purpose.
     */
    public static void preparePlaygroundContainer() {
        LOGGER.info("Adding JSON files to the bucket");
        loadJsonFiles();
        LOGGER.info("JSON Files added successfully");

        LOGGER.info("Adding CSV files to the bucket");
        loadCsvFiles();
        LOGGER.info("CSV Files added successfully");

        LOGGER.info("Adding TSV files to the bucket");
        loadTsvFiles();
        LOGGER.info("TSV Files added successfully");

        LOGGER.info("Adding a big JSON file");
        loadBigJson();
        LOGGER.info("JSON file added successfully");

        LOGGER.info("Loading " + OVER_1000_OBJECTS_COUNT + " into " + OVER_1000_OBJECTS_PATH);
        loadLargeNumberOfFiles();
        LOGGER.info("Added " + OVER_1000_OBJECTS_COUNT + " files into " + OVER_1000_OBJECTS_PATH + " successfully");

        LOGGER.info("Files added successfully");
    }

    /**
     * This bucket is being filled by fixed data, a test is counting all records in this bucket. If this bucket is
     * changed, the test case will fail and its result will need to be updated each time
     */
    public static void prepareFixedDataContainer() {
        LOGGER.info("Loading fixed data to " + FIXED_DATA_CONTAINER);

        // Files data
        String path = Paths.get(JSON_DATA_PATH, "single-line", "20-records.json").toString();
        fixedDataLoader.upload("1.json", path, true, false);
        fixedDataLoader.upload("2.json", path, true, false);
        fixedDataLoader.upload("lvl1/3.json", path, true, false);
        fixedDataLoader.upload("lvl1/34.json", path, true, false);
        fixedDataLoader.upload("lvl1/lvl2/5.json", path, true, false);
    }

    public static void loadJsonFiles() {
        String dataBasePath = JSON_DATA_PATH;
        String definition = JSON_DEFINITION;

        // Normal format
        String definitionSegment = "json";
        loadData(dataBasePath, "single-line", "20-records.json", definition, definitionSegment, false);
        loadData(dataBasePath, "multi-lines", "20-records.json", definition, definitionSegment, false);
        loadData(dataBasePath, "multi-lines-with-arrays", "5-records.json", definition, definitionSegment, false);
        loadData(dataBasePath, "multi-lines-with-nested-objects", "5-records.json", definition, definitionSegment,
                false);

        definitionSegment = "json-array-of-objects";
        loadData(dataBasePath, "single-line", "array_of_objects.json", "json-data/", definitionSegment, false, false);

        // gz compressed format
        definitionSegment = "gz";
        loadGzData(dataBasePath, "single-line", "20-records.json", definition, definitionSegment, false);
        loadGzData(dataBasePath, "multi-lines", "20-records.json", definition, definitionSegment, false);
        loadGzData(dataBasePath, "multi-lines-with-arrays", "5-records.json", definition, definitionSegment, false);
        loadGzData(dataBasePath, "multi-lines-with-nested-objects", "5-records.json", definition, definitionSegment,
                false);

        // Mixed normal and gz compressed format
        definitionSegment = "mixed";
        loadData(dataBasePath, "single-line", "20-records.json", definition, definitionSegment, false);
        loadData(dataBasePath, "multi-lines", "20-records.json", definition, definitionSegment, false);
        loadData(dataBasePath, "multi-lines-with-arrays", "5-records.json", definition, definitionSegment, false);
        loadData(dataBasePath, "multi-lines-with-nested-objects", "5-records.json", definition, definitionSegment,
                false);
        loadGzData(dataBasePath, "single-line", "20-records.json", definition, definitionSegment, false);
        loadGzData(dataBasePath, "multi-lines", "20-records.json", definition, definitionSegment, false);
        loadGzData(dataBasePath, "multi-lines-with-arrays", "5-records.json", definition, definitionSegment, false);
        loadGzData(dataBasePath, "multi-lines-with-nested-objects", "5-records.json", definition, definitionSegment,
                false);
    }

    private static void loadCsvFiles() {
        String dataBasePath = CSV_DATA_PATH;
        String definition = CSV_DEFINITION;

        // Normal format
        String definitionSegment = "csv";
        loadData(dataBasePath, "", "01.csv", definition, definitionSegment, false);
        loadData(dataBasePath, "", "02.csv", definition, definitionSegment, false);

        // gz compressed format
        definitionSegment = "gz";
        loadGzData(dataBasePath, "", "01.csv", definition, definitionSegment, false);
        loadGzData(dataBasePath, "", "02.csv", definition, definitionSegment, false);

        // Mixed normal and gz compressed format
        definitionSegment = "mixed";
        loadData(dataBasePath, "", "01.csv", definition, definitionSegment, false);
        loadData(dataBasePath, "", "02.csv", definition, definitionSegment, false);
        loadGzData(dataBasePath, "", "01.csv", definition, definitionSegment, false);
        loadGzData(dataBasePath, "", "02.csv", definition, definitionSegment, false);
    }

    private static void loadTsvFiles() {
        String dataBasePath = TSV_DATA_PATH;
        String definition = TSV_DEFINITION;

        // Normal format
        String definitionSegment = "tsv";
        loadData(dataBasePath, "", "01.tsv", definition, definitionSegment, false);
        loadData(dataBasePath, "", "02.tsv", definition, definitionSegment, false);

        // gz compressed format
        definitionSegment = "gz";
        loadGzData(dataBasePath, "", "01.tsv", definition, definitionSegment, false);
        loadGzData(dataBasePath, "", "02.tsv", definition, definitionSegment, false);

        // Mixed normal and gz compressed format
        definitionSegment = "mixed";
        loadData(dataBasePath, "", "01.tsv", definition, definitionSegment, false);
        loadData(dataBasePath, "", "02.tsv", definition, definitionSegment, false);
        loadGzData(dataBasePath, "", "01.tsv", definition, definitionSegment, false);
        loadGzData(dataBasePath, "", "02.tsv", definition, definitionSegment, false);
    }

    private static void loadData(String fileBasePath, String filePathSegment, String filename, String definition,
            String definitionSegment, boolean removeExtension) {
        loadData(fileBasePath, filePathSegment, filename, definition, definitionSegment, removeExtension, true);
    }

    private static void loadData(String fileBasePath, String filePathSegment, String filename, String definition,
            String definitionSegment, boolean removeExtension, boolean copyToSubLevels) {
        // Files data
        Path filePath = Paths.get(fileBasePath, filePathSegment, filename);

        // Keep or remove the file extension
        Assert.assertFalse("Files with no extension are not supported yet for external datasets", removeExtension);
        String finalFileName;
        if (removeExtension) {
            finalFileName = FilenameUtils.removeExtension(filename);
        } else {
            finalFileName = filename;
        }

        // Files base definition
        filePathSegment = filePathSegment.isEmpty() ? "" : filePathSegment + "/";
        definitionSegment = definitionSegment.isEmpty() ? "" : definitionSegment + "/";
        String basePath = definition + filePathSegment + definitionSegment;

        // Load the data
        String path = filePath.toString();
        playgroundDataLoader.upload(basePath + finalFileName, path, true, false);
        if (copyToSubLevels) {
            playgroundDataLoader.upload(basePath + "level1a/" + finalFileName, path, true, false);
            playgroundDataLoader.upload(basePath + "level1b/" + finalFileName, path, true, false);
            playgroundDataLoader.upload(basePath + "level1a/level2a/" + finalFileName, path, true, false);
            playgroundDataLoader.upload(basePath + "level1a/level2b/" + finalFileName, path, true, false);
        }
    }

    private static void loadGzData(String fileBasePath, String filePathSegment, String filename, String definition,
            String definitionSegment, boolean removeExtension) {
        // Keep or remove the file extension
        Assert.assertFalse("Files with no extension are not supported yet for external datasets", removeExtension);
        String finalFileName;
        if (removeExtension) {
            finalFileName = FilenameUtils.removeExtension(filename);
        } else {
            finalFileName = filename;
        }
        finalFileName += ".gz";

        // Files base definition
        filePathSegment = filePathSegment.isEmpty() ? "" : filePathSegment + "/";
        definitionSegment = definitionSegment.isEmpty() ? "" : definitionSegment + "/";
        String basePath = definition + filePathSegment + definitionSegment;

        // Load the data
        String path = Paths.get(fileBasePath, filePathSegment, filename).toString();
        playgroundDataLoader.upload(basePath + finalFileName, path, true, true);
        playgroundDataLoader.upload(basePath + "level1a/" + finalFileName, path, true, true);
        playgroundDataLoader.upload(basePath + "level1b/" + finalFileName, path, true, true);
        playgroundDataLoader.upload(basePath + "level1a/level2a/" + finalFileName, path, true, true);
        playgroundDataLoader.upload(basePath + "level1a/level2b/" + finalFileName, path, true, true);
    }

    private static void loadBigJson() {
        String fileName = "big_record.json";
        int bufferSize = 4 * 1024 * 1024;
        int maxSize = bufferSize * 9;
        Path filePath = Paths.get("target", "rttest", "tmp", fileName);
        try {
            if (Files.notExists(filePath)) {
                Files.createDirectories(filePath.getParent());
                Files.createFile(filePath);
            }
        } catch (IOException ex) {
            throw new IllegalStateException("File " + fileName + " not found");
        }

        try (FileWriter writer = new FileWriter(filePath.toFile(), false);
                BufferedWriter bw = new BufferedWriter(writer, bufferSize)) {
            bw.append("{ \"large_field\": \"");
            for (int i = 0; i < maxSize; i++) {
                bw.append('A');
            }
            bw.append("\" }");
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        String key = "big-json/" + fileName;
        playgroundDataLoader.upload(key, filePath.toString(), true, false);
    }

    /**
     * Generates over 1000 objects and upload them to S3 mock server, 1 record per object
     */
    private static void loadLargeNumberOfFiles() {
        for (int i = 0; i < OVER_1000_OBJECTS_COUNT; i++) {
            playgroundDataLoader.upload(OVER_1000_OBJECTS_PATH + "/" + i + ".json", "{\"id\":" + i + "}");
        }
    }

    /**
     * Loads a combination of different file formats in the same path
     */
    public static void prepareMixedDataContainer() {
        // JSON
        mixedDataLoader.upload(MIXED_DEFINITION + "json/extension/" + "hello-world-2018.json", "{\"id\":" + 1 + "}");
        mixedDataLoader.upload(MIXED_DEFINITION + "json/extension/" + "hello-world-2019.json", "{\"id\":" + 2 + "}");
        mixedDataLoader.upload(MIXED_DEFINITION + "json/extension/" + "hello-world-2020.json", "{\"id\":" + 3 + "}");
        mixedDataLoader.upload(MIXED_DEFINITION + "json/EXTENSION/" + "goodbye-world-2018.json", "{\"id\":" + 4 + "}");
        mixedDataLoader.upload(MIXED_DEFINITION + "json/EXTENSION/" + "goodbye-world-2019.json", "{\"id\":" + 5 + "}");
        mixedDataLoader.upload(MIXED_DEFINITION + "json/EXTENSION/" + "goodbye-world-2020.json", "{\"id\":" + 6 + "}");

        // CSV
        mixedDataLoader.upload(MIXED_DEFINITION + "csv/extension/" + "hello-world-2018.csv", "7,\"good\"");
        mixedDataLoader.upload(MIXED_DEFINITION + "csv/extension/" + "hello-world-2019.csv", "8,\"good\"");
        mixedDataLoader.upload(MIXED_DEFINITION + "csv/extension/" + "hello-world-2020.csv", "9,\"good\"");
        mixedDataLoader.upload(MIXED_DEFINITION + "csv/EXTENSION/" + "goodbye-world-2018.csv", "10,\"good\"");
        mixedDataLoader.upload(MIXED_DEFINITION + "csv/EXTENSION/" + "goodbye-world-2019.csv", "11,\"good\"");
        mixedDataLoader.upload(MIXED_DEFINITION + "csv/EXTENSION/" + "goodbye-world-2020.csv", "12,\"good\"");

        // TSV
        mixedDataLoader.upload(MIXED_DEFINITION + "tsv/extension/" + "hello-world-2018.tsv", "13\t\"good\"");
        mixedDataLoader.upload(MIXED_DEFINITION + "tsv/extension/" + "hello-world-2019.tsv", "14\t\"good\"");
        mixedDataLoader.upload(MIXED_DEFINITION + "tsv/extension/" + "hello-world-2020.tsv", "15\t\"good\"");
        mixedDataLoader.upload(MIXED_DEFINITION + "tsv/EXTENSION/" + "goodbye-world-2018.tsv", "16\t\"good\"");
        mixedDataLoader.upload(MIXED_DEFINITION + "tsv/EXTENSION/" + "goodbye-world-2019.tsv", "17\t\"good\"");
        mixedDataLoader.upload(MIXED_DEFINITION + "tsv/EXTENSION/" + "goodbye-world-2020.tsv", "18\t\"good\"");

        // JSON no extension
        mixedDataLoader.upload(MIXED_DEFINITION + "json/no-extension/" + "hello-world-2018", "{\"id\":" + 1 + "}");
        mixedDataLoader.upload(MIXED_DEFINITION + "json/no-extension/" + "hello-world-2019", "{\"id\":" + 2 + "}");
        mixedDataLoader.upload(MIXED_DEFINITION + "json/no-extension/" + "hello-world-2020", "{\"id\":" + 3 + "}");
        mixedDataLoader.upload(MIXED_DEFINITION + "json/NO-EXTENSION/" + "goodbye-world-2018", "{\"id\":" + 4 + "}");
        mixedDataLoader.upload(MIXED_DEFINITION + "json/NO-EXTENSION/" + "goodbye-world-2019", "{\"id\":" + 5 + "}");
        mixedDataLoader.upload(MIXED_DEFINITION + "json/NO-EXTENSION/" + "goodbye-world-2020", "{\"id\":" + 6 + "}");
    }
}