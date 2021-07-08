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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.fs.Path;
import org.apache.hyracks.api.util.IoUtil;
import org.kitesdk.data.spi.JsonUtil;
import org.kitesdk.data.spi.filesystem.JSONFileReader;

import parquet.avro.AvroParquetWriter;

public class BinaryFileConverterUtil {
    public static final String DEFAULT_PARQUET_SRC_PATH = "data/hdfs/parquet";
    public static final String BINARY_GEN_BASEDIR = "target" + File.separatorChar + "generated_bin_files";

    //How many records should the schema inference method inspect to infer the schema for parquet files
    private static final int NUM_OF_RECORDS_SCHEMA = 20;

    private BinaryFileConverterUtil() {
    }

    public static void cleanBinaryDirectory(File localDataRoot, String binaryFilesPath) throws IOException {
        File destPath = new File(localDataRoot, binaryFilesPath);
        //Delete old generated files
        if (destPath.exists()) {
            IoUtil.delete(destPath);
        }
        //Create new directory
        Files.createDirectory(Paths.get(destPath.getAbsolutePath()));
    }

    public static void convertToParquet(File localDataRoot, String src, String dest) throws IOException {
        File srcPath = new File(localDataRoot, src);
        File destPath = new File(localDataRoot, dest);

        //Write parquet files
        File[] listOfFiles = srcPath.listFiles();
        for (File jsonFile : listOfFiles) {
            String fileName = jsonFile.getName().substring(0, jsonFile.getName().indexOf(".")) + ".parquet";
            Path outputPath = new Path(destPath.getAbsolutePath(), fileName);
            writeParquetFile(jsonFile, outputPath);
        }
    }

    private static void writeParquetFile(File jsonInputPath, Path parquetOutputPath) throws IOException {
        final FileInputStream schemaInputStream = new FileInputStream(jsonInputPath);
        final FileInputStream jsonInputStream = new FileInputStream(jsonInputPath);
        //Infer Avro schema
        final Schema inputSchema = JsonUtil.inferSchema(schemaInputStream, "parquet_schema", NUM_OF_RECORDS_SCHEMA);
        try (JSONFileReader<Record> reader = new JSONFileReader<>(jsonInputStream, inputSchema, Record.class)) {
            reader.initialize();
            try (AvroParquetWriter<Record> writer = new AvroParquetWriter<>(parquetOutputPath, inputSchema)) {
                for (Record record : reader) {
                    writer.write(record);
                }
            }
        }
    }
}
