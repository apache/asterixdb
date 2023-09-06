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
package org.apache.asterix.test.external_dataset.parquet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.fs.Path;
import org.apache.hyracks.api.util.IoUtil;
import org.apache.parquet.avro.AvroParquetWriter;

import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

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
        //Write parquet example that contains the specialized types
        ParquetFileExampleGeneratorUtil.writeExample();
    }

    public static void convertToParquetRecursively(File localDataRoot, String src, String dest, FilenameFilter filter,
            int startIndex) throws IOException {
        File destPath = new File(localDataRoot, dest);

        File dir = new File(src);
        if (!dir.exists() || !dir.isDirectory()) {
            return;
        }

        Collection<File> files = IoUtil.getMatchingFiles(dir.toPath(), filter);
        for (File file : files) {
            String fileName = file.getName().substring(0, file.getName().indexOf(".")) + ".parquet";
            Path outputPath = new Path(
                    Paths.get(destPath.getAbsolutePath(), file.getParent().substring(startIndex), fileName).toString());
            writeParquetFile(file, outputPath);
        }
    }

    private static void writeParquetFile(File jsonInputPath, Path parquetOutputPath) throws IOException {
        FileInputStream schemaInputStream = new FileInputStream(jsonInputPath);
        //Infer Avro schema
        Schema inputSchema = JsonUtil.inferSchema(schemaInputStream, "parquet_schema", NUM_OF_RECORDS_SCHEMA);
        try (BufferedReader reader = new BufferedReader(new FileReader(jsonInputPath));
                AvroParquetWriter<Record> writer = new AvroParquetWriter<>(parquetOutputPath, inputSchema)) {
            JsonAvroConverter converter = new JsonAvroConverter();
            String line;
            while ((line = reader.readLine()) != null) {
                Record record = converter.convertToGenericDataRecord(line.getBytes(), inputSchema);
                writer.write(record);
            }
        }
    }
}
