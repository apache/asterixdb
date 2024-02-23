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
package org.apache.asterix.test.external_dataset.avro;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collection;

import org.apache.asterix.test.external_dataset.parquet.JsonUtil;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hyracks.api.util.IoUtil;

import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

public class AvroFileConverterUtil {

    public static final String DEFAULT_PARQUET_SRC_PATH = "data/hdfs/parquet";
    public static final String AVRO_GEN_BASEDIR = "target" + File.separatorChar + "generated_avro_files";

    //How many records should the schema inference method inspect to infer the schema for parquet files
    private static final int NUM_OF_RECORDS_SCHEMA = 20;

    private AvroFileConverterUtil() {
    }

    private static void convertToJsonAndWriteAvro(File jsonFile, Schema schema, Path avroFilePath) throws IOException {
        File outputFile = new File(avroFilePath.toString());
        File parentDir = outputFile.getParentFile();
        if (!parentDir.exists() && !parentDir.mkdirs()) {
            throw new IOException("Failed to create directory " + parentDir);
        }
        GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(schema, new File(avroFilePath.toString()));
            JsonAvroConverter converter = new JsonAvroConverter();
            try (BufferedReader reader = new BufferedReader(new FileReader(jsonFile))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    GenericRecord record = converter.convertToGenericDataRecord(line.getBytes(), schema);
                    dataFileWriter.append(record);
                }
            } catch (DataFileWriter.AppendWriteException e) {
                System.err.println("Failed to append record to Avro file: " + e.getMessage());
            }
        }
    }

    public static void writeAvroFile(File jsonFile, Path avroPath) throws IOException {
        FileInputStream schemaInputStream = new FileInputStream(jsonFile);
        Schema schema = JsonUtil.inferSchema(schemaInputStream, "avro_schema", NUM_OF_RECORDS_SCHEMA);
        convertToJsonAndWriteAvro(jsonFile, schema, avroPath);
    }

    public static void convertToAvroRecursively(File localDataRoot, String src, String dest, FilenameFilter filter,
            int startIndex) throws IOException {
        File destPath = new File(localDataRoot, dest);

        File dir = new File(src);
        if (!dir.exists() || !dir.isDirectory()) {
            return;
        }

        Collection<File> files = IoUtil.getMatchingFiles(dir.toPath(), filter);
        for (File file : files) {
            String fileName = file.getName().substring(0, file.getName().indexOf(".")) + ".avro";
            Path outputPath = new Path(
                    Paths.get(destPath.getAbsolutePath(), file.getParent().substring(startIndex), fileName).toString());

            writeAvroFile(file, outputPath);
        }
    }

    public static void convertToAvro(File localDataRoot, String src, String dest) throws IOException {
        File srcPath = new File(localDataRoot, src);
        File destPath = new File(localDataRoot, dest);

        //write avro files
        File[] listOfFiles = srcPath.listFiles();
        for (File jsonFile : listOfFiles) {
            String fileName = jsonFile.getName().substring(0, jsonFile.getName().indexOf(".")) + ".avro";
            Path outputPath = new Path(destPath.getAbsolutePath(), fileName);
            writeAvroFile(jsonFile, outputPath);
        }
        AvroFileExampleGeneratorUtil.writeExample();
    }
}
