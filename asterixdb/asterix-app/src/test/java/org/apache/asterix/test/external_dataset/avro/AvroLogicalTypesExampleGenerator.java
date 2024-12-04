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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Test;

public class AvroLogicalTypesExampleGenerator {
    private static final String SCHEMA_STRING = "{\n" + "  \"type\": \"record\",\n"
            + "  \"name\": \"LogicalTypesRecord\",\n" + "  \"namespace\": \"com.example\",\n" + "  \"fields\": [\n"
            + "    { \"name\": \"decimalField\", \"type\": { \"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 10, \"scale\": 2 } },\n"
            + "    { \"name\": \"uuidField\", \"type\": { \"type\": \"string\", \"logicalType\": \"uuid\" } },\n"
            + "    { \"name\": \"dateField\", \"type\": { \"type\": \"int\", \"logicalType\": \"date\" } },\n"
            + "    { \"name\": \"timeMillisField\", \"type\": { \"type\": \"int\", \"logicalType\": \"time-millis\" } },\n"
            + "    { \"name\": \"timeMicrosField\", \"type\": { \"type\": \"long\", \"logicalType\": \"time-micros\" } },\n"
            + "    { \"name\": \"timestampMillisField\", \"type\": { \"type\": \"long\", \"logicalType\": \"timestamp-millis\" } },\n"
            + "    { \"name\": \"timestampMicrosField\", \"type\": { \"type\": \"long\", \"logicalType\": \"timestamp-micros\" } },\n"
            + "    { \"name\": \"localTimestampMillisField\", \"type\": { \"type\": \"long\", \"logicalType\": \"local-timestamp-millis\" } },\n"
            + "    { \"name\": \"localTimestampMicrosField\", \"type\": { \"type\": \"long\", \"logicalType\": \"local-timestamp-micros\" } }\n"
            + "  ]\n" + "}";

    private static final String AVRO_GEN_BASEDIR = "target/generated_avro_files";
    private static final String FILE_NAME = "avro_logical_type.avro";

    public static void writeLogicalTypesExample() throws IOException {
        Schema schema = new Schema.Parser().parse(SCHEMA_STRING);
        File destPath = new File(AVRO_GEN_BASEDIR);
        if (!destPath.exists()) {
            destPath.mkdirs();
        }
        File outputFile = new File(destPath, FILE_NAME);

        DatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter<>(schema);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(schema, outputFile);

            // First record
            GenericRecord record = new GenericData.Record(schema);
            record.put("decimalField", ByteBuffer.wrap(new byte[] { 0x01, 0x02 }));
            record.put("uuidField", "123e4567-e89b-12d3-a456-426614174000");
            record.put("dateField", 20061);
            record.put("timeMillisField", 12345678);
            record.put("timeMicrosField", 12345678901234L);
            record.put("timestampMillisField", 1733344079083L);
            record.put("timestampMicrosField", 1733344079083000L);
            record.put("localTimestampMillisField", 1733344079083L);
            record.put("localTimestampMicrosField", 1733344079083000L);

            dataFileWriter.append(record);
        } catch (IOException e) {
            System.err.println("Failed to write AVRO file: " + e.getMessage());
            e.printStackTrace();
        }
    }

    @Test
    public void main() throws IOException {
        writeLogicalTypesExample();
    }
}
