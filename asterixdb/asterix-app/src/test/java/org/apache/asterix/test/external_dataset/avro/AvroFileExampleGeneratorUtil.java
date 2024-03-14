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
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Test;

public class AvroFileExampleGeneratorUtil {
    private static final String SCHEMA_STRING = "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"SimpleRecord\",\n"
            + "  \"namespace\": \"com.example\",\n" + "  \"fields\": [\n" + "    {\n"
            + "      \"name\": \"unionField\",\n" + "      \"type\": [\"int\", \"string\", \"bytes\"],\n"
            + "      \"doc\": \"This field can be either an int or a string.\"\n" + "    },\n" + "    {\n"
            + "      \"name\": \"mapField\",\n" + "      \"type\": {\n" + "        \"type\": \"map\",\n"
            + "        \"values\": \"int\",\n" + "        \"doc\": \"This is a map of string keys to int values.\"\n"
            + "      },\n" + "      \"doc\": \"This field represents a map with string keys and integer values.\"\n"
            + "    },\n" + "    {\n" + "      \"name\": \"nestedRecord\",\n" + "      \"type\": {\n"
            + "        \"type\": \"record\",\n" + "        \"name\": \"NestedRecord\",\n" + "        \"fields\": [\n"
            + "          {\n" + "            \"name\": \"nestedInt\",\n" + "            \"type\": \"int\"\n"
            + "          },\n" + "          {\n" + "            \"name\": \"nestedString\",\n"
            + "            \"type\": \"string\"\n" + "          }\n" + "        ]\n" + "      },\n"
            + "      \"doc\": \"This is a nested record.\"\n" + "    },\n" + "    {\n"
            + "      \"name\": \"booleanField\",\n" + "      \"type\": \"boolean\",\n"
            + "      \"doc\": \"This is a boolean field.\"\n" + "    },\n" + "    {\n"
            + "      \"name\": \"intField\",\n" + "      \"type\": \"int\",\n"
            + "      \"doc\": \"This is an int field.\"\n" + "    },\n" + "    {\n" + "      \"name\": \"longField\",\n"
            + "      \"type\": \"long\",\n" + "      \"doc\": \"This is a long field.\"\n" + "    },\n" + "    {\n"
            + "      \"name\": \"floatField\",\n" + "      \"type\": \"float\",\n"
            + "      \"doc\": \"This is a float field.\"\n" + "    },\n" + "    {\n"
            + "      \"name\": \"doubleField\",\n" + "      \"type\": \"double\",\n"
            + "      \"doc\": \"This is a double field.\"\n" + "    },\n" + "    {\n"
            + "      \"name\": \"bytesField\",\n" + "      \"type\": \"bytes\",\n"
            + "      \"doc\": \"This is a bytes field.\"\n" + "    },\n" + "    {\n"
            + "      \"name\": \"stringField\",\n" + "      \"type\": \"string\",\n"
            + "      \"doc\": \"This is a string field.\"\n" + "    }\n" + "  ]\n" + "}\n";

    private static final String AVRO_GEN_BASEDIR = "target/generated_avro_files";
    private static final String FILE_NAME = "avro_type.avro";

    public static void writeExample() throws IOException {
        Schema schema = new Schema.Parser().parse(SCHEMA_STRING);
        File destPath = new File(AVRO_GEN_BASEDIR);
        File outputFile = new File(destPath, FILE_NAME);

        DatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter<>(schema);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(schema, outputFile);

            // First record with unionField as int
            GenericRecord nestedRecord = new GenericData.Record(schema.getField("nestedRecord").schema());
            nestedRecord.put("nestedInt", 100);
            nestedRecord.put("nestedString", "Inside Nested");

            // First record with various fields
            GenericRecord record = new GenericData.Record(schema);
            record.put("unionField", 42);
            Map<String, Integer> map = new HashMap<>();
            map.put("key1", 1);
            map.put("key2", 2);
            record.put("mapField", map);
            record.put("nestedRecord", nestedRecord);
            record.put("booleanField", true);
            record.put("intField", 32);
            record.put("longField", 64L);
            record.put("floatField", 1.0f);
            record.put("doubleField", 2.0);
            record.put("bytesField", ByteBuffer.wrap(new byte[] { 0x01, 0x02 }));
            record.put("stringField", "Example string");
            dataFileWriter.append(record);

            //second record to be added
            GenericRecord record2 = new GenericData.Record(schema);
            record2.put("unionField", ByteBuffer.wrap(new byte[] { 0x01, 0x05 }));
            Map<String, Integer> map2 = new HashMap<>();
            map2.put("key3", 3);
            map2.put("key4", 4);
            record2.put("mapField", map2);
            record2.put("nestedRecord", nestedRecord);
            record2.put("booleanField", false);
            record2.put("intField", 54);
            record2.put("longField", 60L);
            record2.put("floatField", 3.6f);
            record2.put("doubleField", 5.77777);
            record2.put("bytesField", ByteBuffer.wrap(new byte[] { 0x06, 0x04 }));
            record2.put("stringField", "Sample Values");
            dataFileWriter.append(record2);
        } catch (IOException e) {
            System.err.println("Failed to write AVRO file: " + e.getMessage());
            e.printStackTrace();
        }
    }

    @Test
    public void main() throws IOException {
        writeExample();
    }
}
