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

import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.dataflow.data.nontagged.serde.AUUIDSerializerDeserializer;
import org.apache.asterix.om.base.AMutableUUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;

/**
 * A generator of a parquet file that contains different specialized type
 * Adopted from:
 *
 * @see <a href="https://github.com/apache/parquet-mr/blob/master/parquet-hadoop/src/test/java/org/apache/parquet/hadoop/TestParquetWriter.java">TestParquetWriter</a>
 */
public class ParquetFileExampleGeneratorUtil {
    //Jan 1st 2022 01:00:00 UTC
    private static final long TIME_MILLIS = TimeUnit.SECONDS.toMillis(1640998800);
    private static final int TIME_DAYS = (int) TimeUnit.MILLISECONDS.toDays(TIME_MILLIS);
    private static final int SINCE_MIDNIGHT_MILLIS = getSecondsSinceMidnight();

    private static final int PST_OFFSET = TimeZone.getTimeZone("PST").getRawOffset();
    private static final long PST_TIME_MILLIS = TimeUnit.SECONDS.toMillis(1640998800) + PST_OFFSET;
    private static final int PST_TIME_DAYS = (int) TimeUnit.MILLISECONDS.toDays(PST_TIME_MILLIS);
    private static final int PST_SINCE_MIDNIGHT_MILLIS = SINCE_MIDNIGHT_MILLIS + PST_OFFSET;
    private static final int JULIAN_DAY_OF_EPOCH = 2440588;

    private static final String FILE_NAME = "parquetTypes.parquet";

    private static final String SCHEMA = "message test { \n" + "   required boolean boolean_field;\n"
            + "   required int32 int8_field (INTEGER(8,true));\n"
            + "   required int32 int16_field (INTEGER(16,true));\n" + "   required int32 int32_field;\n"
            + "   required int64 int64_field;\n" + "   required int32 uint8_field (INTEGER(8,false));\n"
            + "   required int32 uint16_field (INTEGER(16,false));\n"
            + "   required int32 uint32_field (INTEGER(32,false));\n"
            + "   required int64 uint64_field (INTEGER(64,false));\n"
            + "   required int64 overflowed_uint64_field (INTEGER(64,false));\n" + "   required float float_field;\n"
            + "   required double double_field;\n" + "   required int32 decimal32_field (DECIMAL(5, 4));\n"
            + "   required int64 decimal64_field (DECIMAL(12, 9));\n"
            + "   required fixed_len_byte_array(10) decimal_fixed80_field (DECIMAL(22,21));\n"
            + "   required binary decimal_arbitrary_length_field (DECIMAL(22,21));\n"
            + "   required binary binary_field;\n" + "   required binary string_field (UTF8);\n"
            + "   required binary enum_field (ENUM);\n" + "   required binary json_field (JSON);\n"
            + "   required int32 date_field (DATE);\n" + "   required int32 time32_millis_field (TIME(MILLIS, true));\n"
            + "   required int64 time64_micros_field (TIME(MICROS, true));\n"
            + "   required int64 time64_nanos_field (TIME(NANOS, true));\n"
            + "   required int32 time32_millis_pst_field (TIME(MILLIS, false));\n"
            + "   required int64 time64_micros_pst_field (TIME(MICROS, false));\n"
            + "   required int64 time64_nanos_pst_field (TIME(NANOS, false));\n"
            + "   required int64 timestamp64_millis_field (TIMESTAMP(MILLIS, true));\n"
            + "   required int64 timestamp64_micros_field (TIMESTAMP(MICROS, true));\n"
            + "   required int64 timestamp64_nanos_field (TIMESTAMP(NANOS, true));\n"
            + "   required int64 timestamp64_millis_pst_field (TIMESTAMP(MILLIS, false));\n"
            + "   required int64 timestamp64_micros_pst_field (TIMESTAMP(MICROS, false));\n"
            + "   required int64 timestamp64_nanos_pst_field (TIMESTAMP(NANOS, false));\n"
            + "   required int96 timestamp96_field;\n" + "   required fixed_len_byte_array(16) uuid_field (UUID);"
            + "     required group mapField (MAP) {\n" + "   repeated group key_value {\n"
            + "     required int32 key;\n" + "     required int32 value;\n" + "   }\n" + " }" + "}";

    private ParquetFileExampleGeneratorUtil() {
    }

    public static void writeExample() throws IOException {
        Configuration conf = new Configuration();
        Path root = new Path(BinaryFileConverterUtil.BINARY_GEN_BASEDIR);
        MessageType schema = parseMessageType(SCHEMA);
        GroupWriteSupport.setSchema(schema, conf);
        Path file = new Path(root, FILE_NAME);
        ParquetWriter<Group> writer = ExampleParquetWriter.builder(new TestOutputFile(file, conf))
                .withCompressionCodec(UNCOMPRESSED).withRowGroupSize(1024).withPageSize(1024)
                .withDictionaryPageSize(512).enableDictionaryEncoding().withValidation(false)
                .withWriterVersion(WriterVersion.PARQUET_2_0).withConf(conf).build();
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        Group message = groupFactory.newGroup().append("boolean_field", true).append("int8_field", 8)
                .append("int16_field", 16).append("int32_field", 32).append("int64_field", 64L)
                .append("uint8_field", Byte.MAX_VALUE + 1).append("uint16_field", Short.MAX_VALUE + 1)
                .append("uint32_field", Integer.MAX_VALUE + 1).append("uint64_field", 151L)
                .append("overflowed_uint64_field", Long.MAX_VALUE + 1).append("float_field", 1.0F)
                .append("double_field", 1.0D).append("decimal32_field", getDecimal32())
                .append("decimal64_field", getDecimal64()).append("decimal_fixed80_field", getDecimal80())
                .append("decimal_arbitrary_length_field", getDecimal80()).append("binary_field", createConstantBinary())
                .append("string_field", "stringVal").append("enum_field", "enumVal").append("json_field", "[1,2,3]")
                .append("date_field", TIME_DAYS).append("time32_millis_field", SINCE_MIDNIGHT_MILLIS)
                .append("time64_micros_field", TimeUnit.MILLISECONDS.toMicros(SINCE_MIDNIGHT_MILLIS))
                .append("time64_nanos_field", TimeUnit.MILLISECONDS.toNanos(SINCE_MIDNIGHT_MILLIS))
                .append("time32_millis_pst_field", PST_SINCE_MIDNIGHT_MILLIS)
                .append("time64_micros_pst_field", TimeUnit.MILLISECONDS.toMicros(PST_SINCE_MIDNIGHT_MILLIS))
                .append("time64_nanos_pst_field", TimeUnit.MILLISECONDS.toNanos(PST_SINCE_MIDNIGHT_MILLIS))
                .append("timestamp64_millis_field", TIME_MILLIS)
                .append("timestamp64_micros_field", TimeUnit.MILLISECONDS.toMicros(TIME_MILLIS))
                .append("timestamp64_nanos_field", TimeUnit.MILLISECONDS.toNanos(TIME_MILLIS))
                .append("timestamp64_millis_pst_field", PST_TIME_MILLIS)
                .append("timestamp64_micros_pst_field", TimeUnit.MILLISECONDS.toMicros(PST_TIME_MILLIS))
                .append("timestamp64_nanos_pst_field", TimeUnit.MILLISECONDS.toNanos(PST_TIME_MILLIS))
                .append("timestamp96_field",
                        new NanoTime(PST_TIME_DAYS + JULIAN_DAY_OF_EPOCH,
                                TimeUnit.MILLISECONDS.toNanos(PST_SINCE_MIDNIGHT_MILLIS)))
                .append("uuid_field", createUUIDBinary());
        Group mapField = message.addGroup("mapField");
        mapField.addGroup("key_value").append("key", 1).append("value", 1);
        writer.write(message);
        writer.close();
    }

    private static int getSecondsSinceMidnight() {
        Instant instant = Instant.ofEpochMilli(TIME_MILLIS);
        Instant midnight = LocalDate.ofInstant(instant, ZoneOffset.UTC).atStartOfDay().toInstant(ZoneOffset.UTC);
        return (int) Duration.between(midnight, instant).toMillis();
    }

    private static int getDecimal32() {
        BigDecimal decimal = new BigDecimal("1.1000");
        return decimal.unscaledValue().intValue();
    }

    private static long getDecimal64() {
        BigDecimal decimal = new BigDecimal("154.000000001");
        return decimal.unscaledValue().longValue();
    }

    private static Binary getDecimal80() {
        BigDecimal decimal = new BigDecimal("9.223372036854775800001");
        return Binary.fromConstantByteArray(decimal.unscaledValue().toByteArray());
    }

    private static Binary createConstantBinary() {
        byte[] binaryBytes = { 0x00, 0x01, 0x02 };
        return Binary.fromConstantByteArray(binaryBytes);
    }

    private static Binary createUUIDBinary() throws HyracksDataException {
        char[] digit = "123e4567-e89b-12d3-a456-426614174000".toCharArray();
        AMutableUUID uuid = new AMutableUUID();
        uuid.parseUUIDString(digit, 0, digit.length);
        ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
        AUUIDSerializerDeserializer.INSTANCE.serialize(uuid, storage.getDataOutput());
        return Binary.fromConstantByteArray(storage.getByteArray(), 0, storage.getLength());
    }

    private static class TestOutputFile implements OutputFile {

        private final OutputFile outputFile;

        TestOutputFile(Path path, Configuration conf) throws IOException {
            outputFile = HadoopOutputFile.fromPath(path, conf);
        }

        @Override
        public PositionOutputStream create(long blockSizeHint) throws IOException {
            return outputFile.create(blockSizeHint);
        }

        @Override
        public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
            return outputFile.createOrOverwrite(blockSizeHint);
        }

        @Override
        public boolean supportsBlockSize() {
            return outputFile.supportsBlockSize();
        }

        @Override
        public long defaultBlockSize() {
            return outputFile.defaultBlockSize();
        }
    }
}
