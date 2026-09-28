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
package org.apache.asterix.external.input.record.reader.hdfs.parquet;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.dataflow.data.nontagged.serde.AObjectSerializerDeserializer;
import org.apache.asterix.external.util.ExternalDataConstants.ParquetOptions;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.ARecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Reads FIXED_LEN_BYTE_ARRAY-backed decimals with {@link ParquetOptions#DECIMAL_TO_DOUBLE} enabled.
 */
public class ParquetDecimalToDoubleTest {
    private static final Column D9 = new Column("d9", 9, 2, 4, "1234567.89", "-1234567.89", "9999999.99", "-0.01");
    private static final Column D18 =
            new Column("d18", 18, 5, 8, "123.45600", "-9876543210.12345", "9999999999999.99999", "-0.00001");
    // an unscaled value of precision 19 or 20 needs 9 bytes, and may not fit in a long
    private static final Column D20 =
            new Column("d20", 20, 5, 9, "123.45600", "-123.45600", "999999999999999.99999", "-92233720368547.75809");
    private static final Column D38 = new Column("d38", 38, 10, 16, "123.4560000000", "-1.5000000000",
            "9999999999999999999999999999.9999999999", "-0.0000000001");
    // wider than precision 10 needs, as some writers produce
    private static final Column D10_WIDE =
            new Column("d10_wide", 10, 2, 12, "99999999.99", "-99999999.99", "0.01", "-21474836.48");
    private static final Column[] COLUMNS = { D9, D18, D20, D38, D10_WIDE };
    private static final int ROW_COUNT = 4;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testFixedLenByteArrayDecimals() throws Exception {
        Configuration conf = new Configuration();
        conf.set(ParquetOptions.HADOOP_DECIMAL_TO_DOUBLE, "true");
        conf.set(ParquetOptions.HADOOP_TIMEZONE, "");
        Path path = new Path(new File(tempFolder.getRoot(), "decimals.parquet").toURI());
        write(path, conf);

        int row = 0;
        try (ParquetReader<IValueReference> reader =
                ParquetReader.builder(new ParquetReadSupport(), path).withConf(conf).build()) {
            for (IValueReference value = reader.read(); value != null; value = reader.read()) {
                Map<String, Double> actual = toDoubles(value);
                for (Column column : COLUMNS) {
                    String decimal = column.values[row];
                    Assert.assertEquals(column.name + " = " + decimal, new BigDecimal(decimal).doubleValue(),
                            actual.get(column.name), 0.0);
                }
                row++;
            }
        }
        Assert.assertEquals(ROW_COUNT, row);
    }

    private static void write(Path path, Configuration conf) throws Exception {
        Types.MessageTypeBuilder builder = Types.buildMessage();
        for (Column column : COLUMNS) {
            builder.required(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY).length(column.width)
                    .as(LogicalTypeAnnotation.decimalType(column.scale, column.precision)).named(column.name);
        }
        MessageType schema = builder.named("decimals");
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (ParquetWriter<Group> writer =
                ExampleParquetWriter.builder(HadoopOutputFile.fromPath(path, conf)).withType(schema).build()) {
            for (int row = 0; row < ROW_COUNT; row++) {
                Group group = factory.newGroup();
                for (Column column : COLUMNS) {
                    group.append(column.name, toFixedLength(new BigDecimal(column.values[row]), column.width));
                }
                writer.write(group);
            }
        }
    }

    private static Binary toFixedLength(BigDecimal decimal, int width) {
        BigInteger unscaled = decimal.unscaledValue();
        byte[] minimal = unscaled.toByteArray();
        byte[] bytes = new byte[width];
        Arrays.fill(bytes, unscaled.signum() < 0 ? (byte) 0xFF : 0);
        System.arraycopy(minimal, 0, bytes, width - minimal.length, minimal.length);
        return Binary.fromConstantByteArray(bytes);
    }

    private static Map<String, Double> toDoubles(IValueReference value) throws Exception {
        DataInputStream in = new DataInputStream(
                new ByteArrayInputStream(value.getByteArray(), value.getStartOffset(), value.getLength()));
        ARecord record = (ARecord) AObjectSerializerDeserializer.INSTANCE.deserialize(in);
        Map<String, Double> doubles = new HashMap<>();
        for (int i = 0; i < record.numberOfFields(); i++) {
            doubles.put(record.getType().getFieldNames()[i], ((ADouble) record.getValueByPos(i)).getDoubleValue());
        }
        return doubles;
    }

    private record Column(String name, int precision, int scale, int width, String... values) {
    }
}
