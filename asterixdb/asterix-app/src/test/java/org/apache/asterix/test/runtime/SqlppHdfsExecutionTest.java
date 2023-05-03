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
package org.apache.asterix.test.runtime;

import static org.apache.iceberg.hadoop.HadoopOutputFile.fromPath;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Tables;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Runs the SQL++ runtime tests with the storage parallelism.
 */
@RunWith(Parameterized.class)
public class SqlppHdfsExecutionTest {
    protected static final String TEST_CONFIG_FILE_NAME = "src/main/resources/cc.conf";

    private static DataFile writeFile(String filename, List<Record> records, String location, Schema schema,
            Configuration conf) throws IOException {
        Path path = new Path(location, filename);
        FileFormat fileFormat = FileFormat.fromFileName(filename);
        Preconditions.checkNotNull(fileFormat, "Cannot determine format for file: %s", filename);

        FileAppender<Record> fileAppender =
                new GenericAppenderFactory(schema).newAppender(fromPath(path, conf), fileFormat);
        try (FileAppender<Record> appender = fileAppender) {
            appender.addAll(records);
        }

        return DataFiles.builder(PartitionSpec.unpartitioned()).withInputFile(HadoopInputFile.fromPath(path, conf))
                .withMetrics(fileAppender.metrics()).build();
    }

    private static void setUpIcebergData() {
        Configuration conf = new Configuration();
        conf.set(ExternalDataConstants.KEY_HADOOP_FILESYSTEM_URI, "hdfs://127.0.0.1:31888/");

        Tables tables = new HadoopTables(conf);

        Schema schema =
                new Schema(required(1, "id", Types.IntegerType.get()), required(2, "data", Types.StringType.get()));

        String path = "hdfs://localhost:31888/my_table/";

        Table table = tables.create(schema, PartitionSpec.unpartitioned(),
                ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.PARQUET.name()), path);

        Record genericRecord = GenericRecord.create(schema);
        List<Record> fileFirstSnapshotRecords =
                ImmutableList.of(genericRecord.copy(ImmutableMap.of("id", 0, "data", "vibrant_mclean")),
                        genericRecord.copy(ImmutableMap.of("id", 1, "data", "frosty_wilson")),
                        genericRecord.copy(ImmutableMap.of("id", 2, "data", "serene_kirby")));

        // load test data
        try {
            DataFile file =
                    writeFile(FileFormat.PARQUET.addExtension("file"), fileFirstSnapshotRecords, path, schema, conf);
            table.newAppend().appendFile(file).commit();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeClass
    public static void setUp() throws Exception {
        LangExecutionUtil.setUp(TEST_CONFIG_FILE_NAME, new TestExecutor(), true);
        setUpIcebergData();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        LangExecutionUtil.tearDown();
    }

    @Parameters(name = "SqlppHdfsExecutionTest {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        return LangExecutionUtil.tests("only_sqlpp_hdfs.xml", "testsuite_sqlpp_hdfs.xml");
    }

    protected TestCaseContext tcCtx;

    public SqlppHdfsExecutionTest(TestCaseContext tcCtx) {
        this.tcCtx = tcCtx;
    }

    @Test
    public void test() throws Exception {
        LangExecutionUtil.test(tcCtx);
    }
}
