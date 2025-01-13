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
package org.apache.asterix.external.input.record.reader.awss3;

import static io.delta.kernel.internal.InternalScanFileUtils.ADD_FILE_ORDINAL;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.external.input.record.reader.aws.delta.DeltaReaderFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.junit.Assert;
import org.junit.Test;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;

public class DeltaTopicPartitionDistributionTest {

    @Test
    public void distributeFilesMoreFilesThanPartitions() {
        int rowCount = 25;
        int numberOfPartition = 13;
        List<Row> scanFiles = createMockRows(rowCount);
        DeltaReaderFactory d = new DeltaReaderFactory() {
            @Override
            protected void configureJobConf(IApplicationContext appCtx, JobConf conf, Map<String, String> configuration)
                    throws AlgebricksException {

            }

            @Override
            protected String getTablePath(Map<String, String> configuration) throws AlgebricksException {
                return null;
            }

            @Override
            public List<String> getRecordReaderNames() {
                return null;
            }
        };
        d.distributeFiles(scanFiles, numberOfPartition);
        Assert.assertEquals(numberOfPartition, d.getPartitionWorkLoadsBasedOnSize().size());
        verifyFileDistribution(scanFiles.size(), d.getPartitionWorkLoadsBasedOnSize());
    }

    @Test
    public void distributeFilesLessFilesThanPartitions() {
        int rowCount = 15;
        int numberOfPartition = 23;
        List<Row> scanFiles = createMockRows(rowCount);
        DeltaReaderFactory d = new DeltaReaderFactory() {
            @Override
            protected void configureJobConf(IApplicationContext appCtx, JobConf conf, Map<String, String> configuration)
                    throws AlgebricksException {

            }

            @Override
            protected String getTablePath(Map<String, String> configuration) throws AlgebricksException {
                return null;
            }

            @Override
            public List<String> getRecordReaderNames() {
                return null;
            }
        };
        d.distributeFiles(scanFiles, numberOfPartition);
        Assert.assertEquals(numberOfPartition, d.getPartitionWorkLoadsBasedOnSize().size());
        verifyFileDistribution(scanFiles.size(), d.getPartitionWorkLoadsBasedOnSize());
    }

    @Test
    public void distributeFilesEqualFilesAndPartitions() {
        int rowCount = 9;
        int numberOfPartition = 9;
        List<Row> scanFiles = createMockRows(rowCount);
        DeltaReaderFactory d = new DeltaReaderFactory() {
            @Override
            protected void configureJobConf(IApplicationContext appCtx, JobConf conf, Map<String, String> configuration)
                    throws AlgebricksException {

            }

            @Override
            protected String getTablePath(Map<String, String> configuration) throws AlgebricksException {
                return null;
            }

            @Override
            public List<String> getRecordReaderNames() {
                return null;
            }
        };
        d.distributeFiles(scanFiles, numberOfPartition);
        Assert.assertEquals(numberOfPartition, d.getPartitionWorkLoadsBasedOnSize().size());
        verifyFileDistribution(scanFiles.size(), d.getPartitionWorkLoadsBasedOnSize());
    }

    private void verifyFileDistribution(int numberOfFiles,
            List<DeltaReaderFactory.PartitionWorkLoadBasedOnSize> workloads) {
        int totalDistributedFiles = 0;

        for (DeltaReaderFactory.PartitionWorkLoadBasedOnSize workload : workloads) {
            totalDistributedFiles += workload.getScanFiles().size();
            Assert.assertTrue(workload.getTotalSize() >= 0);
        }
        Assert.assertEquals(numberOfFiles, totalDistributedFiles);
    }

    private List<Row> createMockRows(int count) {
        List<Row> rows = new ArrayList<>();
        StructType sch = createMockSchema();

        for (int i = 1; i <= count; i++) {
            int finalI = i;
            Row row = new Row() {

                @Override
                public StructType getSchema() {
                    return sch;
                }

                @Override
                public boolean isNullAt(int i) {
                    return false;
                }

                @Override
                public boolean getBoolean(int i) {
                    return false;
                }

                @Override
                public byte getByte(int i) {
                    return 0;
                }

                @Override
                public short getShort(int i) {
                    return 0;
                }

                @Override
                public int getInt(int i) {
                    if (i == 1) {
                        return finalI;
                    } else if (i == 2) {
                        return finalI * 10;
                    }
                    return 0;
                }

                @Override
                public long getLong(int i) {
                    return 0;
                }

                @Override
                public float getFloat(int i) {
                    return 0;
                }

                @Override
                public double getDouble(int i) {
                    return 0;
                }

                @Override
                public String getString(int i) {
                    if (i == 0) {
                        return "tableRoot_" + finalI;
                    } else if (i == 1) {
                        return "addFilePath_" + finalI;
                    }
                    return null;
                }

                @Override
                public BigDecimal getDecimal(int i) {
                    return null;
                }

                @Override
                public byte[] getBinary(int i) {
                    return new byte[0];
                }

                @Override
                public ArrayValue getArray(int i) {
                    return null;
                }

                @Override
                public MapValue getMap(int i) {
                    return null;
                }

                @Override
                public Row getStruct(int index) {
                    if (index == ADD_FILE_ORDINAL) {
                        return createAddFileEntry(finalI);
                    }
                    return null;
                }
            };

            rows.add(row);
        }

        return rows;
    }

    private StructType createMockSchema() {
        List<StructField> fields = new ArrayList<>();

        fields.add(new StructField("field1", StringType.STRING, true));
        fields.add(new StructField("field2", IntegerType.INTEGER, true));
        fields.add(new StructField("field3", IntegerType.INTEGER, true));

        return new StructType(fields);
    }

    private Row createAddFileEntry(int i) {
        List<StructField> addFileFields = new ArrayList<>();

        addFileFields.add(new StructField("addFilePath", StringType.STRING, true));
        addFileFields.add(new StructField("size", IntegerType.INTEGER, true));

        StructType addFileSchema = new StructType(addFileFields);

        Row addFileRow = new Row() {
            @Override
            public StructType getSchema() {
                return addFileSchema;
            }

            @Override
            public boolean isNullAt(int index) {
                return false;
            }

            @Override
            public boolean getBoolean(int i) {
                return false;
            }

            @Override
            public byte getByte(int i) {
                return 0;
            }

            @Override
            public short getShort(int i) {
                return 0;
            }

            @Override
            public int getInt(int index) {
                if (index == 1) {
                    return i * 100;
                }
                return 0;
            }

            @Override
            public long getLong(int i) {
                return 0;
            }

            @Override
            public float getFloat(int i) {
                return 0;
            }

            @Override
            public double getDouble(int i) {
                return 0;
            }

            @Override
            public BigDecimal getDecimal(int i) {
                return null;
            }

            @Override
            public byte[] getBinary(int i) {
                return new byte[0];
            }

            @Override
            public Row getStruct(int index) {
                return null;
            }

            @Override
            public ArrayValue getArray(int index) {
                return null;
            }

            @Override
            public MapValue getMap(int index) {
                return null;
            }

            @Override
            public String getString(int index) {
                if (index == 0) {
                    return "addFilePath_" + i;
                }
                return null;
            }
        };

        return addFileRow;
    }
}
