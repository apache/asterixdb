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
package org.apache.asterix.test.external_dataset.deltalake;

import static io.delta.kernel.internal.util.Utils.toCloseableIterator;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;

import io.delta.kernel.DataWriteContext;
import io.delta.kernel.Operation;
import io.delta.kernel.Table;
import io.delta.kernel.Transaction;
import io.delta.kernel.TransactionBuilder;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatus;

public class DeltaAllTypeGenerator {
    public static final DecimalType decimal_t = new DecimalType(10, 5);
    protected static final StructType exampleTableSchema = new StructType().add("integer_type", IntegerType.INTEGER)
            .add("string_type", StringType.STRING).add("decimal_type", decimal_t).add("double_type", DoubleType.DOUBLE)
            .add("timestamp_type", TimestampType.TIMESTAMP).add("date_type", DateType.DATE);
    public static final String DELTA_ALL_TYPE_TABLE_PATH =
            "target" + File.separatorChar + "generated_delta_files" + File.separatorChar + "delta_all_type";

    public static void createTableInsertData(Configuration conf) throws IOException {
        Engine engine = DefaultEngine.create(conf);
        Table table = Table.forPath(engine, DELTA_ALL_TYPE_TABLE_PATH);
        TransactionBuilder txnBuilder = table.createTransactionBuilder(engine, "Examples", Operation.CREATE_TABLE);
        txnBuilder = txnBuilder.withSchema(engine, exampleTableSchema);
        Transaction txn = txnBuilder.build(engine);
        Row txnState = txn.getTransactionState(engine);
        ColumnVector[] vectors = new ColumnVector[exampleTableSchema.length()];
        vectors[0] = intVector(Arrays.asList(123, 124, 125, 126, 127));
        vectors[1] = stringVector(
                Arrays.asList("FirstPerson", "SecondPerson", "ThirdPerson", "FourthPerson", "FifthPerson"));
        vectors[2] = decimalVector(Arrays.asList(new BigDecimal("1.25432"), new BigDecimal("2666.223"),
                new BigDecimal("1245.2421"), new BigDecimal("23731.2"), new BigDecimal("80911.222456")));
        vectors[3] = doubleVector(Arrays.asList(100.34d, 200.055d, 300.02d, 400.21014d, 500.219d));
        vectors[4] = timestampVector(
                Arrays.asList(1732010400000L, 1732010400330L, 1732010400450L, 1732010403000L, 1732010401200L));
        vectors[5] = dateVector(Arrays.asList(127, 23, 11, 456, 23));
        ColumnarBatch batch = new DefaultColumnarBatch(5, exampleTableSchema, vectors);
        FilteredColumnarBatch f1 = new FilteredColumnarBatch(batch, Optional.empty());
        CloseableIterator<FilteredColumnarBatch> data = toCloseableIterator(Arrays.asList(f1).iterator());
        CloseableIterator<FilteredColumnarBatch> physicalData =
                Transaction.transformLogicalData(engine, txnState, data, Collections.emptyMap());
        DataWriteContext writeContext = Transaction.getWriteContext(engine, txnState, Collections.emptyMap());
        CloseableIterator<DataFileStatus> dataFiles = engine.getParquetHandler().writeParquetFiles(
                writeContext.getTargetDirectory(), physicalData, writeContext.getStatisticsColumns());
        CloseableIterator<Row> dataActions =
                Transaction.generateAppendActions(engine, txnState, dataFiles, writeContext);
        CloseableIterable<Row> dataActionsIterable = CloseableIterable.inMemoryIterable(dataActions);
        txn.commit(engine, dataActionsIterable);

    }

    static ColumnVector stringVector(List<String> data) {
        return new ColumnVector() {
            @Override
            public DataType getDataType() {
                return StringType.STRING;
            }

            @Override
            public int getSize() {
                return data.size();
            }

            @Override
            public void close() {
            }

            @Override
            public boolean isNullAt(int rowId) {
                return data.get(rowId) == null;
            }

            @Override
            public String getString(int rowId) {
                return data.get(rowId);
            }
        };
    }

    static ColumnVector intVector(List<Integer> data) {
        return new ColumnVector() {
            @Override
            public DataType getDataType() {
                return IntegerType.INTEGER;
            }

            @Override
            public int getSize() {
                return data.size();
            }

            @Override
            public void close() {
            }

            @Override
            public boolean isNullAt(int rowId) {
                return false;
            }

            @Override
            public int getInt(int rowId) {
                return data.get(rowId);
            }
        };
    }

    static ColumnVector doubleVector(List<Double> data) {
        return new ColumnVector() {
            @Override
            public DataType getDataType() {
                return DoubleType.DOUBLE;
            }

            @Override
            public int getSize() {
                return data.size();
            }

            @Override
            public void close() {
            }

            @Override
            public boolean isNullAt(int rowId) {
                return data.get(rowId) == null;
            }

            @Override
            public double getDouble(int rowId) {
                return data.get(rowId);
            }
        };
    }

    static ColumnVector decimalVector(List<BigDecimal> data) {
        return new ColumnVector() {
            @Override
            public DataType getDataType() {
                return decimal_t; // Use the specific DecimalType passed (scale and precision)
            }

            @Override
            public int getSize() {
                return data.size();
            }

            @Override
            public void close() {
            }

            @Override
            public boolean isNullAt(int rowId) {
                return data.get(rowId) == null;
            }

            @Override
            public BigDecimal getDecimal(int rowId) {
                // Return the BigDecimal value directly as Delta Kernel works natively with BigDecimal for decimals
                return data.get(rowId);
            }
        };
    }

    static ColumnVector timestampVector(List<Long> data) { // Assuming timestamp values are stored as microseconds since epoch
        return new ColumnVector() {
            @Override
            public DataType getDataType() {
                return TimestampType.TIMESTAMP;
            }

            @Override
            public int getSize() {
                return data.size();
            }

            @Override
            public void close() {
            }

            @Override
            public boolean isNullAt(int rowId) {
                return data.get(rowId) == null;
            }

            @Override
            public long getLong(int rowId) {
                // Delta Lake often uses microseconds since epoch for timestamps
                return data.get(rowId);
            }
        };
    }

    static ColumnVector dateVector(List<Integer> data) { // Assuming date values are stored as days since epoch
        return new ColumnVector() {
            @Override
            public DataType getDataType() {
                return DateType.DATE;
            }

            @Override
            public int getSize() {
                return data.size();
            }

            @Override
            public void close() {
            }

            @Override
            public boolean isNullAt(int rowId) {
                return data.get(rowId) == null;
            }

            @Override
            public int getInt(int rowId) {
                // Delta Lake often uses days since epoch for dates
                return data.get(rowId);
            }
        };
    }

}
