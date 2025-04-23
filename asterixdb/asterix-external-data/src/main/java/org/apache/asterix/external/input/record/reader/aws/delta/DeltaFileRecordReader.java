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
package org.apache.asterix.external.input.record.reader.aws.delta;

import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;
import static org.apache.hyracks.api.util.ExceptionUtils.getMessageOrToString;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.dataflow.AbstractFeedDataFlowController;
import org.apache.asterix.external.input.record.GenericRecord;
import org.apache.asterix.external.util.IFeedLogManager;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.hdfs.dataflow.ConfFactory;

import io.delta.kernel.Scan;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.engine.ExpressionHandler;
import io.delta.kernel.expressions.ExpressionEvaluator;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.internal.data.SelectionColumnVector;
import io.delta.kernel.internal.deletionvectors.DeletionVectorUtils;
import io.delta.kernel.internal.deletionvectors.RoaringBitmapArray;
import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.internal.util.InternalUtils;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;

/**
 * Delta record reader.
 * The reader returns records in Delta Kernel Row format.
 */
public class DeltaFileRecordReader implements IRecordReader<Row> {

    private Engine engine;
    private List<Row> scanFiles;
    private Row scanState;
    protected IRawRecord<Row> record;
    protected VoidPointable value = null;
    private FileStatus fileStatus;
    private StructType physicalReadSchema;
    private CloseableIterator<ColumnarBatch> physicalDataIter;
    private CloseableIterator<FilteredColumnarBatch> dataIter;
    private int fileIndex;
    private Row scanFile;
    private CloseableIterator<Row> rows;
    private Optional<Predicate> filterPredicate;

    public DeltaFileRecordReader(List<String> serScanFiles, String serScanState, ConfFactory config,
            String filterExpressionStr) throws HyracksDataException {
        JobConf conf = config.getConf();
        this.engine = DeltaEngine.create(conf);
        this.scanFiles = new ArrayList<>();
        for (String scanFile : serScanFiles) {
            this.scanFiles.add(RowSerDe.deserializeRowFromJson(scanFile));
        }
        this.scanState = RowSerDe.deserializeRowFromJson(serScanState);
        this.fileStatus = null;
        this.physicalReadSchema = null;
        this.physicalDataIter = null;
        this.dataIter = null;
        this.record = new GenericRecord<>();
        if (scanFiles.size() > 0) {
            this.fileIndex = 0;
            this.scanFile = scanFiles.get(0);
            this.fileStatus = InternalScanFileUtils.getAddFileStatus(scanFile);
            this.physicalReadSchema = ScanStateRow.getPhysicalDataReadSchema(engine, scanState);
            this.filterPredicate = PredicateSerDe.deserializeExpressionFromJson(filterExpressionStr);
            try {
                this.physicalDataIter = engine.getParquetHandler()
                        .readParquetFiles(singletonCloseableIterator(fileStatus), physicalReadSchema, filterPredicate);
                this.dataIter = transformPhysicalData(engine, scanState, scanFile, physicalDataIter);
                if (dataIter.hasNext()) {
                    rows = dataIter.next().getRows();
                }
            } catch (IOException e) {
                throw new RuntimeDataException(ErrorCode.EXTERNAL_SOURCE_ERROR, e, getMessageOrToString(e));
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (dataIter != null) {
            dataIter.close();
        }
        if (physicalDataIter != null) {
            physicalDataIter.close();
        }
    }

    @Override
    public boolean hasNext() throws Exception {
        if (rows != null && rows.hasNext()) {
            return true;
        } else if (dataIter != null && dataIter.hasNext()) {
            rows = dataIter.next().getRows();
            return this.hasNext();
        } else if (fileIndex < scanFiles.size() - 1) {
            fileIndex++;
            scanFile = scanFiles.get(fileIndex);
            fileStatus = InternalScanFileUtils.getAddFileStatus(scanFile);
            physicalReadSchema = ScanStateRow.getPhysicalDataReadSchema(engine, scanState);
            try {
                physicalDataIter = engine.getParquetHandler().readParquetFiles(singletonCloseableIterator(fileStatus),
                        physicalReadSchema, filterPredicate);
                dataIter = Scan.transformPhysicalData(engine, scanState, scanFile, physicalDataIter);
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
            return this.hasNext();
        } else {
            return false;
        }
    }

    @Override
    public IRawRecord<Row> next() throws IOException, InterruptedException {
        Row row = rows.next();
        record.set(row);
        return record;
    }

    @Override
    public boolean stop() {
        return false;
    }

    @Override
    public void setController(AbstractFeedDataFlowController controller) {

    }

    @Override
    public void setFeedLogManager(IFeedLogManager feedLogManager) throws HyracksDataException {

    }

    @Override
    public boolean handleException(Throwable th) {
        return false;
    }

    static CloseableIterator<FilteredColumnarBatch> transformPhysicalData(Engine engine, Row scanState, Row scanFile,
            CloseableIterator<ColumnarBatch> physicalDataIter) throws IOException {
        return new CloseableIterator<FilteredColumnarBatch>() {
            boolean inited = false;

            // initialized as part of init()
            StructType physicalReadSchema = null;
            StructType logicalReadSchema = null;
            String tablePath = null;

            RoaringBitmapArray currBitmap = null;
            DeletionVectorDescriptor currDV = null;

            private void initIfRequired() {
                if (inited) {
                    return;
                }
                physicalReadSchema = ScanStateRow.getPhysicalSchema(engine, scanState);
                logicalReadSchema = ScanStateRow.getLogicalSchema(engine, scanState);

                tablePath = ScanStateRow.getTableRoot(scanState);
                inited = true;
            }

            @Override
            public void close() throws IOException {
                physicalDataIter.close();
            }

            @Override
            public boolean hasNext() {
                initIfRequired();
                return physicalDataIter.hasNext();
            }

            @Override
            public FilteredColumnarBatch next() {
                initIfRequired();
                ColumnarBatch nextDataBatch = physicalDataIter.next();

                DeletionVectorDescriptor dv = InternalScanFileUtils.getDeletionVectorDescriptorFromRow(scanFile);

                int rowIndexOrdinal = nextDataBatch.getSchema().indexOf(StructField.METADATA_ROW_INDEX_COLUMN_NAME);

                // Get the selectionVector if DV is present
                Optional<ColumnVector> selectionVector;
                if (dv == null) {
                    selectionVector = Optional.empty();
                } else {
                    if (rowIndexOrdinal == -1) {
                        throw new IllegalArgumentException(
                                "Row index column is not " + "present in the data read from the Parquet file.");
                    }
                    if (!dv.equals(currDV)) {
                        Tuple2<DeletionVectorDescriptor, RoaringBitmapArray> dvInfo =
                                DeletionVectorUtils.loadNewDvAndBitmap(engine, tablePath, dv);
                        this.currDV = dvInfo._1;
                        this.currBitmap = dvInfo._2;
                    }
                    ColumnVector rowIndexVector = nextDataBatch.getColumnVector(rowIndexOrdinal);
                    selectionVector = Optional.of(new SelectionColumnVector(currBitmap, rowIndexVector));
                }
                if (rowIndexOrdinal != -1) {
                    nextDataBatch = nextDataBatch.withDeletedColumnAt(rowIndexOrdinal);
                }

                // Add partition columns
                nextDataBatch = withPartitionColumns(engine.getExpressionHandler(), nextDataBatch,
                        InternalScanFileUtils.getPartitionValues(scanFile), physicalReadSchema);

                // Change back to logical schema
                String columnMappingMode = ScanStateRow.getColumnMappingMode(scanState);
                switch (columnMappingMode) {
                    case ColumnMapping.COLUMN_MAPPING_MODE_NAME:
                    case ColumnMapping.COLUMN_MAPPING_MODE_ID:
                        nextDataBatch = nextDataBatch.withNewSchema(logicalReadSchema);
                        break;
                    case ColumnMapping.COLUMN_MAPPING_MODE_NONE:
                        break;
                    default:
                        throw new UnsupportedOperationException(
                                "Column mapping mode is not yet supported: " + columnMappingMode);
                }

                return new FilteredColumnarBatch(nextDataBatch, selectionVector);
            }
        };
    }

    public static ColumnarBatch withPartitionColumns(ExpressionHandler expressionHandler, ColumnarBatch dataBatch,
            Map<String, String> partitionValues, StructType schemaWithPartitionCols) {
        if (partitionValues == null || partitionValues.size() == 0) {
            // no partition column vectors to attach to.
            return dataBatch;
        }

        for (int colIdx = 0; colIdx < schemaWithPartitionCols.length(); colIdx++) {
            StructField structField = schemaWithPartitionCols.at(colIdx);

            if (partitionValues.containsKey(structField.getName())) {
                // Create a partition vector

                ExpressionEvaluator evaluator = expressionHandler.getEvaluator(dataBatch.getSchema(),
                        literalForPartitionValue(structField.getDataType(), partitionValues.get(structField.getName())),
                        structField.getDataType());

                ColumnVector partitionVector = evaluator.eval(dataBatch);
                dataBatch = dataBatch.withNewColumn(colIdx, structField, partitionVector);
            }
        }

        return dataBatch;
    }

    protected static Literal literalForPartitionValue(DataType dataType, String partitionValue) {
        if (partitionValue == null) {
            return Literal.ofNull(dataType);
        }

        if (dataType instanceof BooleanType) {
            return Literal.ofBoolean(Boolean.parseBoolean(partitionValue));
        }
        if (dataType instanceof ByteType) {
            return Literal.ofByte(Byte.parseByte(partitionValue));
        }
        if (dataType instanceof ShortType) {
            return Literal.ofShort(Short.parseShort(partitionValue));
        }
        if (dataType instanceof IntegerType) {
            return Literal.ofInt(Integer.parseInt(partitionValue));
        }
        if (dataType instanceof LongType) {
            return Literal.ofLong(Long.parseLong(partitionValue));
        }
        if (dataType instanceof FloatType) {
            return Literal.ofFloat(Float.parseFloat(partitionValue));
        }
        if (dataType instanceof DoubleType) {
            return Literal.ofDouble(Double.parseDouble(partitionValue));
        }
        if (dataType instanceof StringType) {
            return Literal.ofString(partitionValue);
        }
        if (dataType instanceof BinaryType) {
            return Literal.ofBinary(partitionValue.getBytes());
        }
        if (dataType instanceof DateType) {
            return Literal.ofDate(InternalUtils.daysSinceEpoch(Date.valueOf(partitionValue)));
        }
        if (dataType instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) dataType;
            return Literal.ofDecimal(new BigDecimal(partitionValue), decimalType.getPrecision(),
                    decimalType.getScale());
        }
        if (dataType instanceof TimestampType) {
            try {
                Timestamp timestamp = Timestamp.valueOf(partitionValue);
                return Literal.ofTimestamp(InternalUtils.microsSinceEpoch(timestamp));
            } catch (IllegalArgumentException e) {
                Instant instant = Instant.parse(partitionValue);
                return Literal.ofTimestamp(ChronoUnit.MICROS.between(Instant.EPOCH, instant));
            }
        }
        if (dataType instanceof TimestampNTZType) {
            // Both the timestamp and timestamp_ntz have no timezone info, so they are interpreted
            // in local time zone.
            try {
                Timestamp timestamp = Timestamp.valueOf(partitionValue);
                return Literal.ofTimestampNtz(InternalUtils.microsSinceEpoch(timestamp));
            } catch (IllegalArgumentException e) {
                Instant instant = Instant.parse(partitionValue);
                return Literal.ofTimestampNtz(ChronoUnit.MICROS.between(Instant.EPOCH, instant));
            }
        }

        throw new UnsupportedOperationException("Unsupported partition column: " + dataType);
    }
}
