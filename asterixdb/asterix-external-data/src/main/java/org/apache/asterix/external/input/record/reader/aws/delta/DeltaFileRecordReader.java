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
import static org.apache.asterix.external.util.aws.s3.S3Constants.SERVICE_END_POINT_FIELD_NAME;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.asterix.external.api.IExternalDataRuntimeContext;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.dataflow.AbstractFeedDataFlowController;
import org.apache.asterix.external.input.record.GenericRecord;
import org.apache.asterix.external.util.IFeedLogManager;
import org.apache.asterix.external.util.aws.s3.S3Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.VoidPointable;

import io.delta.kernel.Scan;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.types.StructType;
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

    public DeltaFileRecordReader(List<String> serScanFiles, String serScanState, Map<String, String> conf,
            IExternalDataRuntimeContext context) {
        Configuration config = new Configuration();
        config.set(S3Constants.HADOOP_ACCESS_KEY_ID, conf.get(S3Constants.ACCESS_KEY_ID_FIELD_NAME));
        config.set(S3Constants.HADOOP_SECRET_ACCESS_KEY, conf.get(S3Constants.SECRET_ACCESS_KEY_FIELD_NAME));
        if (conf.get(S3Constants.SESSION_TOKEN_FIELD_NAME) != null) {
            config.set(S3Constants.HADOOP_SESSION_TOKEN, conf.get(S3Constants.SESSION_TOKEN_FIELD_NAME));
        }
        config.set(S3Constants.HADOOP_REGION, conf.get(S3Constants.REGION_FIELD_NAME));
        String serviceEndpoint = conf.get(SERVICE_END_POINT_FIELD_NAME);
        if (serviceEndpoint != null) {
            config.set(S3Constants.HADOOP_SERVICE_END_POINT, serviceEndpoint);
        }
        this.engine = DefaultEngine.create(config);
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
            try {
                this.physicalDataIter = engine.getParquetHandler()
                        .readParquetFiles(singletonCloseableIterator(fileStatus), physicalReadSchema, Optional.empty());
                this.dataIter = Scan.transformPhysicalData(engine, scanState, scanFile, physicalDataIter);
                if (dataIter.hasNext()) {
                    rows = dataIter.next().getRows();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
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
            physicalDataIter = engine.getParquetHandler().readParquetFiles(singletonCloseableIterator(fileStatus),
                    physicalReadSchema, Optional.empty());
            dataIter = Scan.transformPhysicalData(engine, scanState, scanFile, physicalDataIter);
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
}
