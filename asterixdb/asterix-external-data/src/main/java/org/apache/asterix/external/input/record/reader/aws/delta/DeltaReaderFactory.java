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

import static org.apache.hyracks.api.util.ExceptionUtils.getMessageOrToString;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.external.IExternalFilterEvaluatorFactory;
import org.apache.asterix.external.api.IExternalDataRuntimeContext;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.api.IRecordReaderFactory;
import org.apache.asterix.external.input.record.reader.aws.delta.converter.DeltaConverterContext;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.HDFSUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.runtime.projection.FunctionCallInformation;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.hdfs.dataflow.ConfFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.delta.kernel.Scan;
import io.delta.kernel.Snapshot;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;

public abstract class DeltaReaderFactory implements IRecordReaderFactory<Object> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LogManager.getLogger();
    private transient AlgebricksAbsolutePartitionConstraint locationConstraints;
    private String scanState;
    protected final List<PartitionWorkLoadBasedOnSize> partitionWorkLoadsBasedOnSize = new ArrayList<>();
    protected ConfFactory confFactory;

    @Override
    public AlgebricksAbsolutePartitionConstraint getPartitionConstraint() {
        return locationConstraints;
    }

    protected abstract void configureJobConf(JobConf conf, Map<String, String> configuration)
            throws AlgebricksException;

    protected abstract String getTablePath(Map<String, String> configuration) throws AlgebricksException;

    @Override
    public void configure(IServiceContext serviceCtx, Map<String, String> configuration,
            IWarningCollector warningCollector, IExternalFilterEvaluatorFactory filterEvaluatorFactory)
            throws AlgebricksException, HyracksDataException {
        JobConf conf = new JobConf();
        ICcApplicationContext appCtx = (ICcApplicationContext) serviceCtx.getApplicationContext();
        configureJobConf(conf, configuration);
        confFactory = new ConfFactory(conf);
        String tableMetadataPath = getTablePath(configuration);
        Engine engine = DefaultEngine.create(conf);
        io.delta.kernel.Table table = io.delta.kernel.Table.forPath(engine, tableMetadataPath);
        Snapshot snapshot;
        try {
            snapshot = table.getLatestSnapshot(engine);
        } catch (KernelException e) {
            LOGGER.info("Failed to get latest snapshot for table: {}", tableMetadataPath, e);
            throw RuntimeDataException.create(ErrorCode.EXTERNAL_SOURCE_ERROR, e, getMessageOrToString(e));
        }

        List<Warning> warnings = new ArrayList<>();
        DeltaConverterContext converterContext = new DeltaConverterContext(configuration, warnings);
        AsterixTypeToDeltaTypeVisitor visitor = new AsterixTypeToDeltaTypeVisitor(converterContext);
        StructType requiredSchema;
        try {
            ARecordType expectedType = HDFSUtils.getExpectedType(conf);
            Map<String, FunctionCallInformation> functionCallInformationMap =
                    HDFSUtils.getFunctionCallInformationMap(conf);
            StructType fileSchema = snapshot.getSchema(engine);
            requiredSchema = visitor.clipType(expectedType, fileSchema, functionCallInformationMap);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (AsterixDeltaRuntimeException e) {
            throw e.getHyracksDataException();
        }
        Scan scan = snapshot.getScanBuilder(engine).withReadSchema(engine, requiredSchema).build();
        scanState = RowSerDe.serializeRowToJson(scan.getScanState(engine));
        CloseableIterator<FilteredColumnarBatch> iter = scan.getScanFiles(engine);

        List<Row> scanFiles = new ArrayList<>();
        while (iter.hasNext()) {
            FilteredColumnarBatch batch = iter.next();
            CloseableIterator<Row> rowIter = batch.getRows();
            while (rowIter.hasNext()) {
                Row row = rowIter.next();
                scanFiles.add(row);
            }
        }
        locationConstraints = configureLocationConstraints(appCtx, scanFiles);
        configuration.put(ExternalDataConstants.KEY_PARSER, ExternalDataConstants.FORMAT_DELTA);
        distributeFiles(scanFiles);
        issueWarnings(warnings, warningCollector);
    }

    private void issueWarnings(List<Warning> warnings, IWarningCollector warningCollector) {
        if (!warnings.isEmpty()) {
            for (Warning warning : warnings) {
                if (warningCollector.shouldWarn()) {
                    warningCollector.warn(warning);
                }
            }
        }
        warnings.clear();
    }

    private AlgebricksAbsolutePartitionConstraint configureLocationConstraints(ICcApplicationContext appCtx,
            List<Row> scanFiles) {
        IClusterStateManager csm = appCtx.getClusterStateManager();

        String[] locations = csm.getClusterLocations().getLocations();
        if (scanFiles.size() == 0) {
            return AlgebricksAbsolutePartitionConstraint.randomLocation(locations);
        } else if (locations.length > scanFiles.size()) {
            LOGGER.debug(
                    "configured partitions ({}) exceeds total partition count ({}); limiting configured partitions to total partition count",
                    locations.length, scanFiles.size());
            final String[] locationCopy = locations.clone();
            ArrayUtils.shuffle(locationCopy);
            locations = ArrayUtils.subarray(locationCopy, 0, scanFiles.size());
        }
        return new AlgebricksAbsolutePartitionConstraint(locations);
    }

    private void distributeFiles(List<Row> scanFiles) {
        final int partitionsCount = getPartitionConstraint().getLocations().length;
        PriorityQueue<PartitionWorkLoadBasedOnSize> workloadQueue = new PriorityQueue<>(partitionsCount,
                Comparator.comparingLong(PartitionWorkLoadBasedOnSize::getTotalSize));

        // Prepare the workloads based on the number of partitions
        for (int i = 0; i < partitionsCount; i++) {
            workloadQueue.add(new PartitionWorkLoadBasedOnSize());
        }
        for (Row scanFileRow : scanFiles) {
            PartitionWorkLoadBasedOnSize workload = workloadQueue.poll();
            FileStatus fileStatus = InternalScanFileUtils.getAddFileStatus(scanFileRow);
            workload.addScanFile(RowSerDe.serializeRowToJson(scanFileRow), fileStatus.getSize());
            workloadQueue.add(workload);
        }
        partitionWorkLoadsBasedOnSize.addAll(workloadQueue);
    }

    @Override
    public IRecordReader<?> createRecordReader(IExternalDataRuntimeContext context) throws HyracksDataException {
        try {
            int partition = context.getPartition();
            return new DeltaFileRecordReader(partitionWorkLoadsBasedOnSize.get(partition).getScanFiles(), scanState,
                    confFactory);
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public Class<?> getRecordClass() throws AsterixException {
        return Row.class;
    }

    @Override
    public Set<String> getReaderSupportedFormats() {
        return Collections.singleton(ExternalDataConstants.FORMAT_DELTA);
    }

    public static class PartitionWorkLoadBasedOnSize implements Serializable {
        private static final long serialVersionUID = 1L;
        private final List<String> scanFiles = new ArrayList<>();
        private long totalSize = 0;

        public PartitionWorkLoadBasedOnSize() {
        }

        public List<String> getScanFiles() {
            return scanFiles;
        }

        public void addScanFile(String scanFile, long size) {
            this.scanFiles.add(scanFile);
            this.totalSize += size;
        }

        public long getTotalSize() {
            return totalSize;
        }

        @Override
        public String toString() {
            return "Files: " + scanFiles.size() + ", Total Size: " + totalSize;
        }
    }

}