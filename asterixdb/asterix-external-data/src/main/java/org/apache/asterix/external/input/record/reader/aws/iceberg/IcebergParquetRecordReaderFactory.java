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
package org.apache.asterix.external.input.record.reader.aws.iceberg;

import static org.apache.asterix.common.exceptions.ErrorCode.EXTERNAL_SOURCE_ERROR;
import static org.apache.asterix.external.util.iceberg.IcebergConstants.ICEBERG_SCHEMA_ID_PROPERTY_KEY;
import static org.apache.asterix.external.util.iceberg.IcebergConstants.ICEBERG_SNAPSHOT_ID_PROPERTY_KEY;
import static org.apache.asterix.external.util.iceberg.IcebergConstants.ICEBERG_SNAPSHOT_TIMESTAMP_PROPERTY_KEY;
import static org.apache.asterix.external.util.iceberg.IcebergUtils.getProjectedFields;
import static org.apache.asterix.external.util.iceberg.IcebergUtils.snapshotIdExists;
import static org.apache.hyracks.api.util.ExceptionUtils.getMessageOrToString;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.external.IExternalFilterEvaluatorFactory;
import org.apache.asterix.external.api.IExternalDataRuntimeContext;
import org.apache.asterix.external.api.IIcebergRecordReaderFactory;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.iceberg.IcebergConstants;
import org.apache.asterix.external.util.iceberg.IcebergUtils;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.util.SnapshotUtil;

public class IcebergParquetRecordReaderFactory implements IIcebergRecordReaderFactory<Record> {

    private static final long serialVersionUID = 1L;
    private static final List<String> RECORD_READER_NAMES =
            Collections.singletonList(ExternalDataConstants.KEY_ADAPTER_NAME_AWS_S3);

    private final List<FileScanTask> fileScanTasks = new ArrayList<>();
    private final List<PartitionWorkLoadBasedOnSize> partitionWorkLoadsBasedOnSize = new ArrayList<>();

    private Schema projectedSchema;
    private Map<String, String> configurationCopy;

    private transient AlgebricksAbsolutePartitionConstraint partitionConstraint;

    public IcebergParquetRecordReaderFactory() {
    }

    @Override
    public Class<?> getRecordClass() throws AsterixException {
        return Record.class;
    }

    @Override
    public List<String> getRecordReaderNames() {
        return RECORD_READER_NAMES;
    }

    @Override
    public AlgebricksAbsolutePartitionConstraint getPartitionConstraint() {
        return partitionConstraint;
    }

    private int getPartitionsCount() {
        return getPartitionConstraint().getLocations().length;
    }

    @Override
    public IRecordReader<Record> createRecordReader(IExternalDataRuntimeContext context) throws HyracksDataException {
        try {
            int partition = context.getPartition();
            return new IcebergFileRecordReader(partitionWorkLoadsBasedOnSize.get(partition).getFileScanTasks(),
                    projectedSchema, new HashMap<>(configurationCopy));
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public Set<String> getReaderSupportedFormats() {
        return Collections.singleton(IcebergConstants.ICEBERG_PARQUET_FORMAT);
    }

    @Override
    public void configure(IServiceContext ctx, Map<String, String> configuration, IWarningCollector warningCollector,
            IExternalFilterEvaluatorFactory filterEvaluatorFactory) throws AlgebricksException, HyracksDataException {
        this.configurationCopy = new HashMap<>(configuration);
        this.partitionConstraint = ((ICcApplicationContext) ctx.getApplicationContext()).getDataPartitioningProvider()
                .getClusterLocations();

        Catalog catalog = null;
        Throwable throwable = null;
        try {
            String namespace = IcebergUtils.getNamespace(configuration);
            String tableName = configuration.get(IcebergConstants.ICEBERG_TABLE_NAME_PROPERTY_KEY);

            catalog = IcebergUtils.initializeCatalog(IcebergUtils.filterCatalogProperties(configuration), namespace);
            TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of(namespace), tableName);
            if (!catalog.tableExists(tableIdentifier)) {
                throw CompilationException.create(ErrorCode.ICEBERG_TABLE_DOES_NOT_EXIST, tableName);
            }

            Table table = catalog.loadTable(tableIdentifier);
            TableScan scan = table.newScan();
            scan = setAndPinScanSnapshot(configurationCopy, table, scan);
            long snapshotId = Long.parseLong(configurationCopy.get(ICEBERG_SNAPSHOT_ID_PROPERTY_KEY));
            Schema schemaAtSnapshot = table.schemas().get(table.snapshot(snapshotId).schemaId());

            String[] projectedFields = getProjectedFields(configuration);
            projectedSchema = schemaAtSnapshot;
            if (projectedFields != null && projectedFields.length > 0) {
                projectedSchema = projectedSchema.select(projectedFields);
            }
            scan = scan.project(projectedSchema);
            try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
                tasks.forEach(fileScanTasks::add);
            }
            distributeWorkLoad(fileScanTasks, getPartitionsCount());
        } catch (CompilationException ex) {
            throwable = ex;
            throw ex;
        } catch (Exception ex) {
            throwable = ex;
            throw CompilationException.create(EXTERNAL_SOURCE_ERROR, ex, ex.getMessage());
        } finally {
            try {
                IcebergUtils.closeCatalog(catalog);
            } catch (Exception ex) {
                if (throwable != null) {
                    throwable.addSuppressed(ex);
                } else {
                    throw CompilationException.create(EXTERNAL_SOURCE_ERROR, ex, ex.getMessage());
                }
            }
        }
    }

    private void distributeWorkLoad(List<FileScanTask> fileScanTasks, int partitionsCount) {
        PriorityQueue<PartitionWorkLoadBasedOnSize> workloadQueue = new PriorityQueue<>(partitionsCount,
                Comparator.comparingLong(PartitionWorkLoadBasedOnSize::getTotalSize));

        // Prepare the workloads based on the number of partitions
        for (int i = 0; i < partitionsCount; i++) {
            workloadQueue.add(new PartitionWorkLoadBasedOnSize());
        }

        for (FileScanTask fileScanTask : fileScanTasks) {
            PartitionWorkLoadBasedOnSize workload = workloadQueue.poll();
            workload.addFileScanTask(fileScanTask, fileScanTask.length());
            workloadQueue.add(workload);
        }
        partitionWorkLoadsBasedOnSize.addAll(workloadQueue);
    }

    @Override
    public Schema getProjectedSchema() {
        return projectedSchema;
    }

    /**
     * Sets the snapshot id (or timestamp) if present and pin it to be used by both compile and runtime phases. If no
     * snapshot is provided, the latest snapshot is used and pinned.
     *
     * @param configurationCopy configurationCopy
     * @param table table
     * @param scan scan
     * @return table scan
     * @throws CompilationException CompilationException
     */
    private TableScan setAndPinScanSnapshot(Map<String, String> configurationCopy, Table table, TableScan scan)
            throws CompilationException {
        long snapshot;
        String snapshotIdStr = configurationCopy.get(ICEBERG_SNAPSHOT_ID_PROPERTY_KEY);
        String asOfTimestampStr = configurationCopy.get(ICEBERG_SNAPSHOT_TIMESTAMP_PROPERTY_KEY);

        if (snapshotIdStr != null) {
            snapshot = Long.parseLong(snapshotIdStr);
            if (!snapshotIdExists(table, snapshot)) {
                throw CompilationException.create(ErrorCode.ICEBERG_SNAPSHOT_ID_NOT_FOUND, snapshot);
            }
        } else if (asOfTimestampStr != null) {
            try {
                snapshot = SnapshotUtil.snapshotIdAsOfTime(table, Long.parseLong(asOfTimestampStr));
                if (!snapshotIdExists(table, snapshot)) {
                    throw CompilationException.create(ErrorCode.ICEBERG_SNAPSHOT_ID_NOT_FOUND, snapshot);
                }
            } catch (IllegalArgumentException e) {
                throw CompilationException.create(EXTERNAL_SOURCE_ERROR, e, getMessageOrToString(e));
            }
        } else {
            if (table.currentSnapshot() == null) {
                throw CompilationException.create(EXTERNAL_SOURCE_ERROR, "table " + table.name() + " has no snapshots");
            }
            snapshot = table.currentSnapshot().snapshotId();
        }

        scan = scan.useSnapshot(snapshot);
        pinSnapshotId(configurationCopy, table, snapshot);
        return scan;
    }

    private void pinSnapshotId(Map<String, String> configurationCopy, Table table, long snapshotId) {
        Snapshot snapshot = table.snapshot(snapshotId);
        configurationCopy.put(ICEBERG_SNAPSHOT_ID_PROPERTY_KEY, String.valueOf(snapshot.snapshotId()));
        configurationCopy.put(ICEBERG_SCHEMA_ID_PROPERTY_KEY, Integer.toString(snapshot.schemaId()));
    }

    public static class PartitionWorkLoadBasedOnSize implements Serializable {
        private static final long serialVersionUID = 3L;
        private final List<FileScanTask> fileScanTasks = new ArrayList<>();
        private long totalSize = 0;

        public PartitionWorkLoadBasedOnSize() {
        }

        public List<FileScanTask> getFileScanTasks() {
            return fileScanTasks;
        }

        public void addFileScanTask(FileScanTask task, long size) {
            this.fileScanTasks.add(task);
            this.totalSize += size;
        }

        public long getTotalSize() {
            return totalSize;
        }

        @Override
        public String toString() {
            return "PartitionWorkLoadBasedOnSize{" + "fileScanTasks=" + fileScanTasks + ", totalSize=" + totalSize
                    + '}';
        }
    }
}
