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

import static org.apache.asterix.external.util.iceberg.IcebergUtils.getProjectedFields;
import static org.apache.asterix.external.util.iceberg.IcebergUtils.setSnapshot;

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
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;

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
            scan = setSnapshot(configuration, scan);
            String[] projectedFields = getProjectedFields(configuration);
            projectedSchema = table.schema();
            if (projectedFields != null && projectedFields.length > 0) {
                projectedSchema = projectedSchema.select(projectedFields);
            }
            scan.project(projectedSchema);
            try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
                tasks.forEach(fileScanTasks::add);
            }
            distributeWorkLoad(fileScanTasks, getPartitionsCount());
        } catch (CompilationException ex) {
            throwable = ex;
            throw ex;
        } catch (Exception ex) {
            throwable = ex;
            throw CompilationException.create(ErrorCode.EXTERNAL_SOURCE_ERROR, ex, ex.getMessage());
        } finally {
            try {
                IcebergUtils.closeCatalog(catalog);
            } catch (Exception ex) {
                if (throwable != null) {
                    throwable.addSuppressed(ex);
                } else {
                    throw CompilationException.create(ErrorCode.EXTERNAL_SOURCE_ERROR, ex, ex.getMessage());
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
