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

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.dataflow.AbstractFeedDataFlowController;
import org.apache.asterix.external.input.record.GenericRecord;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.external.util.IFeedLogManager;
import org.apache.asterix.external.util.iceberg.IcebergConstants;
import org.apache.asterix.external.util.iceberg.IcebergUtils;
import org.apache.asterix.external.util.iceberg.VariantProjectionPlan;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.hyracks.api.util.ExceptionUtils;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericDeleteFilter;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Iceberg record reader.
 * The reader returns records in Iceberg Record format.
 */
public class IcebergFileRecordReader implements IRecordReader<Record> {

    private static final Logger LOGGER = LogManager.getLogger();

    private final List<FileScanTask> fileScanTasks;
    private final Schema projectedSchema;
    private final Map<String, String> originalConfiguration;
    private final IRawRecord<Record> record;

    private Map<String, String> catalogProperties;
    private int nextTaskIndex = 0;
    private Catalog catalog;
    private FileIO tableFileIo;
    private Schema schemaAtSnapshot;
    private CloseableIterable<Record> iterable;
    private Iterator<Record> recordsIterator;

    // Variant sub-path projection pushdown plan (reading shredded). Computed once here; consumed by the read path to
    // clip each file's shredded typed_value. Empty when the flag is off or nothing is narrowable, in which case the
    // reader behaves exactly as before.
    private final VariantProjectionPlan variantProjectionPlan;
    private final IWarningCollector warningCollector;

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.REFACTORED, notes = "Read the variantProjectionPushdown flag (default on) and build the per-scan VariantProjectionPlan from the projected Iceberg schema + requested-fields type; any failure falls back to an empty plan so the optimization can never break the read")
    public IcebergFileRecordReader(List<FileScanTask> fileScanTasks, Schema projectedSchema,
            Map<String, String> configuration, IWarningCollector warningCollector) throws HyracksDataException {
        this.fileScanTasks = fileScanTasks;
        this.projectedSchema = projectedSchema;
        this.originalConfiguration = configuration;
        this.warningCollector = warningCollector;
        this.record = new GenericRecord<>();
        try {
            initializeTable();
        } catch (CompilationException e) {
            Throwable throwable = closeResources(e);
            throw HyracksDataException.create(throwable);
        }
        this.variantProjectionPlan = buildVariantProjectionPlan();
    }

    // Best-effort: any problem decoding the requested-fields type just disables pushdown for this scan (empty plan),
    // never fails the reader — it is only an optimization.
    private VariantProjectionPlan buildVariantProjectionPlan() {
        try {
            boolean enabled = Boolean.parseBoolean(originalConfiguration.getOrDefault(
                    ExternalDataConstants.IcebergOptions.VARIANT_PROJECTION_PUSHDOWN,
                    Boolean.toString(ExternalDataConstants.IcebergOptions.DEFAULT_VARIANT_PROJECTION_PUSHDOWN)));
            if (!enabled) {
                return VariantProjectionPlan.none();
            }
            ARecordType projectedType = ExternalDataUtils
                    .getExpectedType(originalConfiguration.get(ExternalDataConstants.KEY_REQUESTED_FIELDS));
            return VariantProjectionPlan.from(projectedSchema, projectedType, true);
        } catch (Exception e) {
            warnProjectionNotPushed(e);
            return VariantProjectionPlan.none();
        }
    }

    /**
     * Reports that variant projection pushdown was skipped because something went wrong, rather than because there was
     * nothing to prune.
     * <p>
     * Raised as a warning, not just a log line, because the failure is otherwise invisible: results stay correct and
     * only the volume read changes, so a pushdown that silently stopped working looks like ordinary slowness. The
     * message deliberately carries <em>no</em> file name or exception text, so every occurrence is the identical
     * warning and the collector folds repeats into a count instead of emitting one per file. The detail that varies
     * goes to the debug log instead.
     * <p>
     * Cost is bounded: the two callers run once per scan task and once per reader, never per record.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Surfaces a silent projection-pushdown fallback as a deduplicated warning plus a debug log with the cause; message is constant so the warning collector counts repeats rather than repeating them")
    // Package-private so a test can execute the warn path itself: the two callers are defensive catches that
    // nothing reachable makes throw, so this line would otherwise never run under test.
    void warnProjectionNotPushed(Exception cause) {
        LOGGER.debug("variant projection pushdown skipped", cause);
        if (warningCollector != null && warningCollector.shouldWarn()) {
            warningCollector.warn(Warning.of(null, ErrorCode.ICEBERG_VARIANT_PROJECTION_NOT_PUSHED));
        }
    }

    private void initializeTable() throws CompilationException {
        if (fileScanTasks.isEmpty()) {
            return;
        }

        String namespace = IcebergUtils.getNamespace(originalConfiguration);
        String tableName = originalConfiguration.get(IcebergConstants.ICEBERG_TABLE_NAME_PROPERTY_KEY);
        catalogProperties = IcebergUtils.filterCatalogProperties(originalConfiguration);
        catalog = IcebergUtils.initializeCatalog(catalogProperties, namespace);
        Namespace parsedNamespace = IcebergUtils.parseNamespace(namespace);
        TableIdentifier tableIdentifier = TableIdentifier.of(parsedNamespace, tableName);
        if (!catalog.tableExists(tableIdentifier)) {
            throw CompilationException.create(ErrorCode.ICEBERG_TABLE_DOES_NOT_EXIST, tableName);
        }
        Table table = catalog.loadTable(tableIdentifier);
        tableFileIo = table.io();

        // we always have a snapshot id since we pin it at compile time
        long snapshotId = getSnapshotId(originalConfiguration);
        Snapshot snapshot = table.snapshot(snapshotId);
        if (snapshot == null) {
            // Snapshot might have been expired/GC'd between compile and runtime
            throw CompilationException.create(ErrorCode.ICEBERG_SNAPSHOT_ID_NOT_FOUND, snapshotId, table.name());
        }

        this.schemaAtSnapshot = table.schemas().get(snapshot.schemaId());
        if (schemaAtSnapshot == null) {
            throw CompilationException.create(ErrorCode.EXTERNAL_SOURCE_ERROR,
                    "Missing schemaId=" + snapshot.schemaId() + " for snapshotId=" + snapshotId);
        }
    }

    @Override
    public boolean hasNext() throws Exception {
        // iterator has more records
        if (recordsIterator != null && recordsIterator.hasNext()) {
            return true;
        }

        // go to next task
        // if a file is empty, we will go to the next task
        while (nextTaskIndex < fileScanTasks.size()) {

            // close previous iterable
            if (iterable != null) {
                iterable.close();
                iterable = null;
            }

            // Load next task
            setNextRecordsIterator();

            // if the new iterator has rows → good
            if (recordsIterator != null && recordsIterator.hasNext()) {
                return true;
            }

            // else: this task is empty → continue the loop to the next task
        }

        // no more tasks & no more rows
        return false;
    }

    @Override
    public IRawRecord<Record> next() throws IOException, InterruptedException {
        Record icebergRecord = recordsIterator.next();
        record.set(icebergRecord);
        return record;
    }

    @Override
    public boolean stop() {
        return false;
    }

    @Override
    public void close() throws IOException {
        Throwable throwable = CleanupUtils.closeSilently(iterable, null);
        throwable = CleanupUtils.closeSilently(tableFileIo, throwable);
        try {
            if (catalog != null) {
                IcebergUtils.closeAndCleanup(catalog, catalogProperties);
            }
        } catch (Exception ex) {
            throwable = ExceptionUtils.suppress(throwable, ex);
        }
        if (throwable != null) {
            throw HyracksDataException.create(throwable);
        }
    }

    @Override
    public void setController(AbstractFeedDataFlowController controller) {
        // no-op
    }

    @Override
    public void setFeedLogManager(IFeedLogManager feedLogManager) throws HyracksDataException {
        // no-op
    }

    @Override
    public boolean handleException(Throwable th) {
        return false;
    }

    private void setNextRecordsIterator() {
        FileScanTask task = fileScanTasks.get(nextTaskIndex++);
        InputFile inFile = tableFileIo.newInputFile(task.file().location());

        if (shouldTryPrunedVariantRead(variantProjectionPlan, task.deletes()) && tryPrunedVariantRead(inFile, task)) {
            return;
        }

        int deletesCount = (task.deletes() == null) ? 0 : task.deletes().size();
        if (deletesCount == 0) {
            // No deletes: read only projected schema
            iterable = Parquet.read(inFile).project(projectedSchema).filter(task.residual())
                    .split(task.start(), task.length())
                    .createReaderFunc(fs -> GenericParquetReaders.buildReader(projectedSchema, fs)).build();
            recordsIterator = iterable.iterator();
            return;
        }

        // Has deletes: read required schema, then apply delete filter
        GenericDeleteFilter deleteFilter =
                new GenericDeleteFilter(tableFileIo, task, schemaAtSnapshot, projectedSchema);

        Schema requiredSchema = deleteFilter.requiredSchema();
        iterable =
                Parquet.read(inFile).project(requiredSchema).filter(task.residual()).split(task.start(), task.length())
                        .createReaderFunc(fs -> GenericParquetReaders.buildReader(requiredSchema, fs)).build();
        recordsIterator = deleteFilter.filter(iterable).iterator();
    }

    /**
     * Whether this task may take the variant-pruned read path. Two conditions, and the second is the interesting one.
     * <p>
     * Variant sub-path projection pushdown is skipped whenever a task carries deletes, because that read is
     * materialized against {@code deleteFilter.requiredSchema()} — a superset of the projection, which for position
     * deletes also includes the synthetic {@code _pos} column that the schema clipper cannot express. Skipping keeps
     * the delete path exactly as it was.
     * <p>
     * Deletes are per-task, not per-table, so the two paths genuinely interleave within one scan: deletion vectors are
     * file-scoped, so a table with DVs on some files yields pruned reads for the rest. Equality deletes attach to every
     * file of a partition and so disable pruning across the whole scan. Extracted from the read path so that routing is
     * assertable on its own — nothing downstream reveals which branch a file took.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.REFACTORED, notes = "Extracted the pruned-vs-delete-path routing decision so a test can assert that a DV-bearing task falls back while DV-free tasks in the same scan are pruned")
    static boolean shouldTryPrunedVariantRead(VariantProjectionPlan plan, List<DeleteFile> deletes) {
        return !plan.isEmpty() && (deletes == null || deletes.isEmpty());
    }

    /**
     * Attempts a read with the file's unreferenced shredded variant sub-columns pruned away.
     *
     * @return {@code true} if the pruned read was installed; {@code false} if the caller must use the standard read
     *         path — either because clipping could not narrow this file (serialized column, the requested paths are
     *         residual-only, array/scalar shredding, ...) or because anything at all went wrong. Pruning is purely an
     *         optimization, so every failure degrades to the proven path instead of failing the query.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Installs the variant-pruned read when the per-file clip actually narrows the schema, and falls back to Iceberg's standard read path on a no-op clip or any failure")
    private boolean tryPrunedVariantRead(InputFile inFile, FileScanTask task) {
        VariantProjectedParquetReader prunedReader = null;
        try {
            prunedReader = VariantProjectedParquetReader.open(inFile, projectedSchema, task.residual(), task.start(),
                    task.length(), true, variantProjectionPlan);
            if (!prunedReader.canPrune()) {
                // Nothing to gain on this file; prefer the standard path over the replicated read logic.
                prunedReader.close();
                return false;
            }
            iterable = prunedReader;
            recordsIterator = prunedReader.iterator();
            return true;
        } catch (Exception e) {
            CleanupUtils.closeSilently(prunedReader, null);
            iterable = null;
            recordsIterator = null;
            warnProjectionNotPushed(e);
            return false;
        }
    }

    private long getSnapshotId(Map<String, String> configuration) {
        String snapshotStr = configuration.get(IcebergConstants.ICEBERG_SNAPSHOT_ID_PROPERTY_KEY);
        if (snapshotStr != null) {
            return Long.parseLong(snapshotStr);
        }
        throw new IllegalStateException("Snapshot must've been pinned during compilation phase");
    }

    private Throwable closeResources(Throwable throwable) {
        if (tableFileIo != null) {
            throwable = CleanupUtils.closeSilently(tableFileIo, throwable);
        }
        if (catalog != null) {
            try {
                IcebergUtils.closeAndCleanup(catalog, catalogProperties);
            } catch (Exception ex) {
                throwable = ExceptionUtils.suppress(throwable, ex);
            }
        }
        return throwable;
    }
}
