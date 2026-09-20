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
import org.apache.iceberg.deletes.PositionDeleteIndex;
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
    // Whether a task carrying deletes may still take the pruned read. Separate from the plan's own flag because this
    // is the only one of the variant pushdowns whose failure mode is a wrong answer rather than extra IO.
    private final boolean variantProjectionPushdownWithDeletes;
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
        this.variantProjectionPushdownWithDeletes = Boolean.parseBoolean(configuration.getOrDefault(
                ExternalDataConstants.IcebergOptions.VARIANT_PROJECTION_PUSHDOWN_WITH_DELETES, Boolean.toString(
                        ExternalDataConstants.IcebergOptions.DEFAULT_VARIANT_PROJECTION_PUSHDOWN_WITH_DELETES)));
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

        CloseableIterable<Record> prunedRead = null;
        try {
            prunedRead = openPrunedReadIfEligible(tableFileIo, inFile, task, schemaAtSnapshot, projectedSchema,
                    variantProjectionPlan, variantProjectionPushdownWithDeletes);
            if (prunedRead != null) {
                iterable = prunedRead;
                recordsIterator = prunedRead.iterator();
                return;
            }
        } catch (Exception e) {
            // Pruning is only an optimization: anything at all going wrong degrades to the proven path below.
            CleanupUtils.closeSilently(prunedRead, null);
            iterable = null;
            recordsIterator = null;
            warnProjectionNotPushed(e);
        }

        iterable = openStandardRead(tableFileIo, inFile, task, schemaAtSnapshot, projectedSchema);
        recordsIterator = iterable.iterator();
    }

    /**
     * Iceberg's standard read of one task over its split range, with the task's deletes applied when it has any.
     * <p>
     * Both branches read only the row groups whose midpoint falls in {@code [start, start + length)}, so the splits of
     * one file partition its row groups between them. With position deletes the rows are matched by their absolute
     * position in the file, which Iceberg's Parquet reader takes from the footer's row-index offsets rather than by
     * counting the rows it has read, so a split that starts mid-file still drops exactly the deleted rows.
     * <p>
     * Static and package-private so a test can drive the exact call the reader makes over generated split tasks.
     *
     * @return the rows of the task; closing it closes the underlying file read
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5_1, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.REFACTORED, notes = "Extracted the standard (non-pruned) per-task read, with and without deletes, so the split-range read is testable in isolation")
    static CloseableIterable<Record> openStandardRead(FileIO io, InputFile inFile, FileScanTask task,
            Schema schemaAtSnapshot, Schema projectedSchema) {
        int deletesCount = (task.deletes() == null) ? 0 : task.deletes().size();
        if (deletesCount == 0) {
            // No deletes: read only projected schema
            return Parquet.read(inFile).project(projectedSchema).filter(task.residual())
                    .split(task.start(), task.length())
                    .createReaderFunc(fs -> GenericParquetReaders.buildReader(projectedSchema, fs)).build();
        }

        // Has deletes: read required schema, then apply delete filter
        GenericDeleteFilter deleteFilter = new GenericDeleteFilter(io, task, schemaAtSnapshot, projectedSchema);

        Schema requiredSchema = deleteFilter.requiredSchema();
        CloseableIterable<Record> rows =
                Parquet.read(inFile).project(requiredSchema).filter(task.residual()).split(task.start(), task.length())
                        .createReaderFunc(fs -> GenericParquetReaders.buildReader(requiredSchema, fs)).build();
        return deleteFilter.filter(rows);
    }

    /**
     * Whether this task may take the variant-pruned read path.
     * <p>
     * A task with no deletes qualifies whenever the plan narrows something. A task that <em>does</em> carry deletes
     * qualifies only while {@code pushdownWithDeletes} is on, which is the
     * {@code variantProjectionPushdownWithDeletes} flag: turning it off restores the previous behaviour, where any
     * delete file sent the whole task down the standard delete-aware read, while leaving pruning untouched on every
     * delete-free file.
     * <p>
     * Deletes are per-task, not per-table, so the paths genuinely interleave within one scan: deletion vectors are
     * file-scoped, so a table with vectors on some files still has delete-free files alongside them. Equality deletes
     * attach to every file of a partition, so with the flag off they disable pruning across the whole scan.
     * <p>
     * Answering {@code true} is permission to <em>try</em>, not a guarantee: the per-file clip may turn out to be a
     * no-op, and a position-delete file whose row groups do not record their first row index is declined at open time.
     * Extracted from the read path so routing is assertable on its own — nothing downstream reveals which branch a
     * file took.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.REFACTORED, notes = "Routing decision now admits delete-bearing tasks when variantProjectionPushdownWithDeletes is on, and still declines them when it is off")
    static boolean shouldTryPrunedVariantRead(VariantProjectionPlan plan, List<DeleteFile> deletes,
            boolean pushdownWithDeletes) {
        if (plan.isEmpty()) {
            return false;
        }
        return deletes == null || deletes.isEmpty() || pushdownWithDeletes;
    }

    /**
     * The one routing decision for a task: the variant-pruned read when — and only when — it is allowed and pays.
     * <p>
     * Returns the pruned read to install, or {@code null} when the task must take the standard read: the plan narrows
     * nothing, the task carries deletes while {@code variantProjectionPushdownWithDeletes} is off, or the per-file
     * clip turns out to be a no-op. With deletes present and allowed, the read is
     * {@link #openPrunedDeleteAwareRead the delete-aware composition}; otherwise it is the plain pruned reader.
     * <p>
     * Package-private and static, like {@link #shouldTryPrunedVariantRead} and {@link #openPrunedDeleteAwareRead},
     * because this is the decision the flag is supposed to control, and the test that proves the flag works has to
     * drive <em>this</em> method — flag on must yield the pruned read, flag off must yield {@code null} and therefore
     * the full-width standard read — rather than a re-assembled copy of it. Every decline happens before a row is
     * produced; the caller's fallback is always the proven path with nothing emitted.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5_1, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.REFACTORED, notes = "Folded the pruned/pruned-with-deletes routing into one testable entry point so the "
            + "variantProjectionPushdownWithDeletes flag can be proven end to end: off must restore the standard read")
    static CloseableIterable<Record> openPrunedReadIfEligible(FileIO io, InputFile inFile, FileScanTask task,
            Schema tableSchema, Schema projectedSchema, VariantProjectionPlan plan, boolean pushdownWithDeletes)
            throws IOException {
        if (!shouldTryPrunedVariantRead(plan, task.deletes(), pushdownWithDeletes)) {
            return null;
        }
        boolean hasDeletes = task.deletes() != null && !task.deletes().isEmpty();
        if (hasDeletes) {
            return openPrunedDeleteAwareRead(io, inFile, task, tableSchema, projectedSchema, plan);
        }
        VariantProjectedParquetReader prunedReader = VariantProjectedParquetReader.open(inFile, projectedSchema,
                task.residual(), task.start(), task.length(), true, plan);
        if (!prunedReader.canPrune()) {
            // Nothing to gain on this file; prefer the standard path over the replicated read logic.
            prunedReader.close();
            return null;
        }
        return prunedReader;
    }

    /**
     * Builds the pruned, delete-aware read for one task, or returns {@code null} when this file must take the standard
     * delete path.
     * <p>
     * Package-private and static for the same reason {@link #shouldTryPrunedVariantRead} is: it is the composition
     * itself, and a test that reassembled these calls by hand would be asserting its own copy rather than the shipped
     * one. Given a task planned by Iceberg, a test can drive exactly what the reader drives.
     * <p>
     * Returning {@code null} rather than throwing keeps the fail-safe honest: every decline is made before a row is
     * produced, so the caller can still fall back to the proven path with nothing emitted.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5_1, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.REFACTORED, notes = "Extracted the pruned delete-aware composition so a test can drive the shipped code from a real "
            + "Iceberg scan task rather than reassembling the same calls itself")
    static CloseableIterable<Record> openPrunedDeleteAwareRead(FileIO io, InputFile inFile, FileScanTask task,
            Schema tableSchema, Schema projectedSchema, VariantProjectionPlan plan) throws IOException {
        PositionlessGenericDeleteFilter deleteFilter =
                new PositionlessGenericDeleteFilter(io, task, tableSchema, projectedSchema);

        // Open first, load deletes second. Both the clip being a no-op and a missing row-index offset are decided at
        // open time, and either sends this task to the standard delete path, which loads its own copy of every
        // delete file. Loading here before knowing would pay that IO twice on exactly the files that gain nothing.
        VariantProjectedParquetReader prunedReader =
                VariantProjectedParquetReader.open(inFile, deleteFilter.requiredSchema(), task.residual(), task.start(),
                        task.length(), true, plan, deleteFilter.hasPosDeletes());
        if (!prunedReader.canPrune()) {
            // Nothing to gain on this file, or row positions are unavailable for a position-delete file.
            prunedReader.close();
            return null;
        }

        try {
            if (deleteFilter.hasPosDeletes()) {
                PositionDeleteIndex deletedPositions = deleteFilter.deletedRowPositions();
                if (deletedPositions == null) {
                    // Position deletes were reported but no bitmap came back: do not guess, take the proven path.
                    prunedReader.close();
                    return null;
                }
                prunedReader.withDeletedPositions(deletedPositions);
            }
            // CloseableIterable.filter adds the reader as a closeable of the wrapper, so closing the returned iterable
            // closes the reader underneath it. eqDeletedRowFilter() loads the equality set right here, after the
            // decision to prune has been made — and before any row is produced.
            return deleteFilter.hasEqDeletes()
                    ? CloseableIterable.filter(prunedReader, deleteFilter.eqDeletedRowFilter()) : prunedReader;
        } catch (RuntimeException | IOException e) {
            // A delete file that cannot be loaded must not leak the open reader; the caller falls back and the
            // standard path reports whatever is wrong with the delete file itself.
            CleanupUtils.closeSilently(prunedReader, null);
            throw e;
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
