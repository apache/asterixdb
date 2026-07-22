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
import static org.apache.asterix.external.util.iceberg.IcebergSnapshotUtils.snapshotIdExists;
import static org.apache.asterix.external.util.iceberg.IcebergUtils.getProjectedFields;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import org.apache.asterix.external.input.filter.IcebergTableFilterEvaluatorFactory;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.iceberg.IcebergConstants;
import org.apache.asterix.external.util.iceberg.IcebergSnapshotUtils;
import org.apache.asterix.external.util.iceberg.IcebergUtils;
import org.apache.asterix.external.util.iceberg.VariantBoundsEvaluator;
import org.apache.asterix.external.util.iceberg.VariantPredicateRewriter;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.And;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;

public class IcebergParquetRecordReaderFactory implements IIcebergRecordReaderFactory<Record> {

    private static final long serialVersionUID = 1L;
    private static final List<String> RECORD_READER_NAMES = Arrays.asList(ExternalDataConstants.KEY_ADAPTER_NAME_AWS_S3,
            ExternalDataConstants.KEY_ADAPTER_NAME_AZURE_BLOB, ExternalDataConstants.KEY_ADAPTER_NAME_AZURE_DATALAKE,
            ExternalDataConstants.KEY_ADAPTER_NAME_GCS);

    private final List<FileScanTask> fileScanTasks = new ArrayList<>();
    private final List<PartitionWorkLoadBasedOnSize> partitionWorkLoadsBasedOnSize = new ArrayList<>();

    private Schema projectedSchema;
    private Map<String, String> originalConfiguration;
    private Map<String, String> catalogProperties;

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
                    projectedSchema, new HashMap<>(originalConfiguration),
                    context.getTaskContext().getWarningCollector());
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public Set<String> getReaderSupportedFormats() {
        return Collections.singleton(IcebergConstants.ICEBERG_PARQUET_FORMAT);
    }

    /**
     * Whether data files may be skipped using the manifest bounds of a shredded VARIANT sub-field. Default on; the
     * off switch exists because this ships in releases rather than patches, and because the off path is a useful
     * oracle — the same query must return the same rows, only reading more files.
     * <p>
     * Separate from {@code variantProjectionPushdown} on purpose: that one only changes how many columns are read,
     * while this one changes which files are read at all.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Reads the variantStatsPushdown WITH-clause flag (default on) gating manifest-bounds data-file skipping")
    private boolean isVariantStatsPushdownEnabled() {
        return Boolean.parseBoolean(
                originalConfiguration.getOrDefault(ExternalDataConstants.IcebergOptions.VARIANT_STATS_PUSHDOWN,
                        Boolean.toString(ExternalDataConstants.IcebergOptions.DEFAULT_VARIANT_STATS_PUSHDOWN)));
    }

    /**
     * Drops the filter rather than let scan planning throw, if it cannot be bound against the schema.
     * <p>
     * This has a known, reachable trigger, not just hypothetical ones: {@code IcebergTableFilterBuilder} takes the
     * reference name from the field-access path the query wrote and never checks it against the Iceberg schema, so a
     * predicate on a field the table does not have — a typo, or a column added in a later schema than the one being
     * read — arrives here unbindable. Pushing it fails the whole query with "Cannot find field"; dropping it returns
     * the right answer and merely reads more, since the engine still applies the predicate to the rows.
     * <p>
     * A warning is raised so the lost pushdown is visible to whoever wrote the query rather than showing up only as
     * unexplained slowness.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Drops an unbindable filter instead of failing scan planning, and warns that pushdown was skipped. Reachable whenever a predicate names a field absent from the Iceberg schema, which the filter builder does not validate")
    static Expression bindableOrNoFilter(Expression expression, Schema schema, IWarningCollector warningCollector) {
        if (expression == null) {
            return Expressions.alwaysTrue();
        }
        // No fast path for "the whole thing binds" here: withoutUnbindable checks exactly that as its first step and
        // returns the expression untouched, so checking twice would just bind the whole expression twice.
        return withoutUnbindable(expression, schema, warningCollector);
    }

    /** @return whether Iceberg can bind {@code expression} against the schema, i.e. whether the scan would accept it. */
    private static boolean bindable(Expression expression, Schema schema) {
        try {
            // caseSensitive=true matches how the scan itself binds, so this rehearses the real call.
            Binder.bind(schema.asStruct(), expression, true);
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    /**
     * Drops only the parts that cannot bind, keeping the rest pushable.
     * <p>
     * {@code Binder.bind} is all-or-nothing over the whole expression, so one unbindable reference would otherwise cost
     * the pushdown of everything ANDed with it: {@code WHERE good = 1 AND nosuchcolumn = 2} would read every file
     * despite {@code good = 1} being perfectly pushable. Weakening rules are the same discipline used when stripping
     * variant sub-field predicates — a pushed filter may only ever admit MORE files:
     * <ul>
     * <li>{@code AND}: drop each unbindable side; losing a conjunct weakens the filter, which is safe.</li>
     * <li>{@code OR}: if either side fails to bind the whole disjunction is dropped, since removing a disjunct would
     * strengthen the filter and could wrongly skip files.</li>
     * <li>{@code NOT}: dropped whole when its child fails to bind, since a weaker child yields a stronger negation.</li>
     * </ul>
     * One warning is raised per dropped part, naming it, so the lost pushdown is attributable rather than showing up
     * as unexplained slowness.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Drops only the unbindable conjuncts instead of the whole filter, so a predicate on a column absent from the Iceberg schema no longer costs the pushdown of everything ANDed with it")
    private static Expression withoutUnbindable(Expression expression, Schema schema,
            IWarningCollector warningCollector) {
        if (bindable(expression, schema)) {
            return expression;
        }
        if (expression instanceof And) {
            And and = (And) expression;
            return Expressions.and(withoutUnbindable(and.left(), schema, warningCollector),
                    withoutUnbindable(and.right(), schema, warningCollector));
        }
        // OR and NOT cannot be weakened piecewise, and a leaf has no parts: drop whole and say which.
        warn(expression, warningCollector);
        return Expressions.alwaysTrue();
    }

    private static void warn(Expression dropped, IWarningCollector warningCollector) {
        if (warningCollector.shouldWarn()) {
            warningCollector.warn(Warning.of(null, ErrorCode.ICEBERG_FILTER_NOT_PUSHED, dropped.toString()));
        }
    }

    @Override
    public void configure(IServiceContext ctx, Map<String, String> configuration, IWarningCollector warningCollector,
            IExternalFilterEvaluatorFactory filterEvaluatorFactory) throws AlgebricksException, HyracksDataException {
        this.originalConfiguration = new HashMap<>(configuration);
        this.partitionConstraint = ((ICcApplicationContext) ctx.getApplicationContext()).getDataPartitioningProvider()
                .getClusterLocations();

        Catalog catalog = null;
        Throwable throwable = null;
        try {
            String namespace = IcebergUtils.getNamespace(configuration);
            String tableName = configuration.get(IcebergConstants.ICEBERG_TABLE_NAME_PROPERTY_KEY);

            catalogProperties = IcebergUtils.filterCatalogProperties(configuration);
            catalog = IcebergUtils.initializeCatalog(catalogProperties, namespace);
            Namespace parsedNamespace = IcebergUtils.parseNamespace(namespace);
            TableIdentifier tableIdentifier = TableIdentifier.of(parsedNamespace, tableName);
            if (!catalog.tableExists(tableIdentifier)) {
                throw CompilationException.create(ErrorCode.ICEBERG_TABLE_DOES_NOT_EXIST, tableName);
            }

            Table table = catalog.loadTable(tableIdentifier);
            TableScan scan = table.newScan();
            scan = setAndPinScanSnapshot(originalConfiguration, table, scan);
            long snapshotId = Long.parseLong(originalConfiguration.get(ICEBERG_SNAPSHOT_ID_PROPERTY_KEY));
            Schema schemaAtSnapshot = table.schemas().get(table.snapshot(snapshotId).schemaId());

            String[] projectedFields = getProjectedFields(configuration);
            projectedSchema = schemaAtSnapshot;
            if (projectedFields != null && projectedFields.length > 0) {
                projectedSchema = projectedSchema.select(projectedFields);
            }
            scan = scan.project(projectedSchema);
            Expression filterExpression =
                    ((IcebergTableFilterEvaluatorFactory) filterEvaluatorFactory).getFilterExpression();
            Expression variantFilter = null;
            if (filterExpression != null) {
                // Predicates on a shredded VARIANT sub-field arrive as a dotted reference (variant_field.status),
                // which cannot bind against the schema because a variant is one opaque VariantType. Rewrite those
                // into Iceberg extract terms here — the first point where the Iceberg schema is known — so the
                // manifest's per-subfield bounds can prune whole data files. Anything not rewritable is unchanged.
                //
                // Skipped when the flag is off, so that path does not depend on the rewriter at all: the guard below
                // then strips every variant sub-field predicate and the scan keeps only the ordinary-column filter.
                if (isVariantStatsPushdownEnabled()) {
                    filterExpression = VariantPredicateRewriter.rewrite(filterExpression, schemaAtSnapshot,
                            ((IcebergTableFilterEvaluatorFactory) filterEvaluatorFactory).getFilterPathSegments());
                }
                // TODO(iceberg-15384): remove this whole extract/non-extract split, the includeColumnStats call and
                // the per-task evaluation loop below, and simply do scan.filter(filterExpression), once
                // https://github.com/apache/iceberg/pull/15384 ships. It fixes both blockers: ExpressionUtil.describe
                // throwing on extract terms (which makes planFiles() fail while logging the filter) and
                // InclusiveMetricsEvaluator reading variant manifest bounds with the wrong byte order. Pushing the
                // predicate properly also restores manifest-level short-circuiting, which this local loop cannot do
                // because it only sees files that planning already listed.
                if (VariantPredicateRewriter.containsExtractTerm(filterExpression)) {
                    // Iceberg 1.11.0 cannot carry an extract term through TableScan: SnapshotScan unconditionally
                    // logs the scan via ExpressionUtil.toSanitizedString, whose describe(Term) throws
                    // "Unsupported term: extract(..)" (apache/iceberg PR #15384 fixes this upstream). Push only the
                    // extract-free part — which is weaker, so it can only admit more files — and apply the full
                    // expression per data file below with InclusiveMetricsEvaluator, which does support extract.
                    variantFilter = filterExpression;
                    filterExpression = VariantPredicateRewriter.withoutExtractTerms(filterExpression);
                }
                // Correctness guard, always applied. The rewrite declines some shapes on purpose (a sub-field IS NULL,
                // a literal that would compare against the wrong bound type, a name dot notation cannot express) and
                // each leaves a dotted variant reference behind. Iceberg cannot bind those, so pushing one fails
                // planning rather than just skipping the optimization. Dropping them only admits more files; the
                // engine still applies the predicate to the rows.
                filterExpression =
                        VariantPredicateRewriter.withoutVariantSubFieldPredicates(filterExpression, schemaAtSnapshot);
                scan = scan.filter(bindableOrNoFilter(filterExpression, schemaAtSnapshot, warningCollector));
            }
            if (variantFilter != null) {
                // TODO(iceberg-15384): remove — needed only because we evaluate the bounds ourselves.
                // Planned data files drop their column statistics unless explicitly requested (BaseScan's
                // shouldReturnColumnStats() defaults to false). Without the variant column's bounds the evaluator
                // below can never prove a file cannot match, so it would keep every file. Ask only for the columns
                // the extract terms reference, to avoid carrying stats for the whole table.
                Set<String> statsColumns = VariantPredicateRewriter.extractTermColumns(variantFilter);
                scan = statsColumns.isEmpty() ? scan.includeColumnStats() : scan.includeColumnStats(statsColumns);
            }
            // TODO(iceberg-15384): remove — Iceberg does this during planning once the fix ships.
            // Compares each data file's manifest bounds for the requested variant sub-fields and returns false only
            // when they prove no row can match, so dropping those tasks is safe. Iceberg's own
            // InclusiveMetricsEvaluator cannot be used here: on 1.11.0 it throws reading variant bounds from a
            // manifest (wrong byte order; apache/iceberg PR #15384).
            VariantBoundsEvaluator variantEvaluator = variantFilter == null ? null
                    : new VariantBoundsEvaluator(schemaAtSnapshot, variantFilter, warningCollector);
            try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
                for (FileScanTask task : tasks) {
                    if (variantEvaluator == null || variantEvaluator.mightMatch(task.file())) {
                        fileScanTasks.add(task);
                    }
                }
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
                IcebergUtils.closeAndCleanup(catalog, catalogProperties);
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
        Optional<Long> snapshotOptional = IcebergSnapshotUtils.getSnapshotId(configurationCopy, table);
        if (snapshotOptional.isPresent()) {
            snapshot = snapshotOptional.get();
            if (!snapshotIdExists(table, snapshot)) {
                throw CompilationException.create(ErrorCode.ICEBERG_SNAPSHOT_ID_NOT_FOUND, snapshot, table.name());
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
