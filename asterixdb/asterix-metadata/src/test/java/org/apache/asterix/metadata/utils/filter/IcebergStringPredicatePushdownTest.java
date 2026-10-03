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
package org.apache.asterix.metadata.utils.filter;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.apache.asterix.external.util.iceberg.StringOrderCorpus;
import org.apache.asterix.external.util.iceberg.StringOrderCorpus.Op;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * String predicates pushed into Iceberg for an ordinary (non-variant) column, checked against the query engine's own
 * string order (see {@link StringOrderCorpus}).
 * <p>
 * {@link IcebergTableFilterBuilder} pushes {@code =, !=, <, <=, >, >=} and {@code starts_with} on strings. Iceberg then skips data at
 * two levels before the engine sees a row, and both compare in code-point order: whole files at scan planning
 * ({@code InclusiveMetricsEvaluator} over manifest bounds), and row groups inside each file (the metrics and dictionary
 * row-group filters, applied through {@code Parquet.read().filter(residual)}). The engine then re-evaluates the
 * predicate on what is left, in UTF-16 order. A row the engine would return but either level skipped is lost.
 * <p>
 * The predicate comes from the builder itself (see {@link #pushedByBuilder}), so a builder that declines to
 * push an unsafe one passes. Both levels are then driven as production drives them: planning through
 * {@code TableScan.filter(..)}, and the read through the same {@code Parquet.read(..).filter(residual)} call as
 * {@code IcebergFileRecordReader.openStandardRead}, which is package-private to the reader.
 */
public class IcebergStringPredicatePushdownTest {

    private static final List<List<String>> LAYOUTS = StringOrderCorpus.fileLayouts();
    private static Table table;

    @BeforeClass
    public static void writeTable() throws Exception {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "s", Types.StringType.get()), Types.NestedField.optional(3, "st",
                        Types.StructType.of(Types.NestedField.optional(4, "s", Types.StringType.get()))));
        File dir = java.nio.file.Files.createTempDirectory("string-pushdown-order").toFile();
        table = new HadoopTables(new org.apache.hadoop.conf.Configuration()).create(schema,
                PartitionSpec.unpartitioned(), Map.of(TableProperties.FORMAT_VERSION, "2"),
                new File(dir, "t").getAbsolutePath());
        AppendFiles append = table.newAppend();
        for (int i = 0; i < LAYOUTS.size(); i++) {
            append.appendFile(writeFile(i, LAYOUTS.get(i)));
        }
        append.commit();
    }

    @Test
    public void topLevelColumn() throws Exception {
        assertNoRowLost("s", r -> (String) r.getField("s"));
    }

    @Test
    public void columnInsideStruct() throws Exception {
        assertNoRowLost("st.s", r -> (String) ((Record) r.getField("st")).getField("s"));
    }

    private static void assertNoRowLost(String column, Function<Record, String> valueOf) throws Exception {
        List<String> lostAtPlanning = new ArrayList<>();
        List<String> lostInReader = new ArrayList<>();
        List<String> notPruned = new ArrayList<>();
        int checks = 0;
        for (Op op : Op.values()) {
            for (String literal : StringOrderCorpus.VALUES) {
                Expression pushed = pushedByBuilder(column, op, literal);
                if (pushed == null) {
                    pushed = Expressions.alwaysTrue();
                }
                Set<String> planned = new HashSet<>();
                Set<String> returned = new HashSet<>();
                try (CloseableIterable<FileScanTask> tasks = table.newScan().filter(pushed).planFiles()) {
                    for (FileScanTask task : tasks) {
                        String file = new File(task.file().location()).getName();
                        planned.add(file);
                        try (CloseableIterable<Record> rows =
                                Parquet.read(table.io().newInputFile(task.file().location())).project(table.schema())
                                        .filter(task.residual()).split(task.start(), task.length())
                                        .createReaderFunc(fs -> GenericParquetReaders.buildReader(table.schema(), fs))
                                        .build()) {
                            for (Record r : rows) {
                                returned.add(file + "#" + r.getField("id"));
                            }
                        }
                    }
                }
                for (int i = 0; i < LAYOUTS.size(); i++) {
                    List<String> values = LAYOUTS.get(i);
                    String file = "L" + i + ".parquet";
                    checks++;
                    for (int id = 0; id < values.size(); id++) {
                        if (!StringOrderCorpus.engineMatches(values.get(id), op, literal)) {
                            continue;
                        }
                        if (!planned.contains(file)) {
                            lostAtPlanning.add(describe(column, op, literal, values, id));
                        } else if (!returned.contains(file + "#" + id)) {
                            lostInReader.add(describe(column, op, literal, values, id));
                        }
                    }
                    if (planned.contains(file) && StringOrderCorpus.orderInsensitive(values, literal)
                            && StringOrderCorpus.boundsExclude(values, op, literal)) {
                        notPruned.add(describe(column, op, literal, values, -1));
                    }
                }
            }
        }
        Assert.assertTrue(report("DATA LOSS at scan planning (file skipped)", lostAtPlanning, checks),
                lostAtPlanning.isEmpty());
        Assert.assertTrue(report("DATA LOSS in the reader (row group skipped)", lostInReader, checks),
                lostInReader.isEmpty());
        Assert.assertTrue(report("pruning lost where both orders agree", notPruned, checks), notPruned.isEmpty());
    }

    /**
     * The predicate the filter builder pushes for {@code column <op> literal}, or {@code null} when it declines to push
     * one. Comparisons come from the builder itself; {@code starts_with} is built inline, as the builder's own
     * handler needs a logical plan, and is the builder's {@code Expressions.startsWith(column, prefix)} verbatim.
     */
    private static Expression pushedByBuilder(String column, Op op, String literal) {
        switch (op) {
            case EQ:
                return IcebergTableFilterBuilder.buildComparisonExpression(AlgebricksBuiltinFunctions.EQ, column,
                        literal, ATypeTag.STRING);
            case NEQ:
                return IcebergTableFilterBuilder.buildComparisonExpression(AlgebricksBuiltinFunctions.NEQ, column,
                        literal, ATypeTag.STRING);
            case LT:
                return IcebergTableFilterBuilder.buildComparisonExpression(AlgebricksBuiltinFunctions.LT, column,
                        literal, ATypeTag.STRING);
            case LTEQ:
                return IcebergTableFilterBuilder.buildComparisonExpression(AlgebricksBuiltinFunctions.LE, column,
                        literal, ATypeTag.STRING);
            case GT:
                return IcebergTableFilterBuilder.buildComparisonExpression(AlgebricksBuiltinFunctions.GT, column,
                        literal, ATypeTag.STRING);
            case GTEQ:
                return IcebergTableFilterBuilder.buildComparisonExpression(AlgebricksBuiltinFunctions.GE, column,
                        literal, ATypeTag.STRING);
            case STARTS_WITH:
                return Expressions.startsWith(column, literal);
            default:
                throw new IllegalArgumentException(op.name());
        }
    }

    private static String describe(String column, Op op, String literal, List<String> values, int row) {
        StringBuilder sb = new StringBuilder(column).append(' ').append(op.name()).append(' ')
                .append(StringOrderCorpus.describe(literal)).append("  file=[");
        for (int i = 0; i < values.size(); i++) {
            sb.append(i == 0 ? "" : ", ").append(i == row ? "*" : "").append(StringOrderCorpus.describe(values.get(i)));
        }
        return sb.append(']').toString();
    }

    private static String report(String what, List<String> failures, int checks) {
        StringBuilder sb = new StringBuilder(what).append(": ").append(failures.size()).append(" (over ").append(checks)
                .append(" file checks; * marks the lost row)");
        failures.stream().limit(25).forEach(f -> sb.append("\n  ").append(f));
        return sb.toString();
    }

    private static DataFile writeFile(int index, List<String> values) throws Exception {
        String location = table.location() + "/data/L" + index + ".parquet";
        OutputFile out = table.io().newOutputFile(location);
        GenericRecord template = GenericRecord.create(table.schema());
        GenericRecord struct = GenericRecord.create(table.schema().findType("st").asStructType());
        try (FileAppender<Record> writer =
                Parquet.write(out).schema(table.schema()).createWriterFunc(GenericParquetWriter::create).build()) {
            for (int id = 0; id < values.size(); id++) {
                Record row = template.copy();
                row.setField("id", id);
                row.setField("s", values.get(id));
                Record st = struct.copy();
                st.setField("s", values.get(id));
                row.setField("st", st);
                writer.add(row);
            }
        }
        Metrics metrics = ParquetUtil.fileMetrics(table.io().newInputFile(location), MetricsConfig.forTable(table));
        return DataFiles.builder(table.spec()).withPath(location).withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(out.toInputFile().getLength()).withMetrics(metrics).build();
    }
}
