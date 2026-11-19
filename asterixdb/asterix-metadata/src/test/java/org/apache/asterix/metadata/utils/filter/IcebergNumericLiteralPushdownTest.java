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
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.apache.asterix.dataflow.data.common.ILogicalBinaryComparator.Result;
import org.apache.asterix.dataflow.data.nontagged.comparators.ComparatorUtil;
import org.apache.asterix.external.util.iceberg.NumericLiteralPredicateRewriter;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AFloat;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.IAObject;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
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
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Numeric predicates pushed into Iceberg against a {@code float}, {@code decimal} or {@code double} column, checked
 * against the engine's own comparator.
 * <p>
 * Binding converts the literal to the column's type, and for a {@code float} or {@code decimal} column that conversion
 * can round, while the engine compares against the literal as written. The predicate goes through
 * {@link IcebergTableFilterBuilder} and then {@link NumericLiteralPredicateRewriter}, as the scan does, and is then
 * evaluated as production evaluates it: scan planning through {@code TableScan.filter(..)}, and the read through
 * {@code Parquet.read(..).filter(residual)}. One row per file, so the row is its file's minimum and maximum.
 */
public class IcebergNumericLiteralPushdownTest {

    private static final float[] FLOATS =
            { 0.1f, 0.2f, 1.5f, 16777217f, -0.0f, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, 0.0f, Float.NaN };
    private static final String[] DECIMALS = { "1.22", "1.23", "20.00", "-1.23" };
    // two values 20 digits wide that read back as the same double, so the engine calls them equal
    private static final String[] WIDE_DECIMALS = { "123456789012345678.90", "123456789012345678.91", "1.23" };
    private static final double[] DOUBLES = { 0.1, 1.23, 16777217.0, 9007199254740993.0, -0.0, 0.0, Double.NaN };

    private static final List<IAObject> LITERALS = new ArrayList<>();
    static {
        for (double d : new double[] { 0.1, 0.2, 1.5, 1.225, 1.2249, 1.23, -1.23, 19.999, 20.0, 16777217.0, 0.0, -0.0,
                1e39, -1e39, 1.2345678901234568E17, 9007199254740993.0 }) {
            LITERALS.add(new ADouble(d));
        }
        for (float f : new float[] { 0.1f, 1.5f, 1.23f, 16777217f }) {
            LITERALS.add(new AFloat(f));
        }
        for (int i : new int[] { 0, 1, 20, 16777217, -2 }) {
            LITERALS.add(new AInt32(i));
        }
        for (long l : new long[] { 20L, 16777217L, 9007199254740993L, 123456789012345679L }) {
            LITERALS.add(new AInt64(l));
        }
    }

    private static final FunctionIdentifier[] OPS =
            { AlgebricksBuiltinFunctions.EQ, AlgebricksBuiltinFunctions.NEQ, AlgebricksBuiltinFunctions.LT,
                    AlgebricksBuiltinFunctions.LE, AlgebricksBuiltinFunctions.GT, AlgebricksBuiltinFunctions.GE };

    private static Table table;
    private static int files;

    @BeforeClass
    public static void writeTable() throws Exception {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "f", Types.FloatType.get()),
                Types.NestedField.optional(3, "d", Types.DecimalType.of(10, 2)),
                Types.NestedField.optional(4, "w", Types.DecimalType.of(20, 2)),
                Types.NestedField.optional(5, "x", Types.DoubleType.get()), Types.NestedField.optional(6, "st",
                        Types.StructType.of(Types.NestedField.optional(7, "f", Types.FloatType.get()))));
        File dir = java.nio.file.Files.createTempDirectory("numeric-pushdown").toFile();
        table = new HadoopTables(new org.apache.hadoop.conf.Configuration()).create(schema,
                PartitionSpec.unpartitioned(), Map.of(TableProperties.FORMAT_VERSION, "2"),
                new File(dir, "t").getAbsolutePath());
        files = Math.max(Math.max(FLOATS.length, DECIMALS.length), Math.max(WIDE_DECIMALS.length, DOUBLES.length));
        AppendFiles append = table.newAppend();
        for (int i = 0; i < files; i++) {
            append.appendFile(writeFile(i));
        }
        append.commit();
    }

    @Test
    public void floatColumn() throws Exception {
        assertNoRowLost("f", r -> engineFloat((Float) r.getField("f")));
    }

    @Test
    public void floatColumnInsideStruct() throws Exception {
        assertNoRowLost("st.f", r -> engineFloat((Float) ((Record) r.getField("st")).getField("f")));
    }

    @Test
    public void decimalColumn() throws Exception {
        assertNoRowLost("d", r -> engineDecimal((BigDecimal) r.getField("d")));
    }

    /** Wider than a double can tell apart: two distinct stored values the engine calls equal. */
    @Test
    public void wideDecimalColumn() throws Exception {
        assertNoRowLost("w", r -> engineDecimal((BigDecimal) r.getField("w")));
    }

    @Test
    public void doubleColumn() throws Exception {
        assertNoRowLost("x", r -> r.getField("x") == null ? null : new ADouble((Double) r.getField("x")));
    }

    /** The guard must keep the pushdown wherever the literal converts exactly; dropping everything would also pass. */
    @Test
    public void exactLiteralsStillPrune() throws Exception {
        assertStillPrunes("f", AlgebricksBuiltinFunctions.GT, new ADouble(1.5), 0);
        assertStillPrunes("f", AlgebricksBuiltinFunctions.GT, new AInt32(1), 0);
        assertStillPrunes("f", AlgebricksBuiltinFunctions.LT, new AFloat(1.5f), 3);
        assertStillPrunes("d", AlgebricksBuiltinFunctions.GT, new ADouble(1.23), 1);
        assertStillPrunes("d", AlgebricksBuiltinFunctions.EQ, new AInt32(20), 0);
        assertStillPrunes("x", AlgebricksBuiltinFunctions.GT, new ADouble(1.225), 0);
    }

    private static void assertStillPrunes(String column, FunctionIdentifier op, IAObject literal, int prunedFile)
            throws Exception {
        Expression pushed = pushed(column, op, literal, false);
        Assert.assertNotEquals(column + " " + op.getName() + " " + literal + " must stay pushed",
                Expressions.alwaysTrue(), pushed);
        Assert.assertFalse(column + " " + op.getName() + " " + literal + " must still skip file " + prunedFile,
                plannedFiles(pushed).contains(prunedFile));
    }

    private static void assertNoRowLost(String column, Function<Record, IAObject> engineValueOf) throws Exception {
        List<String> lostAtPlanning = new ArrayList<>();
        List<String> lostInReader = new ArrayList<>();
        int checks = 0;
        for (FunctionIdentifier op : OPS) {
            for (IAObject literal : LITERALS) {
                for (boolean negated : new boolean[] { false, true }) {
                    Expression pushed = pushed(column, op, literal, negated);
                    Set<Integer> planned = new HashSet<>();
                    Set<Integer> returned = new HashSet<>();
                    try (CloseableIterable<FileScanTask> tasks = table.newScan().filter(pushed).planFiles()) {
                        for (FileScanTask task : tasks) {
                            planned.add(fileIndex(task));
                            try (CloseableIterable<Record> rows = Parquet
                                    .read(table.io().newInputFile(task.file().location())).project(table.schema())
                                    .filter(task.residual()).split(task.start(), task.length())
                                    .createReaderFunc(fs -> GenericParquetReaders.buildReader(table.schema(), fs))
                                    .build()) {
                                for (Record r : rows) {
                                    returned.add((Integer) r.getField("id"));
                                }
                            }
                        }
                    }
                    for (Record row : allRows()) {
                        int id = (Integer) row.getField("id");
                        IAObject value = engineValueOf.apply(row);
                        checks++;
                        // NOT of a comparison over a non-null value is its complement; Iceberg rewrites the NOT into the
                        // opposite comparison, which is where a guard reasoning only about the written operator would fail
                        if (value == null || engineMatches(value, op, literal) == negated) {
                            continue;
                        }
                        String lost = (negated ? "not " : "") + column + " " + op.getName() + " " + describe(literal)
                                + " lost " + describe(value) + "  (pushed " + pushed + ")";
                        if (!planned.contains(id)) {
                            lostAtPlanning.add(lost);
                        } else if (!returned.contains(id)) {
                            lostInReader.add(lost);
                        }
                    }
                }
            }
        }
        Assert.assertTrue(report("DATA LOSS at scan planning (file skipped)", lostAtPlanning, checks),
                lostAtPlanning.isEmpty());
        Assert.assertTrue(report("DATA LOSS in the reader (row group skipped)", lostInReader, checks),
                lostInReader.isEmpty());
    }

    /**
     * What the scan pushes for {@code column <op> literal}, or for its negation: the builder's predicate, wrapped in a
     * NOT as the builder's own NOT handling wraps it, then the numeric guard.
     */
    private static Expression pushed(String column, FunctionIdentifier op, IAObject literal, boolean negated) {
        Expression built = IcebergTableFilterBuilder.buildComparisonExpression(op, column, javaValue(literal),
                literal.getType().getTypeTag());
        if (built == null) {
            return Expressions.alwaysTrue();
        }
        return NumericLiteralPredicateRewriter.rewrite(negated ? Expressions.not(built) : built, table.schema());
    }

    /** The Java value the builder's {@code createLiteralValue} produces for each numeric literal type. */
    private static Object javaValue(IAObject literal) {
        switch (literal.getType().getTypeTag()) {
            case DOUBLE:
                return ((ADouble) literal).getDoubleValue();
            case FLOAT:
                return ((AFloat) literal).getFloatValue();
            case INTEGER:
                return ((AInt32) literal).getIntegerValue();
            case BIGINT:
                return ((AInt64) literal).getLongValue();
            default:
                throw new IllegalArgumentException(literal.toString());
        }
    }

    private static boolean engineMatches(IAObject value, FunctionIdentifier op, IAObject literal) {
        boolean equality = op.equals(AlgebricksBuiltinFunctions.EQ) || op.equals(AlgebricksBuiltinFunctions.NEQ);
        Result c = ComparatorUtil.createLogicalComparator(value.getType(), literal.getType(), equality).compare(value,
                literal);
        if (c != Result.LT && c != Result.EQ && c != Result.GT) {
            return false;
        }
        if (op.equals(AlgebricksBuiltinFunctions.EQ)) {
            return c == Result.EQ;
        } else if (op.equals(AlgebricksBuiltinFunctions.NEQ)) {
            return c != Result.EQ;
        } else if (op.equals(AlgebricksBuiltinFunctions.LT)) {
            return c == Result.LT;
        } else if (op.equals(AlgebricksBuiltinFunctions.LE)) {
            return c != Result.GT;
        } else if (op.equals(AlgebricksBuiltinFunctions.GT)) {
            return c == Result.GT;
        }
        return c != Result.LT;
    }

    /** How the reader hands a float to the engine. */
    private static IAObject engineFloat(Float f) {
        return f == null ? null : new AFloat(f);
    }

    /** How the reader hands a decimal to the engine, with decimal-to-double on. */
    private static IAObject engineDecimal(BigDecimal d) {
        return d == null ? null : new ADouble(d.doubleValue());
    }

    private static String describe(IAObject o) {
        return o.getType().getTypeTag() + "(" + o + ")";
    }

    private static Set<Integer> plannedFiles(Expression pushed) throws Exception {
        Set<Integer> planned = new HashSet<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().filter(pushed).planFiles()) {
            tasks.forEach(task -> planned.add(fileIndex(task)));
        }
        return planned;
    }

    private static int fileIndex(FileScanTask task) {
        String name = new File(task.file().location()).getName();
        return Integer.parseInt(name.substring(1, name.indexOf('.')));
    }

    private static List<Record> allRows() throws Exception {
        List<Record> rows = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                try (CloseableIterable<Record> read = Parquet.read(table.io().newInputFile(task.file().location()))
                        .project(table.schema())
                        .createReaderFunc(fs -> GenericParquetReaders.buildReader(table.schema(), fs)).build()) {
                    read.forEach(rows::add);
                }
            }
        }
        Assert.assertEquals(files, rows.size());
        return rows;
    }

    private static String report(String what, List<String> failures, int checks) {
        StringBuilder sb = new StringBuilder(what).append(": ").append(failures.size()).append(" (over ").append(checks)
                .append(" row checks)");
        failures.stream().limit(30).forEach(f -> sb.append("\n  ").append(f));
        return sb.toString();
    }

    private static DataFile writeFile(int index) throws Exception {
        String location = table.location() + "/data/N" + index + ".parquet";
        OutputFile out = table.io().newOutputFile(location);
        // the writer's own metrics, which carry the NaN counts Iceberg prunes NaN by, as a committing engine's do
        FileAppender<Record> appender =
                Parquet.write(out).schema(table.schema()).createWriterFunc(GenericParquetWriter::create).build();
        try (FileAppender<Record> writer = appender) {
            Record row = GenericRecord.create(table.schema());
            row.setField("id", index);
            Float f = index < FLOATS.length ? FLOATS[index] : null;
            row.setField("f", f);
            row.setField("d", index < DECIMALS.length ? new BigDecimal(DECIMALS[index]) : null);
            row.setField("w", index < WIDE_DECIMALS.length ? new BigDecimal(WIDE_DECIMALS[index]) : null);
            row.setField("x", index < DOUBLES.length ? DOUBLES[index] : null);
            Record st = GenericRecord.create(table.schema().findType("st").asStructType());
            st.setField("f", f);
            row.setField("st", st);
            writer.add(row);
        }
        return DataFiles.builder(table.spec()).withPath(location).withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(out.toInputFile().getLength()).withMetrics(appender.metrics()).build();
    }
}
