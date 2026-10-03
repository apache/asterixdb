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
package org.apache.asterix.external.util.iceberg;

import java.io.File;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.WarningCollector;
import org.apache.asterix.external.util.iceberg.StringOrderCorpus.Op;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.ContentFile;
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
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;
import org.junit.Assert;
import org.junit.Test;

/**
 * {@link VariantBoundsEvaluator} on shredded STRING sub-fields, over real Iceberg tables whose bounds round-trip
 * through Avro manifests, checked against the query engine's own string order (see {@link StringOrderCorpus}).
 * <p>
 * Every file holds one or two rows, so bounds span a real range: with a single row per file lower equals upper and
 * the order bounds were computed in can never matter, which is how the single-row type matrix in
 * {@link VariantBoundsAllTypesTest} misses this. Two properties are checked for every file x operator x literal:
 * <ul>
 * <li><b>No data loss</b>: a file holding a row the engine would return is never skipped.</li>
 * <li><b>Pruning survives</b>: where the two orders provably agree (no character at or above U+D800, nothing
 * truncated), a file with no matching row is still skipped, so a fix cannot pass by simply giving up on strings.</li>
 * </ul>
 */
public class VariantBoundsStringOrderTest {

    private static final String COLUMN = "variant_field";

    private static Schema schema() {
        return new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, COLUMN, Types.VariantType.get()));
    }

    @Test
    public void topLevelSubField() throws Exception {
        assertSoundAndEffective("f", List.of("f"));
    }

    @Test
    public void nestedSubField() throws Exception {
        assertSoundAndEffective("a.b", List.of("a", "b"));
    }

    private static void assertSoundAndEffective(String subField, List<String> path) throws Exception {
        List<List<String>> layouts = StringOrderCorpus.fileLayouts();
        Table table = newTable(subField);
        AppendFiles append = table.newAppend();
        for (int i = 0; i < layouts.size(); i++) {
            append.appendFile(writeFile(table, "L" + i, path, layouts.get(i)));
        }
        append.commit();
        Map<String, ContentFile<?>> files = plannedFiles(table);
        Assert.assertEquals(layouts.size(), files.size());

        List<String> dataLoss = new ArrayList<>();
        List<String> notPruned = new ArrayList<>();
        int checks = 0;
        for (Op op : EnumSet.complementOf(EnumSet.of(Op.STARTS_WITH))) {
            for (String literal : StringOrderCorpus.VALUES) {
                Expression rewritten = VariantPredicateRewriter.rewriteAssumingNesting(
                        StringOrderCorpus.pushed(COLUMN + "." + subField, op, literal), schema());
                VariantBoundsEvaluator evaluator =
                        new VariantBoundsEvaluator(table.schema(), rewritten, new WarningCollector());
                for (int i = 0; i < layouts.size(); i++) {
                    List<String> rows = layouts.get(i);
                    boolean kept = evaluator.mightMatch(files.get("L" + i + ".parquet"));
                    boolean anyMatch = rows.stream().anyMatch(v -> StringOrderCorpus.engineMatches(v, op, literal));
                    checks++;
                    if (anyMatch && !kept) {
                        dataLoss.add(describe(op, literal, rows));
                    } else if (kept && StringOrderCorpus.orderInsensitive(rows, literal)
                            && StringOrderCorpus.boundsExclude(rows, op, literal)) {
                        notPruned.add(describe(op, literal, rows));
                    }
                }
            }
        }
        Assert.assertTrue(report("DATA LOSS — file skipped although a row matches", subField, dataLoss, checks),
                dataLoss.isEmpty());
        Assert.assertTrue(report("pruning lost where both orders agree", subField, notPruned, checks),
                notPruned.isEmpty());
    }

    private static String describe(Op op, String literal, List<String> rows) {
        StringBuilder sb =
                new StringBuilder(op.name()).append(' ').append(StringOrderCorpus.describe(literal)).append("  file=[");
        for (int i = 0; i < rows.size(); i++) {
            sb.append(i == 0 ? "" : ", ").append(StringOrderCorpus.describe(rows.get(i)));
        }
        return sb.append(']').toString();
    }

    private static String report(String what, String subField, List<String> failures, int checks) {
        StringBuilder sb = new StringBuilder(what).append(" on '").append(subField).append("': ")
                .append(failures.size()).append(" of ").append(checks).append(" checks");
        failures.stream().limit(25).forEach(f -> sb.append("\n  ").append(f));
        return sb.toString();
    }

    private static Table newTable(String name) throws Exception {
        File dir = java.nio.file.Files.createTempDirectory("vbso-" + name).toFile();
        return new HadoopTables(new org.apache.hadoop.conf.Configuration()).create(schema(),
                PartitionSpec.unpartitioned(), Map.of(TableProperties.FORMAT_VERSION, "3"),
                new File(dir, "t").getAbsolutePath());
    }

    /** The variant {@code {path[0]: {path[1]: ... value}}}, shredded down to the leaf. */
    private static Variant variantAt(List<String> path, String value) {
        VariantMetadata meta = Variants.metadata(path);
        VariantValue leaf = Variants.of(value);
        for (int i = path.size() - 1; i >= 0; i--) {
            ShreddedObject obj = Variants.object(meta);
            obj.put(path.get(i), leaf);
            leaf = obj;
        }
        return Variant.of(meta, leaf);
    }

    private static DataFile writeFile(Table table, String fileName, List<String> path, List<String> values)
            throws Exception {
        java.lang.reflect.Method toParquetSchema = Class.forName("org.apache.iceberg.parquet.ParquetVariantUtil")
                .getDeclaredMethod("toParquetSchema", VariantValue.class);
        toParquetSchema.setAccessible(true);
        org.apache.parquet.schema.Type typed =
                (org.apache.parquet.schema.Type) toParquetSchema.invoke(null, variantAt(path, values.get(0)).value());
        String location = table.location() + "/data/" + fileName + ".parquet";
        OutputFile out = table.io().newOutputFile(location);
        GenericRecord template = GenericRecord.create(table.schema());
        try (FileAppender<Record> writer = Parquet.write(out).schema(table.schema())
                .createWriterFunc(GenericParquetWriter::create).variantShreddingFunc((fid, n) -> typed).build()) {
            for (int id = 0; id < values.size(); id++) {
                Record row = template.copy();
                row.setField("id", id);
                row.setField(COLUMN, variantAt(path, values.get(id)));
                writer.add(row);
            }
        }
        Metrics metrics = ParquetUtil.fileMetrics(table.io().newInputFile(location), MetricsConfig.forTable(table));
        return DataFiles.builder(table.spec()).withPath(location).withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(out.toInputFile().getLength()).withMetrics(metrics).build();
    }

    private static Map<String, ContentFile<?>> plannedFiles(Table table) throws Exception {
        Map<String, ContentFile<?>> files = new LinkedHashMap<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().includeColumnStats().planFiles()) {
            for (FileScanTask task : tasks) {
                files.put(new File(task.file().location()).getName(), task.file().copy());
            }
        }
        return files;
    }
}
