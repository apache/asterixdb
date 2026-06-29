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
package org.apache.asterix.test.iceberg;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.VariantShreddingFunction;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.Variants;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.junit.Test;

/**
 * Verifies the <em>physical Parquet layout</em> produced when writing an Iceberg VARIANT column, for the unshredded
 * (serialized) and shredded encodings. This is a fast, self-contained <b>write-side</b> unit test: it writes local
 * Parquet files and inspects each file's footer schema. It does <b>not</b> touch the Analytics read pipeline /
 * {@code IcebergParquetDataParser} — that is covered by the read-side integration tests. Its sole job is to guarantee
 * that a fixture claimed to be "shredded" is genuinely shredded on disk (a {@code typed_value} sub-column exists), so
 * that any downstream read test built on such a fixture actually exercises the shredded path instead of silently
 * re-testing serialized data.
 *
 * <p>Shredding is driven purely by {@link VariantShreddingFunction}. Per the Iceberg 1.10.x contract (see
 * {@code TypeToMessageType.variant(...)}), the function returns <b>only</b> the {@code typed_value} subtree: it must be
 * a group named exactly {@code "typed_value"} with {@code OPTIONAL} repetition. Iceberg itself wraps it with the
 * {@code metadata} (required binary) and residual {@code value} (optional binary) fields. Object fields present in
 * {@code typed_value} are shredded into typed sub-columns; anything omitted stays in the residual {@code value}, which
 * is exactly how partial shredding is expressed.
 *
 * <p>The row also carries ordinary (non-variant) columns so the variant is exercised as one column among several,
 * the way it appears in a real table — not as the sole column.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Write-side unit test asserting serialized vs (fully/partially) shredded physical Parquet layout for a VARIANT column, with ordinary columns alongside and per-file verification across multiple files")
public class IcebergVariantSerializedAndShreddedTest {

    // A realistic-ish row: several ordinary columns plus one variant column. Field ids are arbitrary but stable.
    private static final Schema SCHEMA = new Schema(required(1, "id", Types.IntegerType.get()),
            optional(2, "name", Types.StringType.get()), optional(3, "amount", Types.DoubleType.get()),
            optional(4, "active", Types.BooleanType.get()), optional(5, "variant_field", Types.VariantType.get()));

    private static final String VARIANT_COLUMN = "variant_field";

    /**
     * The three physical encodings a variant column can take on disk. Each value knows both how to write itself (which
     * {@link VariantShreddingFunction}, if any) and how to assert its own resulting layout — so single-file tests and
     * the multi-file test share exactly one source of truth for "what does this encoding look like".
     */
    private enum Encoding {
        /** No shredding: variant is a single {@code metadata}+{@code value} blob, no {@code typed_value}. */
        SERIALIZED,
        /** Both object fields ("a" and "b") promoted to typed sub-columns under {@code typed_value}. */
        FULLY_SHREDDED,
        /** Only "a" promoted; "b" stays in the residual {@code value}. */
        PARTIALLY_SHREDDED
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Single-file tests — one per encoding.
    // ---------------------------------------------------------------------------------------------------------------

    /**
     * Serialized (unshredded) write. Confirms that with no {@link VariantShreddingFunction} the variant column is a
     * plain {@code metadata}+{@code value} group and carries <b>no</b> {@code typed_value} sub-column — i.e. the
     * default, whole-blob encoding. This is the negative baseline: it proves the assertions can actually distinguish
     * "not shredded", so a passing shredded test below isn't a false positive.
     */
    @Test
    public void serializedWriteHasNoTypedValue() throws Exception {
        File file = writeSingle(Encoding.SERIALIZED);
        assertLayout(variantColumn(file), Encoding.SERIALIZED);
    }

    /**
     * Fully shredded write. Confirms that supplying a shredding function covering every object field produces a
     * {@code typed_value} group containing a typed sub-column for each field ("a" and "b"), while still keeping
     * {@code metadata} and a residual {@code value}. This is the happy path that read-side shredded fixtures rely on.
     */
    @Test
    public void fullyShreddedWriteHasTypedValueForAllFields() throws Exception {
        File file = writeSingle(Encoding.FULLY_SHREDDED);
        assertLayout(variantColumn(file), Encoding.FULLY_SHREDDED);
    }

    /**
     * Partially shredded write — the highest-risk encoding for readers. Confirms that a shredding function covering
     * only a subset of fields shreds exactly that subset ("a") into {@code typed_value} and leaves the rest ("b") in
     * the residual {@code value}. This is the case that exercises the typed-value/residual split, which is where
     * reconstruction bugs are most likely to hide, so proving the fixture is genuinely partial matters.
     */
    @Test
    public void partiallyShreddedWriteShredsSubsetAndKeepsResidual() throws Exception {
        File file = writeSingle(Encoding.PARTIALLY_SHREDDED);
        assertLayout(variantColumn(file), Encoding.PARTIALLY_SHREDDED);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Multi-file test.
    // ---------------------------------------------------------------------------------------------------------------

    /**
     * Verifies that layout is a <b>per-file</b> property and can be confirmed file-by-file. Writes several independent
     * Parquet files with a mix of encodings (including a repeated encoding) and asserts each file's variant column
     * against its own expected encoding. This mirrors reality: a single Iceberg table can contain data files written
     * by different engines/versions, so one file may be serialized while another is shredded. The same per-file check
     * used here (open the file, read its footer schema, inspect the variant group) is exactly what a read-side test
     * would apply to each {@code FileScanTask.file()} of a real table.
     */
    @Test
    public void multipleFilesEachVerifiedIndependently() throws Exception {
        Encoding[] plan =
                { Encoding.SERIALIZED, Encoding.FULLY_SHREDDED, Encoding.PARTIALLY_SHREDDED, Encoding.FULLY_SHREDDED };

        List<File> files = new ArrayList<>();
        for (int i = 0; i < plan.length; i++) {
            files.add(writeFile("multi-" + i + "-" + plan[i].name().toLowerCase(), plan[i]));
        }

        // Confirm every file independently against its own expected encoding.
        for (int i = 0; i < plan.length; i++) {
            assertLayout(variantColumn(files.get(i)), plan[i]);
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Layout assertions — the single source of truth for what each encoding looks like on disk.
    // ---------------------------------------------------------------------------------------------------------------

    private static void assertLayout(GroupType variant, Encoding encoding) {
        // Every encoding keeps the variant metadata and a value slot (residual for shredded, whole-blob for serialized).
        assertTrue("variant must keep metadata", variant.containsField("metadata"));
        assertTrue("variant must keep a value blob", variant.containsField("value"));

        switch (encoding) {
            case SERIALIZED:
                assertFalse("serialized variant must NOT have typed_value", variant.containsField("typed_value"));
                break;
            case FULLY_SHREDDED: {
                assertTrue("shredded variant must have typed_value", variant.containsField("typed_value"));
                GroupType typedValue = variant.getType("typed_value").asGroupType();
                assertTrue("field 'a' must be shredded", typedValue.containsField("a"));
                assertTrue("field 'b' must be shredded", typedValue.containsField("b"));
                break;
            }
            case PARTIALLY_SHREDDED: {
                assertTrue("partially shredded variant must have typed_value", variant.containsField("typed_value"));
                GroupType typedValue = variant.getType("typed_value").asGroupType();
                assertTrue("field 'a' must be shredded", typedValue.containsField("a"));
                assertFalse("field 'b' must stay in residual value, not typed_value", typedValue.containsField("b"));
                break;
            }
            default:
                throw new IllegalArgumentException("unhandled encoding: " + encoding);
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Fixture: rows with ordinary columns + a small variant object { "a": <int>, "b": <string> }. The variant is kept
    // deliberately tiny so the hand-authored shredding schema is easy to verify by eye.
    // ---------------------------------------------------------------------------------------------------------------

    private static List<Record> rows() {
        List<Record> records = new ArrayList<>();
        records.add(row(1, "alice", 10.5d, true, 123, "hello"));
        records.add(row(2, "bob", -3.25d, false, 456, "world"));
        return records;
    }

    private static Record row(int id, String name, double amount, boolean active, int a, String b) {
        VariantMetadata metadata = Variants.metadata("a", "b");
        ShreddedObject object = Variants.object(metadata);
        object.put("a", Variants.of(a));
        object.put("b", Variants.of(b));

        Record record = GenericRecord.create(SCHEMA);
        record.setField("id", id);
        record.setField("name", name);
        record.setField("amount", amount);
        record.setField("active", active);
        record.setField(VARIANT_COLUMN, Variant.of(metadata, object));
        return record;
    }

    /**
     * Builds the {@code typed_value} subtree the writer expects. Per the Iceberg contract the returned type must be an
     * OPTIONAL group named {@code "typed_value"}; each shredded object field is a {@code required group} with an
     * optional binary {@code value} (per-field residual) and an optional typed {@code typed_value}.
     *
     * @param shredB when true shred both "a" and "b"; when false shred only "a" (partial shredding).
     */
    private static VariantShreddingFunction shreddingFunc(boolean shredB) {
        // There is only one variant column, so fieldId/name are ignored here; branch on them if a schema ever has more.
        return (fieldId, name) -> {
            Type aField = org.apache.parquet.schema.Types.requiredGroup().optional(PrimitiveTypeName.BINARY)
                    .named("value").optional(PrimitiveTypeName.INT32).named("typed_value").named("a");

            org.apache.parquet.schema.Types.GroupBuilder<GroupType> typedValue =
                    org.apache.parquet.schema.Types.optionalGroup().addField(aField);

            if (shredB) {
                Type bField = org.apache.parquet.schema.Types.requiredGroup().optional(PrimitiveTypeName.BINARY)
                        .named("value").optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType())
                        .named("typed_value").named("b");
                typedValue.addField(bField);
            }

            return typedValue.named("typed_value");
        };
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Write / read helpers
    // ---------------------------------------------------------------------------------------------------------------

    private static File writeSingle(Encoding encoding) throws Exception {
        return writeFile(encoding.name().toLowerCase(), encoding);
    }

    private static File writeFile(String prefix, Encoding encoding) throws Exception {
        File file = newParquetFile(prefix);
        VariantShreddingFunction shreddingFunc = switch (encoding) {
            case SERIALIZED -> null;
            case FULLY_SHREDDED -> shreddingFunc(true);
            case PARTIALLY_SHREDDED -> shreddingFunc(false);
        };

        OutputFile out = org.apache.iceberg.Files.localOutput(file);
        Parquet.WriteBuilder builder =
                Parquet.write(out).schema(SCHEMA).createWriterFunc(GenericParquetWriter::create);
        if (shreddingFunc != null) {
            builder = builder.variantShreddingFunc(shreddingFunc);
        }
        try (FileAppender<Record> writer = builder.build()) {
            for (Record record : rows()) {
                writer.add(record);
            }
        }
        return file;
    }

    /** Reads the Parquet footer of a single file and returns its {@code variant_field} column as a {@link GroupType}. */
    private static GroupType variantColumn(File file) throws Exception {
        try (ParquetFileReader reader =
                ParquetFileReader.open(HadoopInputFile.fromPath(new Path(file.toURI()), new Configuration()))) {
            MessageType schema = reader.getFooter().getFileMetaData().getSchema();
            return schema.getType(VARIANT_COLUMN).asGroupType();
        }
    }

    private static File newParquetFile(String prefix) throws Exception {
        File dir = Files.createTempDirectory("variant-shred-" + prefix).toFile();
        dir.deleteOnExit();
        File file = new File(dir, prefix + ".parquet");
        // Iceberg's localOutput refuses to overwrite; make sure the path is clear.
        if (file.exists()) {
            assertTrue(file.delete());
        }
        file.deleteOnExit();
        return file;
    }
}
