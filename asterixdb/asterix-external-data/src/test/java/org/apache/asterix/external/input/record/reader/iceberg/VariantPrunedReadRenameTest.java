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
package org.apache.asterix.external.input.record.reader.iceberg;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.IntFunction;

import org.apache.asterix.external.util.iceberg.VariantProjectionPlan;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Assert;
import org.junit.Test;

/**
 * The variant-pruned read of a file written before a schema change renamed its variant columns.
 * <p>
 * A data file keeps the column names it was written with, and only field ids tie them to the table's current names.
 * Each case reads such a file through the current schema and asserts two things per row: that the requested field
 * comes back with its value, and that the field the query did not ask for is gone, which shows the read really was
 * pruned. Matching the file's columns by current name fails these in one of two ways: the renamed column is not found
 * and nothing is pruned, or the old name now belongs to another column and the wrong column is pruned, so requested
 * fields are lost.
 */
public class VariantPrunedReadRenameTest {

    private static final int ROW_COUNT = 20;
    private static final VariantMetadata META = Variants.metadata("big", "x", "y");

    /**
     * {@code { x: <id>, y: -<id>, big: "<padding>" }}: a query requests x or y, and big is what pruning must always
     * leave unread.
     */
    private static Variant row(int id) {
        ShreddedObject object = Variants.object(META);
        object.put("x", Variants.of(id));
        object.put("y", Variants.of(-id));
        object.put("big", Variants.of(("padding-" + id).repeat(20)));
        return Variant.of(META, object);
    }

    /** Writes {@link #ROW_COUNT} rows with every variant column shredded on x, y and big. */
    private static File write(Schema schema, IntFunction<Record> rows) throws Exception {
        java.lang.reflect.Method toParquetSchema = Class.forName("org.apache.iceberg.parquet.ParquetVariantUtil")
                .getDeclaredMethod("toParquetSchema", VariantValue.class);
        toParquetSchema.setAccessible(true);
        Type typedValue = (Type) toParquetSchema.invoke(null, row(0).value());

        File dir = java.nio.file.Files.createTempDirectory("variant-pruned-rename").toFile();
        File dataFile = new File(dir, "data.parquet");
        try (FileAppender<Record> writer = Parquet.write(org.apache.iceberg.Files.localOutput(dataFile)).schema(schema)
                .createWriterFunc(GenericParquetWriter::create).variantShreddingFunc((fieldId, name) -> typedValue)
                .build()) {
            for (int i = 0; i < ROW_COUNT; i++) {
                writer.add(rows.apply(i));
            }
        }
        return dataFile;
    }

    /** A plan for the given paths, each starting at a column of {@code schema}. */
    @SafeVarargs
    private static VariantProjectionPlan plan(Schema schema, List<String>... paths) throws Exception {
        ARecordType projected = ProjectionFiltrationTypeUtil.getRecordType(Arrays.asList(paths));
        VariantProjectionPlan plan = VariantProjectionPlan.from(schema, projected, true);
        Assert.assertFalse("the plan must narrow something", plan.isEmpty());
        return plan;
    }

    /** Reads the whole file, asserting that the read was pruned. */
    private static List<Record> readPruned(File dataFile, Schema expected, VariantProjectionPlan plan)
            throws Exception {
        List<Record> records = readPruned(dataFile, expected, plan, null);
        Assert.assertEquals(ROW_COUNT, records.size());
        return records;
    }

    /** Reads the whole file with {@code deleted} positions skipped ({@code null} for none), asserting a pruned read. */
    private static List<Record> readPruned(File dataFile, Schema expected, VariantProjectionPlan plan,
            PositionDeleteIndex deleted) throws Exception {
        List<Record> records = new ArrayList<>();
        try (VariantProjectedParquetReader reader = VariantProjectedParquetReader
                .open(org.apache.iceberg.Files.localInput(dataFile), expected, null, 0, 0, true, plan, deleted)) {
            Assert.assertTrue("the renamed variant must still be pruned", reader.canPrune());
            for (Record record : reader) {
                records.add(record);
            }
        }
        return records;
    }

    /** Asserts the variant holds x = id and neither y nor big: x survived and the rest was pruned away. */
    private static void assertPrunedToX(String column, Object variant, int id) {
        assertPrunedTo(column, variant, "x", id, "y");
    }

    /** Asserts the variant holds y = -id and neither x nor big. */
    private static void assertPrunedToY(String column, Object variant, int id) {
        assertPrunedTo(column, variant, "y", -id, "x");
    }

    private static void assertPrunedTo(String column, Object variant, String kept, int value, String dropped) {
        Assert.assertNotNull(column + " must be read", variant);
        VariantValue object = ((Variant) variant).value();
        VariantValue field = object.asObject().get(kept);
        Assert.assertNotNull(column + "." + kept + " must survive the pruning", field);
        Assert.assertEquals(column + "." + kept, value, field.asPrimitive().get());
        Assert.assertNull(column + "." + dropped + " must be pruned", object.asObject().get(dropped));
        Assert.assertNull(column + ".big must be pruned", object.asObject().get("big"));
    }

    /** A record of {@code schema} with id {@code i} and the given top-level fields. */
    private static Record record(Schema schema, int i, Object... namesAndValues) {
        Record record = GenericRecord.create(schema);
        record.setField("id", i);
        for (int n = 0; n < namesAndValues.length; n += 2) {
            record.setField((String) namesAndValues[n], namesAndValues[n + 1]);
        }
        return record;
    }

    /** A record of {@code struct} with the given fields. */
    private static Record struct(Types.StructType struct, Object... namesAndValues) {
        Record record = GenericRecord.create(struct);
        for (int n = 0; n < namesAndValues.length; n += 2) {
            record.setField((String) namesAndValues[n], namesAndValues[n + 1]);
        }
        return record;
    }

    @Test
    public void renamedColumn_isStillPruned() throws Exception {
        Schema written = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "a", Types.VariantType.get()));
        File dataFile = write(written, i -> {
            Record record = GenericRecord.create(written);
            record.setField("id", i);
            record.setField("a", row(i));
            return record;
        });

        Schema current = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "b", Types.VariantType.get()));
        for (Record record : readPruned(dataFile, current, plan(current, List.of("b", "x")))) {
            int id = (Integer) record.getField("id");
            assertPrunedToX("b", record.getField("b"), id);
        }
    }

    @Test
    public void reusedName_eachColumnIsClippedToItsOwnPaths() throws Exception {
        Schema written = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "a", Types.VariantType.get()));
        File dataFile = write(written, i -> {
            Record record = GenericRecord.create(written);
            record.setField("id", i);
            record.setField("a", row(i));
            return record;
        });

        // a was renamed to b, then a new a was added: in this file, "a" is the column that is now b
        Schema current = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "b", Types.VariantType.get()),
                Types.NestedField.optional(3, "a", Types.VariantType.get()));
        for (Record record : readPruned(dataFile, current, plan(current, List.of("b", "x"), List.of("a", "y")))) {
            int id = (Integer) record.getField("id");
            assertPrunedToX("b", record.getField("b"), id);
            Assert.assertNull("the new a is not in this file", record.getField("a"));
        }
    }

    @Test
    public void renamedParentStruct_variantIsFoundThroughTheParentsId() throws Exception {
        Types.StructType writtenStruct =
                Types.StructType.of(Types.NestedField.optional(4, "v", Types.VariantType.get()));
        Schema written = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(3, "p", writtenStruct));
        File dataFile = write(written, i -> {
            Record struct = GenericRecord.create(writtenStruct);
            struct.setField("v", row(i));
            Record record = GenericRecord.create(written);
            record.setField("id", i);
            record.setField("p", struct);
            return record;
        });

        // p was renamed to q, then a new struct p was added: in this file, "p" is the struct that is now q
        Schema current = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(3, "q",
                        Types.StructType.of(Types.NestedField.optional(4, "v", Types.VariantType.get()))),
                Types.NestedField.optional(5, "p",
                        Types.StructType.of(Types.NestedField.optional(6, "v", Types.VariantType.get()))));
        for (Record record : readPruned(dataFile, current,
                plan(current, List.of("q", "v", "x"), List.of("p", "v", "y")))) {
            int id = (Integer) record.getField("id");
            Record q = (Record) record.getField("q");
            Assert.assertNotNull("q of row " + id + " must be read", q);
            assertPrunedToX("q.v", q.getField("v"), id);
            Assert.assertNull("the new p is not in this file", record.getField("p"));
        }
    }

    @Test
    public void swappedNames_eachColumnIsClippedToItsOwnPaths() throws Exception {
        Schema written = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "a", Types.VariantType.get()),
                Types.NestedField.optional(3, "b", Types.VariantType.get()));
        File dataFile = write(written, i -> record(written, i, "a", row(i), "b", row(i)));

        // a and b swapped names: in this file "a" is the column now called b, and "b" the one now called a
        Schema current = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "b", Types.VariantType.get()),
                Types.NestedField.optional(3, "a", Types.VariantType.get()));
        for (Record record : readPruned(dataFile, current, plan(current, List.of("b", "x"), List.of("a", "y")))) {
            int id = (Integer) record.getField("id");
            assertPrunedToX("b of row " + id, record.getField("b"), id);
            assertPrunedToY("a of row " + id, record.getField("a"), id);
        }
    }

    /** A dropped column and a new one of the same name have different ids, so the file holds nothing of the new one. */
    @Test
    public void droppedAndReaddedName_oldColumnIsNotRead() throws Exception {
        Schema written = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "a", Types.VariantType.get()));
        File dataFile = write(written, i -> record(written, i, "a", row(i)));

        Schema current = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(3, "a", Types.VariantType.get()));
        int rows = 0;
        try (VariantProjectedParquetReader reader =
                VariantProjectedParquetReader.open(org.apache.iceberg.Files.localInput(dataFile), current, null, 0, 0,
                        true, plan(current, List.of("a", "x")))) {
            Assert.assertFalse("there is nothing of the new a to narrow in this file", reader.canPrune());
            for (Record record : reader) {
                Assert.assertNull("the new a is not in this file", record.getField("a"));
                rows++;
            }
        }
        Assert.assertEquals(ROW_COUNT, rows);
    }

    @Test
    public void reusedName_withPositionDeletes() throws Exception {
        Schema written = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "a", Types.VariantType.get()));
        File dataFile = write(written, i -> record(written, i, "a", row(i)));

        Schema current = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "b", Types.VariantType.get()),
                Types.NestedField.optional(3, "a", Types.VariantType.get()));
        Set<Long> deleted = Set.of(0L, 7L, (long) ROW_COUNT - 1);
        PositionDeleteIndex deletions = new PositionDeleteIndex() {
            @Override
            public void delete(long position) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void delete(long posStart, long posEnd) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isDeleted(long position) {
                return deleted.contains(position);
            }

            @Override
            public boolean isEmpty() {
                return false;
            }
        };
        List<Integer> ids = new ArrayList<>();
        for (Record record : readPruned(dataFile, current, plan(current, List.of("b", "x"), List.of("a", "y")),
                deletions)) {
            int id = (Integer) record.getField("id");
            ids.add(id);
            assertPrunedToX("b of row " + id, record.getField("b"), id);
            Assert.assertNull("the new a is not in this file", record.getField("a"));
        }
        List<Integer> expected = new ArrayList<>();
        for (int i = 0; i < ROW_COUNT; i++) {
            if (!deleted.contains((long) i)) {
                expected.add(i);
            }
        }
        Assert.assertEquals(expected, ids);
    }

    @Test
    public void renamedMiddleStruct_twoLevelsDown() throws Exception {
        Types.StructType writtenMid = Types.StructType.of(Types.NestedField.optional(5, "v", Types.VariantType.get()));
        Types.StructType writtenOuter = Types.StructType.of(Types.NestedField.optional(4, "mid", writtenMid));
        Schema written = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(3, "outer", writtenOuter));
        File dataFile = write(written,
                i -> record(written, i, "outer", struct(writtenOuter, "mid", struct(writtenMid, "v", row(i)))));

        // outer.mid was renamed to outer.mid2 and a new outer.mid was added
        Schema current =
                new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
                        Types.NestedField
                                .optional(3, "outer",
                                        Types.StructType.of(
                                                Types.NestedField.optional(
                                                        4, "mid2",
                                                        Types.StructType
                                                                .of(Types.NestedField.optional(5, "v",
                                                                        Types.VariantType.get()))),
                                                Types.NestedField.optional(6, "mid",
                                                        Types.StructType.of(Types.NestedField.optional(7, "v",
                                                                Types.VariantType.get()))))));
        for (Record record : readPruned(dataFile, current,
                plan(current, List.of("outer", "mid2", "v", "x"), List.of("outer", "mid", "v", "y")))) {
            int id = (Integer) record.getField("id");
            Record outer = (Record) record.getField("outer");
            Assert.assertNotNull("outer of row " + id + " must be read", outer);
            Record mid2 = (Record) outer.getField("mid2");
            Assert.assertNotNull("outer.mid2 of row " + id + " must be read", mid2);
            assertPrunedToX("outer.mid2.v of row " + id, mid2.getField("v"), id);
            Assert.assertNull("the new outer.mid is not in this file", outer.getField("mid"));
        }
    }

    /**
     * A file written without field ids gets ids from column positions, the way Iceberg reads such files, and the value
     * readers bind columns by those ids. Clipping has to pick the same column, even when the table has since renamed
     * it.
     */
    @Test
    public void fileWithoutFieldIds_columnIsFoundByPosition() throws Exception {
        Schema written = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "a", Types.VariantType.get()));
        File withIds = write(written, i -> {
            Record record = GenericRecord.create(written);
            record.setField("id", i);
            record.setField("a", row(i));
            return record;
        });
        File withoutIds = new File(withIds.getParentFile(), "no-ids.parquet");
        copyWithoutFieldIds(withIds, withoutIds);

        Schema current = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "b", Types.VariantType.get()));
        for (Record record : readPruned(withoutIds, current, plan(current, List.of("b", "x")))) {
            int id = (Integer) record.getField("id");
            assertPrunedToX("b", record.getField("b"), id);
        }
    }

    /** Rewrites {@code source} row by row under the same schema with every field id removed. */
    private static void copyWithoutFieldIds(File source, File target) throws Exception {
        MessageType schema;
        try (ParquetFileReader reader =
                ParquetFileReader.open(new LocalInputFile(source.toPath()), ParquetReadOptions.builder().build())) {
            schema = reader.getFileMetaData().getSchema();
        }
        org.apache.parquet.schema.Types.MessageTypeBuilder builder = org.apache.parquet.schema.Types.buildMessage();
        for (Type field : schema.getFields()) {
            builder.addField(withoutId(field));
        }
        MessageType stripped = builder.named(schema.getName());
        Assert.assertFalse("the copy must carry no field ids", ParquetSchemaUtil.hasIds(stripped));

        try (ParquetReader<Group> in =
                ParquetReader.builder(new GroupReadSupport(), new org.apache.hadoop.fs.Path(source.toURI())).build();
                ParquetWriter<Group> out =
                        ExampleParquetWriter.builder(new LocalOutputFile(target.toPath())).withType(stripped).build()) {
            for (Group group = in.read(); group != null; group = in.read()) {
                out.write(group);
            }
        }
        try (ParquetFileReader reader =
                ParquetFileReader.open(new LocalInputFile(target.toPath()), ParquetReadOptions.builder().build())) {
            Assert.assertFalse("the written copy must carry no field ids",
                    ParquetSchemaUtil.hasIds(reader.getFileMetaData().getSchema()));
        }
    }

    private static Type withoutId(Type type) {
        if (type.isPrimitive()) {
            PrimitiveType primitive = type.asPrimitiveType();
            return org.apache.parquet.schema.Types
                    .primitive(primitive.getPrimitiveTypeName(), primitive.getRepetition())
                    .length(primitive.getTypeLength()).as(primitive.getLogicalTypeAnnotation())
                    .named(primitive.getName());
        }
        GroupType group = type.asGroupType();
        org.apache.parquet.schema.Types.GroupBuilder<GroupType> builder =
                org.apache.parquet.schema.Types.buildGroup(group.getRepetition()).as(group.getLogicalTypeAnnotation());
        for (Type field : group.getFields()) {
            builder.addField(withoutId(field));
        }
        return builder.named(group.getName());
    }
}
