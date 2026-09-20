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
package org.apache.asterix.external.input.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.asterix.external.input.filter.ParquetFilterConverter.ReadOptions;
import org.apache.asterix.external.input.filter.ParquetFilterExpression.Comparison;
import org.apache.asterix.external.input.filter.ParquetFilterExpression.Operator;
import org.apache.asterix.external.util.MillisecondChronon;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetRecordReader;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.Test;

/**
 * Covers the binding of a row-group filter to one file's schema. The column's physical type is only known here,
 * so every case is about what that type does or does not allow; a comparison the file cannot support has to be
 * dropped rather than pushed, because parquet-mr rejects an ill-typed predicate by throwing when the file opens.
 */
public class ParquetFilterConverterTest {

    /** A collection whose only non-default reader setting is its timezone. */
    private static ReadOptions zoned(String zoneId) {
        return new ReadOptions(ZoneId.of(zoneId), false, false, false);
    }

    /** The common case: a collection with no timezone configured, so nothing the reader returns is shifted. */
    private static FilterPredicate toParquetFilter(MessageType fileSchema, ParquetFilterExpression expression) {
        return ParquetFilterConverter.toParquetFilter(fileSchema, expression, ReadOptions.none());
    }

    private static Comparison comparison(String[] path, Operator operator, ATypeTag tag, Object value) {
        return new Comparison(path, operator, tag, value);
    }

    private static Comparison equals(String column, ATypeTag tag, Object value) {
        return comparison(new String[] { column }, Operator.EQ, tag, value);
    }

    /** Every AsterixDB integer literal is 64-bit, so a 32-bit column must still be filterable. */
    @Test
    public void longLiteralBindsToAnIntegerColumn() {
        MessageType schema = MessageTypeParser.parseMessageType("message m { required int32 id; }");
        FilterPredicate predicate = toParquetFilter(schema, equals("id", ATypeTag.BIGINT, 5L));
        assertNotNull(predicate);
        assertEquals("eq(id, 5)", predicate.toString());
    }

    @Test
    public void longLiteralBindsToALongColumn() {
        MessageType schema = MessageTypeParser.parseMessageType("message m { required int64 id; }");
        FilterPredicate predicate = toParquetFilter(schema, equals("id", ATypeTag.BIGINT, 5L));
        assertNotNull(predicate);
        assertEquals("eq(id, 5)", predicate.toString());
    }

    /** Out of the column's range, the literal cannot be narrowed without changing which rows match. */
    @Test
    public void literalOutOfRangeOfAnIntegerColumnIsDropped() {
        MessageType schema = MessageTypeParser.parseMessageType("message m { required int32 id; }");
        assertNull(toParquetFilter(schema, equals("id", ATypeTag.BIGINT, Integer.MAX_VALUE + 1L)));
    }

    @Test
    public void repeatedColumnIsDropped() {
        MessageType schema = MessageTypeParser.parseMessageType("message m { repeated int64 tags; }");
        assertNull(toParquetFilter(schema, equals("tags", ATypeTag.BIGINT, 1L)));
    }

    @Test
    public void columnAbsentFromTheFileIsDropped() {
        MessageType schema = MessageTypeParser.parseMessageType("message m { required int64 id; }");
        assertNull(toParquetFilter(schema, equals("missing", ATypeTag.BIGINT, 1L)));
    }

    /**
     * parquet-mr's column factories split a name on '.', so a field whose own name contains one would resolve to
     * a nested path that does not exist. Such a predicate excludes every row group and reports nothing, which is
     * a wrong answer rather than a missed optimization.
     */
    @Test
    public void columnWhoseNameContainsADotIsDropped() {
        MessageType schema =
                Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT64).named("a.b").named("m");
        assertNull(toParquetFilter(schema, equals("a.b", ATypeTag.BIGINT, 1L)));
    }

    @Test
    public void nestedColumnIsPushed() {
        MessageType schema =
                MessageTypeParser.parseMessageType("message m { optional group outer { optional int64 inner; } }");
        FilterPredicate predicate = toParquetFilter(schema,
                comparison(new String[] { "outer", "inner" }, Operator.GT, ATypeTag.BIGINT, 7L));
        assertNotNull(predicate);
        assertEquals("gt(outer.inner, 7)", predicate.toString());
    }

    private static MessageType timestampSchema(LogicalTypeAnnotation.TimeUnit unit) {
        return timestampSchema(unit, true);
    }

    private static MessageType timestampSchema(LogicalTypeAnnotation.TimeUnit unit, boolean utcAdjusted) {
        return Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT64)
                .as(LogicalTypeAnnotation.timestampType(utcAdjusted, unit)).named("ts").named("m");
    }

    /** The literal as a user writes it: a wall-clock reading in the frame the collection displays. */
    private static long displayed(int year, int month, int day, int hour, int minute) {
        return LocalDateTime.of(year, month, day, hour, minute).toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    /**
     * The engine decides from the narrowed value, so the pushed filter has to admit the whole millisecond. Pushing
     * the instant instead excludes a row group holding a row the engine matches, which loses rows rather than
     * reading too many.
     */
    @Test
    public void datetimeIsPushedAsItsWholeMillisecond() {
        MessageType micros = timestampSchema(LogicalTypeAnnotation.TimeUnit.MICROS);
        assertEquals("and(gteq(ts, 1000), lt(ts, 2000))",
                toParquetFilter(micros, equals("ts", ATypeTag.DATETIME, 1L)).toString());
        assertEquals("lt(ts, 2000)",
                toParquetFilter(micros, comparison(new String[] { "ts" }, Operator.LT_EQ, ATypeTag.DATETIME, 1L))
                        .toString());
        assertEquals("lt(ts, 1000)",
                toParquetFilter(micros, comparison(new String[] { "ts" }, Operator.LT, ATypeTag.DATETIME, 1L))
                        .toString());
        assertEquals("gteq(ts, 2000)",
                toParquetFilter(micros, comparison(new String[] { "ts" }, Operator.GT, ATypeTag.DATETIME, 1L))
                        .toString());
        assertEquals("gteq(ts, 1000)",
                toParquetFilter(micros, comparison(new String[] { "ts" }, Operator.GT_EQ, ATypeTag.DATETIME, 1L))
                        .toString());
    }

    /** A millisecond column stores what the engine compares, so there is no window to widen to. */
    @Test
    public void millisecondColumnNeedsNoWindow() {
        MessageType millis = timestampSchema(LogicalTypeAnnotation.TimeUnit.MILLIS);
        assertEquals("eq(ts, 1)", toParquetFilter(millis, equals("ts", ATypeTag.DATETIME, 1L)).toString());
    }

    /**
     * Truncation runs toward zero rather than flooring, so the epoch millisecond is double width and the window
     * below the epoch sits above its literal. Getting either branch wrong drops rows on one side of 1970 only.
     */
    @Test
    public void epochAndPreEpochWindowsFollowTruncationTowardZero() {
        MessageType micros = timestampSchema(LogicalTypeAnnotation.TimeUnit.MICROS);
        assertEquals("and(gt(ts, -1000), lt(ts, 1000))",
                toParquetFilter(micros, equals("ts", ATypeTag.DATETIME, 0L)).toString());
        assertEquals("and(gt(ts, -2000), lteq(ts, -1000))",
                toParquetFilter(micros, equals("ts", ATypeTag.DATETIME, -1L)).toString());
    }

    @Test
    public void nanosecondColumnUsesANanosecondWideWindow() {
        MessageType nanos = timestampSchema(LogicalTypeAnnotation.TimeUnit.NANOS);
        assertEquals("and(gteq(ts, 1000000), lt(ts, 2000000))",
                toParquetFilter(nanos, equals("ts", ATypeTag.DATETIME, 1L)).toString());
    }

    /** Within one millisecond of the long bounds the far bound would wrap and invert the comparison. */
    @Test
    public void temporalLiteralAtTheLongBoundsIsDropped() {
        MessageType micros = timestampSchema(LogicalTypeAnnotation.TimeUnit.MICROS);
        assertNull(toParquetFilter(micros, equals("ts", ATypeTag.DATETIME, Long.MAX_VALUE / 1000)));
    }

    /**
     * A collection with a timezone shifts every UTC-adjusted timestamp on read, so the literal is written against
     * {@code stored + offset} while the row group's statistics hold {@code stored}. Pushing it unshifted excludes
     * precisely the row groups holding the matching rows, so this is a wrong answer rather than lost pruning.
     */
    @Test
    public void utcAdjustedLiteralIsConvertedOutOfTheDisplayedZone() {
        MessageType micros = timestampSchema(LogicalTypeAnnotation.TimeUnit.MICROS);
        // three hours ahead of the stored second, which is what the collection would have displayed for it
        long displayMillis = 1000L + TimeUnit.HOURS.toMillis(3);
        String converted = ParquetFilterConverter
                .toParquetFilter(micros, equals("ts", ATypeTag.DATETIME, displayMillis), zoned("+03:00")).toString();
        // the millisecond the stored value sits in -- what the same collection without a zone pushes for it
        assertEquals("and(gteq(ts, 1000000), lt(ts, 1001000))", converted);
        assertEquals(toParquetFilter(micros, equals("ts", ATypeTag.DATETIME, 1000L)).toString(), converted);
        // three hours away from the frame the literal was written in, which is what was pushed before
        assertEquals("and(gteq(ts, 10801000000), lt(ts, 10801001000))",
                toParquetFilter(micros, equals("ts", ATypeTag.DATETIME, displayMillis)).toString());
    }

    /** A wall-clock column has no zone to shift out of, so a configured zone must leave its literal alone. */
    @Test
    public void wallClockTimestampIsNotShifted() {
        MessageType micros = timestampSchema(LogicalTypeAnnotation.TimeUnit.MICROS, false);
        assertEquals("and(gteq(ts, 1000), lt(ts, 2000))", ParquetFilterConverter
                .toParquetFilter(micros, equals("ts", ATypeTag.DATETIME, 1L), zoned("+03:00")).toString());
    }

    /** {@code DateConverter} never applies the zone, so a date literal is already in the stored frame. */
    @Test
    public void dateLiteralIsNotShifted() {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT32)
                .as(LogicalTypeAnnotation.dateType()).named("d").named("m");
        assertEquals("eq(d, 19000)", ParquetFilterConverter
                .toParquetFilter(schema, equals("d", ATypeTag.DATE, 19000L), zoned("+03:00")).toString());
    }

    /** A named zone's offset depends on the date, so the conversion has to ask the rules rather than assume one. */
    @Test
    public void namedZoneUsesTheOffsetInForceOnThatDate() {
        MessageType millis = timestampSchema(LogicalTypeAnnotation.TimeUnit.MILLIS);
        long displayMillis = displayed(2026, 6, 15, 12, 0);
        // New York is four hours behind UTC in June, so a reading of 12:00 was stored as 16:00
        assertEquals("eq(ts, " + (displayMillis + TimeUnit.HOURS.toMillis(4)) + ")", ParquetFilterConverter
                .toParquetFilter(millis, equals("ts", ATypeTag.DATETIME, displayMillis), zoned("America/New_York"))
                .toString());
    }

    /**
     * The conversion is not a function at a daylight-saving boundary: an overlapped reading has two stored values
     * and one in the gap has none. Both give the comparison up, which only ever reads more row groups.
     */
    @Test
    public void daylightSavingBoundaryGivesTheComparisonUp() {
        MessageType millis = timestampSchema(LogicalTypeAnnotation.TimeUnit.MILLIS);
        ReadOptions newYork = zoned("America/New_York");
        // 02:30 never happens on the spring-forward day
        assertNull(ParquetFilterConverter.toParquetFilter(millis,
                equals("ts", ATypeTag.DATETIME, displayed(2026, 3, 8, 2, 30)), newYork));
        // 01:30 happens twice on the fall-back day
        assertNull(ParquetFilterConverter.toParquetFilter(millis,
                equals("ts", ATypeTag.DATETIME, displayed(2026, 11, 1, 1, 30)), newYork));
    }

    private static MessageType timeSchema(PrimitiveType.PrimitiveTypeName physical,
            LogicalTypeAnnotation.TimeUnit unit) {
        return Types.buildMessage().required(physical).as(LogicalTypeAnnotation.timeType(false, unit)).named("t")
                .named("m");
    }

    private static ReadOptions asNumbers() {
        return new ReadOptions(null, true, true, true);
    }

    /**
     * Read as a number, the reader returns the stored value itself. There is nothing narrowed to widen around, so
     * the literal is pushed exactly -- widening it here would read row groups the engine then rejects.
     */
    @Test
    public void timestampReadAsLongIsPushedExactly() {
        MessageType micros = timestampSchema(LogicalTypeAnnotation.TimeUnit.MICROS);
        assertEquals("eq(ts, 1234456)", ParquetFilterConverter
                .toParquetFilter(micros, equals("ts", ATypeTag.BIGINT, 1234456L), asNumbers()).toString());
    }

    /** An epoch value carries no zone, so a configured timezone must not move a literal compared against it. */
    @Test
    public void timestampReadAsLongIgnoresTheConfiguredZone() {
        MessageType micros = timestampSchema(LogicalTypeAnnotation.TimeUnit.MICROS);
        ReadOptions zonedNumbers = new ReadOptions(ZoneId.of("+03:00"), true, false, false);
        assertEquals("eq(ts, 1234456)", ParquetFilterConverter
                .toParquetFilter(micros, equals("ts", ATypeTag.BIGINT, 1234456L), zonedNumbers).toString());
    }

    /** A date read as a number is days since the epoch, and the column is still 32-bit. */
    @Test
    public void dateReadAsIntIsPushedExactly() {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT32)
                .as(LogicalTypeAnnotation.dateType()).named("d").named("m");
        assertEquals("eq(d, 19000)", ParquetFilterConverter
                .toParquetFilter(schema, equals("d", ATypeTag.BIGINT, 19000L), asNumbers()).toString());
    }

    /** A time is 32-bit for milliseconds and 64-bit for microseconds, so both widths have to bind. */
    @Test
    public void timeReadAsIntIsPushedExactlyAtEitherWidth() {
        assertEquals("eq(t, 3600000)",
                ParquetFilterConverter.toParquetFilter(
                        timeSchema(PrimitiveType.PrimitiveTypeName.INT32, LogicalTypeAnnotation.TimeUnit.MILLIS),
                        equals("t", ATypeTag.BIGINT, 3600000L), asNumbers()).toString());
        assertEquals("eq(t, 3600000000)",
                ParquetFilterConverter.toParquetFilter(
                        timeSchema(PrimitiveType.PrimitiveTypeName.INT64, LogicalTypeAnnotation.TimeUnit.MICROS),
                        equals("t", ATypeTag.BIGINT, 3600000000L), asNumbers()).toString());
    }

    /** Read as a TIME the value is narrowed and this does not model it, so the comparison is given up. */
    @Test
    public void timeColumnIsDroppedUnlessReadAsANumber() {
        assertNull(toParquetFilter(
                timeSchema(PrimitiveType.PrimitiveTypeName.INT32, LogicalTypeAnnotation.TimeUnit.MILLIS),
                equals("t", ATypeTag.TIME, 3600000L)));
    }

    /**
     * Reads a real file whose one row carries a sub-millisecond remainder, which is the case the un-widened form
     * lost: the row reads back as the literal, so it must survive every comparison the engine would match.
     */
    @Test
    public void subMillisecondRowSurvivesItsOwnMillisecond() throws Exception {
        File directory = Files.createTempDirectory("parquet-temporal-window").toFile();
        try {
            Path file = new Path(new File(directory, "ts.parquet").getAbsolutePath());
            MessageType schema = timestampSchema(LogicalTypeAnnotation.TimeUnit.MICROS);
            Configuration configuration = new Configuration();
            long storedMicros = 1_000_456L;
            long asEngineSeesIt = MillisecondChronon.narrow(storedMicros, java.util.concurrent.TimeUnit.MICROSECONDS);
            try (org.apache.parquet.hadoop.ParquetWriter<Group> writer = ExampleParquetWriter.builder(file)
                    .withType(schema).withConf(configuration).withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                    .withDictionaryEncoding(false).build()) {
                writer.write(new SimpleGroupFactory(schema).newGroup().append("ts", storedMicros));
            }
            for (Operator operator : new Operator[] { Operator.EQ, Operator.LT_EQ, Operator.GT_EQ }) {
                Comparison c = comparison(new String[] { "ts" }, operator, ATypeTag.DATETIME, asEngineSeesIt);
                assertEquals("row the engine matches was pruned by " + operator, 1,
                        read(file, configuration, schema, c));
            }
            // outside the window on either side, so the row group is genuinely not worth reading
            assertEquals(0, read(file, configuration, schema,
                    comparison(new String[] { "ts" }, Operator.LT, ATypeTag.DATETIME, asEngineSeesIt)));
            assertEquals(0, read(file, configuration, schema,
                    comparison(new String[] { "ts" }, Operator.GT, ATypeTag.DATETIME, asEngineSeesIt)));
        } finally {
            try (Stream<java.nio.file.Path> paths = Files.walk(directory.toPath())) {
                paths.sorted(Comparator.reverseOrder()).map(java.nio.file.Path::toFile).forEach(File::delete);
            }
        }
    }

    @Test
    public void stringLiteralBindsToAStringColumn() {
        MessageType schema = MessageTypeParser.parseMessageType("message m { required binary s (STRING); }");
        FilterPredicate predicate = toParquetFilter(schema, equals("s", ATypeTag.STRING, "abc"));
        assertNotNull(predicate);
        assertTrue(predicate.toString(), predicate.toString().startsWith("eq(s, "));
    }

    /** Raw binary does not order the way a string does. */
    @Test
    public void unannotatedBinaryColumnIsDropped() {
        MessageType schema = MessageTypeParser.parseMessageType("message m { required binary b; }");
        assertNull(toParquetFilter(schema, equals("b", ATypeTag.STRING, "abc")));
    }

    /** An unsigned column does not order the way the signed comparison the engine ran does. */
    @Test
    public void unsignedIntegerColumnIsDropped() {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT32)
                .as(LogicalTypeAnnotation.intType(32, false)).named("id").named("m");
        assertNull(toParquetFilter(schema, equals("id", ATypeTag.BIGINT, 1L)));
    }

    /** Narrowing a double literal to a float would move it, so the pushed comparison would not be the engine's. */
    @Test
    public void floatColumnIsDropped() {
        MessageType schema = MessageTypeParser.parseMessageType("message m { required float f; }");
        assertNull(toParquetFilter(schema, equals("f", ATypeTag.DOUBLE, 1.5d)));
    }

    @Test
    public void decimalColumnIsDropped() {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT64)
                .as(LogicalTypeAnnotation.decimalType(2, 10)).named("d").named("m");
        assertNull(toParquetFilter(schema, equals("d", ATypeTag.BIGINT, 1L)));
    }

    /** One conjunct that cannot be pushed leaves the other still pruning. */
    @Test
    public void conjunctionKeepsTheSideItCanPush() {
        MessageType schema = MessageTypeParser.parseMessageType("message m { required int64 id; repeated int64 t; }");
        FilterPredicate predicate = toParquetFilter(schema,
                new ParquetFilterExpression.And(equals("id", ATypeTag.BIGINT, 5L), equals("t", ATypeTag.BIGINT, 1L)));
        assertEquals("eq(id, 5)", predicate.toString());
    }

    /** One disjunct that cannot be pushed would keep row groups the other excludes, so neither is kept. */
    @Test
    public void disjunctionIsDroppedWholesale() {
        MessageType schema = MessageTypeParser.parseMessageType("message m { required int64 id; repeated int64 t; }");
        assertNull(toParquetFilter(schema,
                new ParquetFilterExpression.Or(equals("id", ATypeTag.BIGINT, 5L), equals("t", ATypeTag.BIGINT, 1L))));
    }

    /**
     * Reads a real file through the same call the reader makes, so the test fails if the predicate stops reaching
     * parquet-mr or stops excluding row groups -- which is the defect this binding exists to fix, and which no
     * assertion on the compiled plan can see.
     */
    @Test
    public void convertedFilterExcludesRowGroups() throws Exception {
        File directory = Files.createTempDirectory("parquet-row-group-filter").toFile();
        try {
            Path file = new Path(new File(directory, "rows.parquet").getAbsolutePath());
            MessageType schema = MessageTypeParser.parseMessageType("message m { required int32 id; }");
            Configuration configuration = new Configuration();
            SimpleGroupFactory groups = new SimpleGroupFactory(schema);
            try (org.apache.parquet.hadoop.ParquetWriter<Group> writer = ExampleParquetWriter.builder(file)
                    .withType(schema).withConf(configuration).withRowGroupSize(1024L)
                    .withCompressionCodec(CompressionCodecName.UNCOMPRESSED).withDictionaryEncoding(false).build()) {
                for (int id = 1; id <= 2000; id++) {
                    writer.write(groups.newGroup().append("id", id));
                }
            }
            int rowGroups;
            try (ParquetFileReader reader = ParquetFileReader.open(configuration, file)) {
                rowGroups = reader.getFooter().getBlocks().size();
            }
            assertTrue("fixture needs several row groups to show pruning, had " + rowGroups, rowGroups > 1);

            long all = read(file, configuration, schema, null);
            assertEquals(2000, all);
            // inside the data: the row group holding it is read whole, the rest are skipped. Rows other than the
            // matching one still come back, because only row groups are filtered here and the engine is what
            // decides which rows satisfy the predicate.
            long surviving = read(file, configuration, schema, equals("id", ATypeTag.BIGINT, 1500L));
            assertTrue("expected some row groups to be skipped, read " + surviving + " of " + all,
                    surviving > 0 && surviving < all);
            // outside every row group's statistics, so no row group is worth reading
            assertEquals(0, read(file, configuration, schema, equals("id", ATypeTag.BIGINT, 101500L)));
        } finally {
            try (Stream<java.nio.file.Path> paths = Files.walk(directory.toPath())) {
                paths.sorted(Comparator.reverseOrder()).map(java.nio.file.Path::toFile).forEach(File::delete);
            }
        }
    }

    /**
     * A zero literal is widened to admit both zeros, not declined: x > 0 is one of the commonest range predicates,
     * and giving it up would forfeit exactly the pruning it is most often written for.
     */
    @Test
    public void zeroLiteralIsWidenedRatherThanDeclined() {
        MessageType schema = MessageTypeParser.parseMessageType("message m { required double x; }");
        String[] x = { "x" };
        assertEquals("and(gteq(x, -0.0), lteq(x, 0.0))",
                toParquetFilter(schema, equals("x", ATypeTag.DOUBLE, 0.0d)).toString());
        assertEquals("and(gteq(x, -0.0), lteq(x, 0.0))",
                toParquetFilter(schema, equals("x", ATypeTag.DOUBLE, -0.0d)).toString());
        assertEquals("gt(x, 0.0)",
                toParquetFilter(schema, comparison(x, Operator.GT, ATypeTag.DOUBLE, 0.0d)).toString());
        assertEquals("gteq(x, -0.0)",
                toParquetFilter(schema, comparison(x, Operator.GT_EQ, ATypeTag.DOUBLE, 0.0d)).toString());
        assertEquals("lt(x, -0.0)",
                toParquetFilter(schema, comparison(x, Operator.LT, ATypeTag.DOUBLE, 0.0d)).toString());
        assertEquals("lteq(x, 0.0)",
                toParquetFilter(schema, comparison(x, Operator.LT_EQ, ATypeTag.DOUBLE, 0.0d)).toString());
        // an integer zero widens to the same double, so it needs the same treatment
        assertEquals("gt(x, 0.0)", toParquetFilter(schema, comparison(x, Operator.GT, ATypeTag.BIGINT, 0L)).toString());
        // any other literal is on the same side of both zeros in either order, so it goes through unchanged
        assertEquals("gt(x, 2.5)",
                toParquetFilter(schema, comparison(x, Operator.GT, ATypeTag.DOUBLE, 2.5d)).toString());
        assertNull(toParquetFilter(schema, equals("x", ATypeTag.DOUBLE, Double.NaN)));
    }

    /** The ordering guard fires only for literals that can disagree; ordinary text keeps its range pruning. */
    @Test
    public void stringRangeIsDeclinedOnlyForLiteralsTheOrdersDisagreeOn() {
        MessageType schema = MessageTypeParser.parseMessageType("message m { required binary s (STRING); }");
        String[] s = { "s" };
        // ASCII, accented Latin and CJK all sit below U+D800, so both orders agree and the range is pushed
        assertNotNull(toParquetFilter(schema, comparison(s, Operator.LT, ATypeTag.STRING, "abc")));
        assertNotNull(toParquetFilter(schema, comparison(s, Operator.GT_EQ, ATypeTag.STRING, "caf\u00e9")));
        assertNotNull(toParquetFilter(schema, comparison(s, Operator.LT, ATypeTag.STRING, "\u4e2d\u6587")));
        // a character in U+E000..U+FFFF or a supplementary one can be ordered differently, so the range is declined
        assertNull(toParquetFilter(schema, comparison(s, Operator.LT, ATypeTag.STRING, String.valueOf((char) 0xFF04))));
        assertNull(toParquetFilter(schema,
                comparison(s, Operator.GT, ATypeTag.STRING, new String(Character.toChars(0x1F600)))));
        // equality is kept for every literal, including the ones a range would decline
        assertNotNull(toParquetFilter(schema, equals("s", ATypeTag.STRING, new String(Character.toChars(0x1F600)))));
    }

    /** Writes one column holding the same value in every row, across enough row groups to show pruning. */
    private static Path singleValueFile(File directory, MessageType schema, String column, Object value)
            throws Exception {
        Path file = new Path(new File(directory, "values.parquet").getAbsolutePath());
        Configuration configuration = new Configuration();
        SimpleGroupFactory groups = new SimpleGroupFactory(schema);
        try (org.apache.parquet.hadoop.ParquetWriter<Group> writer = ExampleParquetWriter.builder(file).withType(schema)
                .withConf(configuration).withRowGroupSize(1024L).withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withDictionaryEncoding(false).build()) {
            for (int i = 0; i < 2000; i++) {
                Group group = groups.newGroup();
                if (value instanceof Double) {
                    group.append(column, (Double) value);
                } else {
                    group.append(column, (String) value);
                }
                writer.write(group);
            }
        }
        return file;
    }

    private static void deleteRecursively(File directory) throws Exception {
        try (Stream<java.nio.file.Path> paths = Files.walk(directory.toPath())) {
            paths.sorted(Comparator.reverseOrder()).map(java.nio.file.Path::toFile).forEach(File::delete);
        }
    }

    /**
     * The engine compares doubles numerically, where -0.0 equals 0.0, while parquet-mr orders statistics with
     * Double.compare, where -0.0 sorts below 0.0. A row group holding only -0.0 therefore has a maximum below a
     * pushed 0.0, and excluding it loses every row the engine would match.
     */
    @Test
    public void negativeZeroRowsSurviveAPositiveZeroLiteral() throws Exception {
        File directory = Files.createTempDirectory("parquet-negative-zero").toFile();
        try {
            MessageType schema = MessageTypeParser.parseMessageType("message m { required double x; }");
            Path file = singleValueFile(directory, schema, "x", -0.0d);
            Configuration configuration = new Configuration();
            long all = read(file, configuration, schema, null);
            // -0.0 = 0.0 and -0.0 >= 0.0 both hold for the engine, so every row group has to survive
            assertEquals(all, readOrAll(file, configuration, schema, equals("x", ATypeTag.DOUBLE, 0.0d)));
            assertEquals(all, readOrAll(file, configuration, schema,
                    comparison(new String[] { "x" }, Operator.GT_EQ, ATypeTag.DOUBLE, 0.0d)));
        } finally {
            deleteRecursively(directory);
        }
    }

    /** The mirror case: +0.0 data against a pushed -0.0. */
    @Test
    public void positiveZeroRowsSurviveANegativeZeroLiteral() throws Exception {
        File directory = Files.createTempDirectory("parquet-positive-zero").toFile();
        try {
            MessageType schema = MessageTypeParser.parseMessageType("message m { required double x; }");
            Path file = singleValueFile(directory, schema, "x", 0.0d);
            Configuration configuration = new Configuration();
            long all = read(file, configuration, schema, null);
            assertEquals(all, readOrAll(file, configuration, schema, equals("x", ATypeTag.DOUBLE, -0.0d)));
            assertEquals(all, readOrAll(file, configuration, schema,
                    comparison(new String[] { "x" }, Operator.LT_EQ, ATypeTag.DOUBLE, -0.0d)));
        } finally {
            deleteRecursively(directory);
        }
    }

    /**
     * The engine orders strings by UTF-16 code unit, parquet-mr by unsigned UTF-8 byte, which is code-point order.
     * They disagree where a character in U+E000..U+FFFF meets a supplementary character: U+1F600 is a surrogate
     * pair starting 0xD83D, so the engine sorts it below U+FF04, while its code point sorts above. A range pushed
     * in parquet's order then excludes the row group holding a row the engine matches.
     */
    @Test
    public void supplementaryCharacterRowsSurviveAStringRangeLiteral() throws Exception {
        File directory = Files.createTempDirectory("parquet-string-order").toFile();
        try {
            MessageType schema = MessageTypeParser.parseMessageType("message m { required binary s (STRING); }");
            String emoji = new String(Character.toChars(0x1F600));
            String fullwidthDollar = String.valueOf((char) 0xFF04);
            Path file = singleValueFile(directory, schema, "s", emoji);
            Configuration configuration = new Configuration();
            long all = read(file, configuration, schema, null);
            // the engine: emoji < fullwidth dollar, so every row matches s < fullwidth dollar
            assertEquals(all, readOrAll(file, configuration, schema,
                    comparison(new String[] { "s" }, Operator.LT, ATypeTag.STRING, fullwidthDollar)));
            // equality is order-independent: a row group holding the value always spans it in parquet's own order
            assertEquals(all, readOrAll(file, configuration, schema, equals("s", ATypeTag.STRING, emoji)));
        } finally {
            deleteRecursively(directory);
        }
    }

    /** Reads with the comparison pushed, or reads everything when the converter declines to push it. */
    private static long readOrAll(Path file, Configuration configuration, MessageType schema,
            ParquetFilterExpression expression) throws Exception {
        return toParquetFilter(schema, expression) == null ? read(file, configuration, schema, null)
                : read(file, configuration, schema, expression);
    }

    private static long read(Path file, Configuration configuration, MessageType schema,
            ParquetFilterExpression expression) throws Exception {
        JobConf jobConf = new JobConf(configuration);
        jobConf.set(ParquetInputFormat.READ_SUPPORT_CLASS, GroupReadSupport.class.getName());
        if (expression != null) {
            FilterPredicate predicate = toParquetFilter(schema, expression);
            assertNotNull("expected the comparison to be pushable", predicate);
            ParquetInputFormat.setFilterPredicate(jobConf, predicate);
            jobConf.setBoolean(ParquetInputFormat.RECORD_FILTERING_ENABLED, false);
            jobConf.setBoolean(ParquetInputFormat.COLUMN_INDEX_FILTERING_ENABLED, false);
        }
        long length = file.getFileSystem(jobConf).getFileStatus(file).getLen();
        ParquetRecordReader<Group> reader =
                new ParquetRecordReader<>(new GroupReadSupport(), ParquetInputFormat.getFilter(jobConf));
        try {
            reader.initialize(new FileSplit(file, 0, length, (String[]) null), jobConf, Reporter.NULL);
            long rows = 0;
            while (reader.nextKeyValue()) {
                rows++;
            }
            return rows;
        } finally {
            reader.close();
        }
    }
}
