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
import java.io.UncheckedIOException;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.asterix.external.util.iceberg.RequestedVariantPaths;
import org.apache.asterix.external.util.iceberg.VariantProjectionPlan;
import org.apache.asterix.external.util.iceberg.VariantSchemaClipper;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.parquet.ParquetBloomRowGroupFilter;
import org.apache.iceberg.parquet.ParquetDictionaryRowGroupFilter;
import org.apache.iceberg.parquet.ParquetMetricsRowGroupFilter;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.parquet.ParquetValueReader;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;

/**
 * Reads an Iceberg Parquet data file with the shredded {@code VARIANT} sub-columns a query does not reference pruned
 * away, so their column chunks are never fetched from storage.
 * <p>
 * This exists because Iceberg's own {@code Parquet.read()} cannot express it: its {@code ReadConf} derives the physical
 * Parquet requested schema from the Iceberg {@link Schema}, and a variant is a single opaque {@code VariantType} there,
 * so every {@code typed_value} sub-column is always requested. The requested schema and the value-reader model must
 * agree, so both have to be built from the clipped schema — which means driving {@link ParquetFileReader} directly.
 * <p>
 * It deliberately mirrors Iceberg's {@code ReadConf} + {@code ParquetReader.FileIterator} so behaviour is unchanged
 * apart from the narrower physical schema:
 * <ul>
 * <li>the split is applied through {@link ParquetReadOptions.Builder#withRange(long, long)}, exactly as Iceberg does;
 * <li>row groups are filtered by the same three public filters ({@link ParquetMetricsRowGroupFilter},
 * {@link ParquetDictionaryRowGroupFilter}, {@link ParquetBloomRowGroupFilter}) and are given the <em>unclipped</em>
 * projection, so row-group skipping decisions are identical to today's; and
 * <li>the record loop skips/reads row groups and feeds page sources to the model the same way.
 * </ul>
 * Only {@link ParquetFileReader#setRequestedSchema} and {@link GenericParquetReaders#buildReader} receive the clipped
 * schema. Iceberg's variant reader building is reused untouched: {@code buildReader} dispatches to Iceberg's
 * {@code VariantReaderBuilder}, which materializes exactly the {@code typed_value} members present in the schema it is
 * handed, so pruning falls out of the clip. {@code metadata} and the residual {@code value} are always retained by
 * {@link VariantSchemaClipper}, so unshredded and requested-but-residual fields still reconstruct.
 * <p>
 * Callers must treat this as best-effort: use {@link #canPrune()} to check that clipping actually narrowed the schema
 * for this file, and fall back to Iceberg's standard read path otherwise (or on any failure).
 */
public final class VariantProjectedParquetReader implements CloseableIterable<Record> {

    private final org.apache.iceberg.io.InputFile input;
    private final Schema expectedSchema;
    private final Expression filter;
    private final long splitStart;
    private final long splitLength;
    private final boolean caseSensitive;
    private final VariantProjectionPlan plan;

    /**
     * Whether position deletes will be applied to this read. Fixed at open time because it decides whether the
     * fail-safe row-index check in {@link #init()} runs; the bitmap itself arrives later via
     * {@link #withDeletedPositions}, so a caller can decline on {@link #canPrune()} before paying to load it.
     */
    private final boolean applyPositionDeletes;
    /** Positions deleted in THIS data file; set only when {@link #applyPositionDeletes}, and only before iteration. */
    private PositionDeleteIndex deletedPositions;

    private ParquetFileReader reader;
    private MessageType clippedProjection;
    private boolean[] shouldSkip;
    /** Per row group, its file-absolute first row index; only populated when {@link #applyPositionDeletes}. */
    private long[] rowGroupFirstRow;
    private long totalValues;
    private boolean pruned;

    private VariantProjectedParquetReader(org.apache.iceberg.io.InputFile input, Schema expectedSchema,
            Expression filter, long splitStart, long splitLength, boolean caseSensitive, VariantProjectionPlan plan,
            boolean applyPositionDeletes) {
        this.input = input;
        this.expectedSchema = expectedSchema;
        this.filter = filter;
        this.splitStart = splitStart;
        this.splitLength = splitLength;
        this.caseSensitive = caseSensitive;
        this.plan = plan;
        this.applyPositionDeletes = applyPositionDeletes;
    }

    /**
     * Opens {@code input} and prepares a pruned read. On success the returned reader is positioned to iterate; the
     * caller must still check {@link #canPrune()} and close/discard it when pruning would not narrow anything.
     *
     * @throws IOException if the file cannot be opened or its footer read
     */
    public static VariantProjectedParquetReader open(org.apache.iceberg.io.InputFile input, Schema expectedSchema,
            Expression filter, long splitStart, long splitLength, boolean caseSensitive, VariantProjectionPlan plan)
            throws IOException {
        return open(input, expectedSchema, filter, splitStart, splitLength, caseSensitive, plan, false);
    }

    /**
     * Opens for a read that will apply position deletes, without yet supplying them.
     * <p>
     * Two phases on purpose. Loading a deletion vector or merging position-delete files costs real IO, and this read
     * is declined whenever the clip is a no-op for the file or a row group lacks its first-row index. Callers should
     * check {@link #canPrune()} first and only then load positions and hand them over with
     * {@link #withDeletedPositions}; declining after the load would pay for the bitmap twice, since the standard
     * delete path loads its own.
     * <p>
     * Position deletes are applied here rather than through {@code DeleteFilter.filter(..)} because that route needs
     * the synthetic {@code _pos} column materialized into every row, which the pruned physical projection cannot
     * express. Skipping against the bitmap reads and decodes nothing extra.
     *
     * @param applyPositionDeletes {@code true} when the task carries position deletes or a deletion vector, so the
     *            fail-safe row-index check runs and {@link #withDeletedPositions} becomes mandatory before iterating
     * @throws IOException if the file cannot be opened or its footer read
     */
    public static VariantProjectedParquetReader open(org.apache.iceberg.io.InputFile input, Schema expectedSchema,
            Expression filter, long splitStart, long splitLength, boolean caseSensitive, VariantProjectionPlan plan,
            boolean applyPositionDeletes) throws IOException {
        VariantProjectedParquetReader created = new VariantProjectedParquetReader(input, expectedSchema, filter,
                splitStart, splitLength, caseSensitive, plan, applyPositionDeletes);
        try {
            created.init();
        } catch (Exception e) {
            created.closeQuietly();
            throw e instanceof IOException ? (IOException) e : new IOException(e);
        }
        return created;
    }

    /**
     * Convenience for callers that already hold the bitmap: opens with position deletes enabled and attaches them.
     * Equivalent to {@code open(.., true).withDeletedPositions(deletedPositions)}; {@code null} means no position
     * deletes at all.
     */
    public static VariantProjectedParquetReader open(org.apache.iceberg.io.InputFile input, Schema expectedSchema,
            Expression filter, long splitStart, long splitLength, boolean caseSensitive, VariantProjectionPlan plan,
            PositionDeleteIndex deletedPositions) throws IOException {
        VariantProjectedParquetReader reader = open(input, expectedSchema, filter, splitStart, splitLength,
                caseSensitive, plan, deletedPositions != null);
        return deletedPositions != null ? reader.withDeletedPositions(deletedPositions) : reader;
    }

    /**
     * Supplies the positions to skip. Required exactly when the reader was opened with position deletes enabled, and
     * must precede {@link #iterator()}.
     */
    public VariantProjectedParquetReader withDeletedPositions(PositionDeleteIndex positions) {
        if (!applyPositionDeletes) {
            throw new IllegalStateException("reader was not opened for position deletes");
        }
        if (positions == null) {
            throw new IllegalArgumentException("positions");
        }
        this.deletedPositions = positions;
        return this;
    }

    /**
     * @return {@code true} if clipping actually removed at least one shredded sub-column for this file. When
     *         {@code false} the caller should close this reader and use the standard read path, since there is nothing
     *         to gain and no reason to run replicated read logic.
     */
    public boolean canPrune() {
        return pruned;
    }

    private void init() throws IOException {
        ParquetReadOptions.Builder optionsBuilder = ParquetReadOptions.builder();
        if (splitLength > 0) {
            // Same range semantics Iceberg uses for a split: row groups are selected by midpoint within [start, end).
            optionsBuilder.withRange(splitStart, splitStart + splitLength);
        }
        reader = ParquetFileReader.open(parquetInputFile(input), optionsBuilder.build());

        MessageType fileSchema = reader.getFileMetaData().getSchema();
        // Mirror ReadConf's projection derivation so the starting point is exactly Iceberg's.
        MessageType projection;
        if (ParquetSchemaUtil.hasIds(fileSchema)) {
            projection = ParquetSchemaUtil.pruneColumns(fileSchema, expectedSchema);
        } else {
            projection = ParquetSchemaUtil.pruneColumnsFallback(ParquetSchemaUtil.addFallbackIds(fileSchema),
                    expectedSchema);
        }

        // Clip each planned variant column's typed_value down to the requested sub-paths. Per file: the same column can
        // be shredded here and residual-only elsewhere, and the clipper no-ops whenever it cannot narrow safely.
        MessageType clipped = projection;
        for (List<String> column : plan.columns()) {
            RequestedVariantPaths paths = plan.get(column);
            clipped = VariantSchemaClipper.clip(clipped, column, paths);
        }
        pruned = clipped != projection;
        clippedProjection = clipped;

        List<BlockMetaData> rowGroups = reader.getRowGroups();
        shouldSkip = new boolean[rowGroups.size()];
        // Row-group filtering uses the UNCLIPPED projection, exactly as Iceberg does, so skipping decisions (and thus
        // which rows are returned) are identical to the standard path.
        if (filter != null && filter != Expressions.alwaysTrue()) {
            ParquetMetricsRowGroupFilter statsFilter =
                    new ParquetMetricsRowGroupFilter(expectedSchema, filter, caseSensitive);
            ParquetDictionaryRowGroupFilter dictFilter =
                    new ParquetDictionaryRowGroupFilter(expectedSchema, filter, caseSensitive);
            ParquetBloomRowGroupFilter bloomFilter =
                    new ParquetBloomRowGroupFilter(expectedSchema, filter, caseSensitive);
            for (int i = 0; i < shouldSkip.length; i++) {
                BlockMetaData rowGroup = rowGroups.get(i);
                boolean shouldRead = statsFilter.shouldRead(projection, rowGroup)
                        && dictFilter.shouldRead(projection, rowGroup, reader.getDictionaryReader(rowGroup))
                        && bloomFilter.shouldRead(projection, rowGroup, reader.getBloomFilterDataReader(rowGroup));
                shouldSkip[i] = !shouldRead;
                if (shouldRead) {
                    totalValues += rowGroup.getRowCount();
                }
            }
        } else {
            for (BlockMetaData rowGroup : rowGroups) {
                totalValues += rowGroup.getRowCount();
            }
        }

        if (applyPositionDeletes) {
            // Fail-safe: establish every row group's file-absolute first row index NOW, before a single row is
            // produced. Parquet records it per row group (a prefix sum taken over the UNFILTERED footer, so it stays
            // correct when a split range drops earlier row groups); -1 means the file does not carry it, and a
            // position delete cannot then be applied safely. Declining here degrades to the standard delete-aware
            // path; discovering it mid-iteration could not, because rows would already have been emitted.
            rowGroupFirstRow = new long[rowGroups.size()];
            for (int i = 0; i < rowGroups.size(); i++) {
                long firstRow = rowGroups.get(i).getRowIndexOffset();
                if (firstRow < 0 && !shouldSkip[i]) {
                    pruned = false;
                    return;
                }
                rowGroupFirstRow[i] = firstRow;
            }
        }

        // The narrowed physical schema: only these column chunks are fetched.
        reader.setRequestedSchema(clippedProjection);
    }

    @Override
    public CloseableIterator<Record> iterator() {
        if (applyPositionDeletes && deletedPositions == null) {
            // Refusing here is what keeps the two-phase open safe: without it, a caller that forgot the bitmap would
            // silently return every deleted row.
            throw new IllegalStateException("opened for position deletes but withDeletedPositions(..) was not called");
        }
        ParquetValueReader<Record> model = GenericParquetReaders.buildReader(expectedSchema, clippedProjection);
        return new RecordIterator(reader, model, shouldSkip, totalValues, deletedPositions, rowGroupFirstRow);
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
            reader = null;
        }
    }

    private void closeQuietly() {
        try {
            close();
        } catch (IOException ignored) {
            // best-effort cleanup on a failed open
        }
    }

    /**
     * Mirrors Iceberg's {@code ParquetReader.FileIterator}, plus position-delete skipping.
     * <p>
     * Without deletes this is a one-to-one loop and {@code valuesRead < totalValues} answers {@link #hasNext()}
     * exactly. Position deletes break that invariant — rows read and rows emitted diverge — so the iterator carries a
     * one-record look-ahead. A deleted row is still <em>read</em> from the page source and discarded, because the
     * column readers are a stream: skipping the call would desynchronize every subsequent value. The saving is in the
     * pruned column chunks, which were never fetched at all, not in skipping the decode of a deleted row.
     */
    private static final class RecordIterator implements CloseableIterator<Record> {
        private final ParquetFileReader reader;
        private final ParquetValueReader<Record> model;
        private final boolean[] shouldSkip;
        private final long totalValues;
        private final PositionDeleteIndex deletedPositions;
        private final long[] rowGroupFirstRow;

        private int nextRowGroup = 0;
        private long nextRowGroupStart = 0;
        private long valuesRead = 0;

        /** File-absolute position of the next row to be read, tracked only when position deletes are being applied. */
        private long currentRowGroupFirstRow = -1;
        private long rowsReadInCurrentRowGroup = 0;

        private Record lookAhead;
        private boolean lookAheadReady;

        private RecordIterator(ParquetFileReader reader, ParquetValueReader<Record> model, boolean[] shouldSkip,
                long totalValues, PositionDeleteIndex deletedPositions, long[] rowGroupFirstRow) {
            this.reader = reader;
            this.model = model;
            this.shouldSkip = shouldSkip;
            this.totalValues = totalValues;
            this.deletedPositions = deletedPositions;
            this.rowGroupFirstRow = rowGroupFirstRow;
        }

        @Override
        public boolean hasNext() {
            if (deletedPositions == null) {
                return valuesRead < totalValues;
            }
            if (!lookAheadReady) {
                fillLookAhead();
            }
            return lookAheadReady;
        }

        @Override
        public Record next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            if (deletedPositions != null) {
                Record value = lookAhead;
                lookAhead = null;
                lookAheadReady = false;
                return value;
            }
            return readNextRow();
        }

        /** Reads forward, discarding deleted rows, until a live row is found or the split is exhausted. */
        private void fillLookAhead() {
            while (valuesRead < totalValues) {
                Record value = readNextRow();
                // readNextRow() has already counted the row it returned, and reset the counter if it crossed into a
                // new row group, so the row just read is at offset (count - 1) within the current row group.
                long position = currentRowGroupFirstRow + rowsReadInCurrentRowGroup - 1;
                if (!deletedPositions.isDeleted(position)) {
                    lookAhead = value;
                    lookAheadReady = true;
                    return;
                }
            }
        }

        private Record readNextRow() {
            if (valuesRead >= nextRowGroupStart) {
                advance();
                rowsReadInCurrentRowGroup = 0;
            }
            // Containers are never reused here (Iceberg's reuseContainers is off on this read path).
            Record value = model.read(null);
            valuesRead += 1;
            rowsReadInCurrentRowGroup += 1;
            return value;
        }

        private void advance() {
            try {
                while (shouldSkip[nextRowGroup]) {
                    nextRowGroup += 1;
                    reader.skipNextRowGroup();
                }
                PageReadStore pages = reader.readNextRowGroup();
                nextRowGroupStart += pages.getRowCount();
                if (rowGroupFirstRow != null) {
                    currentRowGroupFirstRow = rowGroupFirstRow[nextRowGroup];
                }
                nextRowGroup += 1;
                model.setPageSource(pages);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void close() throws IOException {
            // The file reader is owned and closed by the enclosing CloseableIterable.
        }
    }

    /**
     * Adapts an Iceberg {@link org.apache.iceberg.io.InputFile} to a Parquet {@code InputFile}. Iceberg's own
     * {@code ParquetIO.file(..)} does this but is package-private, and this avoids adding a split-package class.
     */
    private static org.apache.parquet.io.InputFile parquetInputFile(org.apache.iceberg.io.InputFile in) {
        return new org.apache.parquet.io.InputFile() {
            @Override
            public long getLength() throws IOException {
                return in.getLength();
            }

            @Override
            public SeekableInputStream newStream() throws IOException {
                org.apache.iceberg.io.SeekableInputStream delegate = in.newStream();
                return new DelegatingSeekableInputStream(delegate) {
                    @Override
                    public long getPos() throws IOException {
                        return delegate.getPos();
                    }

                    @Override
                    public void seek(long newPos) throws IOException {
                        delegate.seek(newPos);
                    }
                };
            }
        };
    }
}
