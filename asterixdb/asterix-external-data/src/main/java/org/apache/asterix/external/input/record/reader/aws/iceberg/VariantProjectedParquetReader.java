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
import org.apache.hyracks.util.annotations.AiProvenance;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
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
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Reads a Parquet data file with unreferenced shredded variant sub-columns pruned from the physical "
        + "requested schema; mirrors Iceberg's ReadConf/FileIterator (split range, the three row-group "
        + "filters on the unclipped projection, page-source loop) and reuses GenericParquetReaders.buildReader")
public final class VariantProjectedParquetReader implements CloseableIterable<Record> {

    private final org.apache.iceberg.io.InputFile input;
    private final Schema expectedSchema;
    private final Expression filter;
    private final long splitStart;
    private final long splitLength;
    private final boolean caseSensitive;
    private final VariantProjectionPlan plan;

    private ParquetFileReader reader;
    private MessageType clippedProjection;
    private boolean[] shouldSkip;
    private long totalValues;
    private boolean pruned;

    private VariantProjectedParquetReader(org.apache.iceberg.io.InputFile input, Schema expectedSchema,
            Expression filter, long splitStart, long splitLength, boolean caseSensitive, VariantProjectionPlan plan) {
        this.input = input;
        this.expectedSchema = expectedSchema;
        this.filter = filter;
        this.splitStart = splitStart;
        this.splitLength = splitLength;
        this.caseSensitive = caseSensitive;
        this.plan = plan;
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
        VariantProjectedParquetReader created = new VariantProjectedParquetReader(input, expectedSchema, filter,
                splitStart, splitLength, caseSensitive, plan);
        try {
            created.init();
        } catch (Exception e) {
            created.closeQuietly();
            throw e instanceof IOException ? (IOException) e : new IOException(e);
        }
        return created;
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

        // The narrowed physical schema: only these column chunks are fetched.
        reader.setRequestedSchema(clippedProjection);
    }

    @Override
    public CloseableIterator<Record> iterator() {
        ParquetValueReader<Record> model = GenericParquetReaders.buildReader(expectedSchema, clippedProjection);
        return new RecordIterator(reader, model, shouldSkip, totalValues);
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

    /** Mirrors Iceberg's {@code ParquetReader.FileIterator}. */
    private static final class RecordIterator implements CloseableIterator<Record> {
        private final ParquetFileReader reader;
        private final ParquetValueReader<Record> model;
        private final boolean[] shouldSkip;
        private final long totalValues;

        private int nextRowGroup = 0;
        private long nextRowGroupStart = 0;
        private long valuesRead = 0;

        private RecordIterator(ParquetFileReader reader, ParquetValueReader<Record> model, boolean[] shouldSkip,
                long totalValues) {
            this.reader = reader;
            this.model = model;
            this.shouldSkip = shouldSkip;
            this.totalValues = totalValues;
        }

        @Override
        public boolean hasNext() {
            return valuesRead < totalValues;
        }

        @Override
        public Record next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            if (valuesRead >= nextRowGroupStart) {
                advance();
            }
            // Containers are never reused here (Iceberg's reuseContainers is off on this read path).
            Record value = model.read(null);
            valuesRead += 1;
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
