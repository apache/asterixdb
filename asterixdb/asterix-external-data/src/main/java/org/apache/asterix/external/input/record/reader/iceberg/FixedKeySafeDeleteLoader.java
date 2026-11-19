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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.StructLikeSet;

/**
 * Iceberg's delete loader, except that equality deletes keyed on a {@code fixed} column are read without reusing
 * containers.
 * <p>
 * Iceberg reads an equality delete file reusing one record for every row and keeps each row through
 * {@code Record.copy()}, which deep-copies a binary value but not the {@code byte[]} of a fixed one, while the Parquet
 * fixed reader refills that same array for every row. Every kept key therefore holds the last key read, and every
 * other deleted row is returned. Reading those files with fresh containers gives each key an array of its own. Delete
 * files of any other format keep Iceberg's path, as no other format is read here.
 * <p>
 * The rows read this way go through {@link #canCache} and {@link #getOrLoad} exactly as Iceberg's own loads do, so a
 * caching subclass shares them across readers. That is safe for the same reason it is for any other key: each row and
 * its array are its own and are only read once loaded.
 */
class FixedKeySafeDeleteLoader extends BaseDeleteLoader {

    private final Function<DeleteFile, InputFile> loadInputFile;

    FixedKeySafeDeleteLoader(Function<DeleteFile, InputFile> loadInputFile) {
        super(loadInputFile);
        this.loadInputFile = loadInputFile;
    }

    @Override
    public StructLikeSet loadEqualityDeletes(Iterable<DeleteFile> deleteFiles, Schema projection) {
        if (!hasFixedField(projection) || !allParquet(deleteFiles)) {
            return super.loadEqualityDeletes(deleteFiles, projection);
        }
        StructLikeSet deleteSet = StructLikeSet.create(projection.asStruct());
        for (DeleteFile deleteFile : deleteFiles) {
            long size = estimatedSize(deleteFile, projection);
            Iterable<StructLike> rows = canCache(size)
                    ? getOrLoad(deleteFile.location(), () -> readEqualityDeletes(deleteFile, projection), size)
                    : readEqualityDeletes(deleteFile, projection);
            for (StructLike row : rows) {
                deleteSet.add(row);
            }
        }
        return deleteSet;
    }

    private List<StructLike> readEqualityDeletes(DeleteFile deleteFile, Schema projection) {
        // converts the stored representation to the one the data rows are compared in, as Iceberg's own load does
        InternalRecordWrapper asStructLike = new InternalRecordWrapper(projection.asStruct());
        List<StructLike> rows = new ArrayList<>();
        try (CloseableIterable<Record> records = Parquet.read(loadInputFile.apply(deleteFile)).project(projection)
                .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(projection, fileSchema)).build()) {
            for (Record record : records) {
                rows.add(asStructLike.copyFor(record));
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to close equality delete file " + deleteFile.location(), e);
        }
        return Collections.unmodifiableList(rows);
    }

    /** The estimate Iceberg charges for an equality delete file it loads: its rows times the key's estimated size. */
    private static long estimatedSize(DeleteFile deleteFile, Schema projection) {
        long rowSize = projection.columns().stream().mapToLong(TypeUtil::estimateSize).sum();
        try {
            return Math.multiplyExact(deleteFile.recordCount(), rowSize);
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    /** @return whether {@code schema} has a {@code fixed} field at any depth */
    static boolean hasFixedField(Schema schema) {
        return TypeUtil.indexById(schema.asStruct()).values().stream()
                .anyMatch(field -> field.type().typeId() == Type.TypeID.FIXED);
    }

    private static boolean allParquet(Iterable<DeleteFile> deleteFiles) {
        for (DeleteFile deleteFile : deleteFiles) {
            if (deleteFile.format() != FileFormat.PARQUET) {
                return false;
            }
        }
        return true;
    }
}
