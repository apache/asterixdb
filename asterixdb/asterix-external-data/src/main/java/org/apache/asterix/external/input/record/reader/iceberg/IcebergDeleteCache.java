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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.DeleteLoader;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.util.StructLikeSet;

/**
 * Delete files loaded once per scan on a node rather than once per data file they apply to.
 * <p>
 * An equality delete applies to every data file of its partition written before it, so without a cache each of those
 * data files reloads it: a table with a handful of delete files costs a remote read and a rebuilt set per data file.
 * One instance is shared by every reader of a scan on a node, the same way Iceberg's executor-wide delete cache is
 * shared by concurrently running tasks: the loaded values are only read after they are built.
 * <p>
 * Entries are kept for the lifetime of the scan. A delete file whose estimated in-memory size exceeds
 * {@link #MAX_ENTRY_SIZE}, or that would take the scan past {@link #MAX_TOTAL_SIZE}, is loaded the uncached way, and
 * so is an equality delete keyed on a field inside a struct, whose loaded rows are not safe to share.
 */
final class IcebergDeleteCache {

    static final long MAX_ENTRY_SIZE = 64L * 1024 * 1024;
    static final long MAX_TOTAL_SIZE = 128L * 1024 * 1024;

    private final long maxEntrySize;
    private final long maxTotalSize;
    private final ConcurrentMap<String, FutureTask<Object>> entries = new ConcurrentHashMap<>();
    private final AtomicLong cachedSize = new AtomicLong();

    IcebergDeleteCache() {
        this(MAX_ENTRY_SIZE, MAX_TOTAL_SIZE);
    }

    IcebergDeleteCache(long maxEntrySize, long maxTotalSize) {
        this.maxEntrySize = maxEntrySize;
        this.maxTotalSize = maxTotalSize;
    }

    /**
     * @return a delete loader reading through this cache, opening delete files with {@code loadInputFile}
     */
    DeleteLoader newLoader(Function<DeleteFile, InputFile> loadInputFile) {
        return new BaseDeleteLoader(loadInputFile) {
            // Iceberg asks this before it looks the file up, so the total budget is not applied here: a file already
            // in the cache is served from it however full the cache is, and a miss that does not fit is loaded
            // uncached in getOrLoad.
            @Override
            protected boolean canCache(long size) {
                return size <= maxEntrySize;
            }

            @Override
            protected <V> V getOrLoad(String key, Supplier<V> valueSupplier, long valueSize) {
                return IcebergDeleteCache.this.getOrLoad(key, valueSupplier, valueSize);
            }

            // The rows Iceberg loads for an equality delete keyed on a field inside a struct all read that struct
            // through one shared wrapper, which every read re-points, so readers sharing them concurrently see each
            // other's values and keep rows that are deleted. Those files are loaded per reader instead.
            @Override
            public StructLikeSet loadEqualityDeletes(Iterable<DeleteFile> deleteFiles, Schema projection) {
                if (hasStructField(projection)) {
                    return new BaseDeleteLoader(loadInputFile).loadEqualityDeletes(deleteFiles, projection);
                }
                return super.loadEqualityDeletes(deleteFiles, projection);
            }
        };
    }

    private static boolean hasStructField(Schema schema) {
        return schema.columns().stream().anyMatch(field -> field.type().isStructType());
    }

    int size() {
        return entries.size();
    }

    long cachedSize() {
        return cachedSize.get();
    }

    // The load runs outside any map lock so that readers of other delete files are not held up, and a reader asking
    // for a file another reader is loading waits for that load instead of repeating it. A failed load is not kept,
    // so the next reader retries it.
    @SuppressWarnings("unchecked")
    private <V> V getOrLoad(String key, Supplier<V> valueSupplier, long valueSize) {
        FutureTask<Object> task = entries.get(key);
        if (task == null) {
            if (cachedSize.get() + valueSize > maxTotalSize) {
                return valueSupplier.get();
            }
            FutureTask<Object> created = new FutureTask<>(valueSupplier::get);
            task = entries.putIfAbsent(key, created);
            if (task == null) {
                task = created;
                cachedSize.addAndGet(valueSize);
                created.run();
            }
        }
        try {
            return (V) task.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while loading delete file " + key, e);
        } catch (ExecutionException e) {
            if (entries.remove(key, task)) {
                cachedSize.addAndGet(-valueSize);
            }
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new IllegalStateException("failed to load delete file " + key, cause);
        }
    }
}
