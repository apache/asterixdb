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
package org.apache.asterix.column.test.explosion;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.column.ColumnManagerFactory;
import org.apache.asterix.column.filter.NoOpColumnFilterEvaluatorFactory;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.asterix.column.operation.query.QueryColumnTupleProjector;
import org.apache.asterix.column.operation.query.QueryColumnWithMetaTupleProjector;
import org.apache.asterix.column.values.writer.ColumnValuesWriterFactory;
import org.apache.asterix.common.exceptions.NoOpWarningCollector;
import org.apache.asterix.formats.nontagged.BinaryComparatorFactoryProvider;
import org.apache.asterix.formats.nontagged.NullIntrospector;
import org.apache.asterix.formats.nontagged.TypeTraitProvider;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnManagerFactory;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.ColumnProjectorType;
import org.apache.hyracks.storage.am.lsm.btree.column.utils.ColumnUtil;
import org.apache.hyracks.storage.am.lsm.btree.column.utils.LSMColumnBTreeUtil;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTree;
import org.apache.hyracks.storage.am.lsm.btree.util.LSMBTreeTestHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.impls.VirtualBufferCache;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.BufferCache;
import org.apache.hyracks.storage.common.buffercache.ClockPageReplacementStrategy;
import org.apache.hyracks.storage.common.buffercache.ColumnBufferPool;
import org.apache.hyracks.storage.common.buffercache.DefaultDiskCachedPageAllocator;
import org.apache.hyracks.storage.common.buffercache.DelayPageCleanerPolicy;
import org.apache.hyracks.storage.common.buffercache.HeapBufferAllocator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.context.read.DefaultBufferCacheReadContextProvider;
import org.apache.hyracks.storage.common.compression.NoOpCompressorDecompressorFactory;
import org.apache.hyracks.storage.common.disk.NoOpDiskCacheMonitoringService;
import org.apache.hyracks.storage.common.file.FileMapManager;
import org.apache.hyracks.util.trace.ITracer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * A real columnar LSM index -- flushes and merges through the production LSM code, reads through the production
 * search cursor and query projector -- for tests that need components to be merged, which {@link ColumnFlushHarness}
 * cannot do. Records are JSON objects keyed by an integer {@code id}, each with a meta record when the index has a meta
 * type. The IO scheduler is synchronous and nothing merges unless asked, so every flush and merge has happened when
 * its call returns.
 */
public final class ColumnLsmHarness implements AutoCloseable {
    private static final int PAGE_SIZE = 32 * 1024;
    private static final int DISK_BYTES = 128 * 1024 * 1024;
    private static final int MEMORY_BYTES = 32 * 1024 * 1024;
    private static final int MUTABLE_COMPONENTS = 2;
    private static final int MAX_TUPLE_COUNT = 15000;
    private static final double TOLERANCE = 0.15;
    /** Where {@link #scan} puts a record's meta record: beside its fields, under this name. */
    public static final String META = "@meta";

    private final LSMBTreeTestHarness harness = new LSMBTreeTestHarness();
    private final ARecordType metaType;
    private final ArrayTupleBuilder tupleBuilder;
    private final ArrayTupleReference tuple = new ArrayTupleReference();
    private final ObjectMapper mapper = new ObjectMapper();
    private final IBufferCache diskBufferCache;
    private final ColumnBufferPool columnBufferPool;
    private static final IBinaryComparatorFactory[] KEY_COMPARATORS = new IBinaryComparatorFactory[] {
            BinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(BuiltinType.AINT64, true) };
    private final LSMBTree lsmBtree;
    private ILSMIndexAccessor accessor;

    public ColumnLsmHarness() throws Exception {
        this(PAGE_SIZE);
    }

    /** With pages of {@code pageSize}; page zero must hold the default 5000 columns, so a wide schema needs 128 KB. */
    public ColumnLsmHarness(int pageSize) throws Exception {
        this(pageSize, null);
    }

    /** With a meta record of {@code metaType} beside each record, as a dataset with a meta record has. */
    public ColumnLsmHarness(int pageSize, ARecordType metaType) throws Exception {
        this.metaType = metaType;
        tupleBuilder = new ArrayTupleBuilder(metaType == null ? 2 : 3);
        harness.setUp();
        IIOManager ioManager = harness.getIOManager();
        diskBufferCache = new BufferCache(ioManager,
                new ClockPageReplacementStrategy(new HeapBufferAllocator(), DefaultDiskCachedPageAllocator.INSTANCE,
                        pageSize, DISK_BYTES / pageSize),
                new DelayPageCleanerPolicy(1000), new FileMapManager(), Integer.MAX_VALUE, 10, Thread::new,
                new HashMap<>(), DefaultBufferCacheReadContextProvider.DEFAULT);
        // As a node's defaults: a 4 KB buffer per column, not a page, and 3% of a 32 GB heap, so a wide flush fits.
        columnBufferPool = new ColumnBufferPool(4 * 1024, 8000, 1024L * 1024 * 1024, TimeUnit.MINUTES.toMillis(2));
        List<IVirtualBufferCache> virtualBufferCaches = new ArrayList<>();
        for (int i = 0; i < MUTABLE_COMPONENTS; i++) {
            virtualBufferCaches
                    .add(new VirtualBufferCache(new HeapBufferAllocator(), pageSize, MEMORY_BYTES / pageSize));
        }
        ARecordType type = ColumnFlushHarness.KEYED_TYPE;
        ITypeTraits[] typeTraits = metaType == null
                ? new ITypeTraits[] { TypeTraitProvider.INSTANCE.getTypeTrait(BuiltinType.AINT64),
                        TypeTraitProvider.INSTANCE.getTypeTrait(type) }
                : new ITypeTraits[] { TypeTraitProvider.INSTANCE.getTypeTrait(BuiltinType.AINT64),
                        TypeTraitProvider.INSTANCE.getTypeTrait(type),
                        TypeTraitProvider.INSTANCE.getTypeTrait(metaType) };
        IColumnManagerFactory columnManagerFactory =
                new ColumnManagerFactory(type, metaType, Collections.singletonList(Collections.singletonList("id")),
                        Collections.singletonList(0), pageSize, MAX_TUPLE_COUNT, TOLERANCE, 8 * pageSize);
        CollapseSettings.configure(harness.getNCConfig().getConfigManager());
        lsmBtree = LSMColumnBTreeUtil.createLSMTree(harness.getNCConfig(), ioManager, virtualBufferCaches,
                harness.getFileReference(), diskBufferCache, columnBufferPool, typeTraits, KEY_COMPARATORS,
                new int[] { 0 }, 0.01, harness.getMergePolicy(), harness.getOperationTracker(),
                harness.getIOScheduler(), harness.getIOOperationCallbackFactory(),
                harness.getPageWriteCallbackFactory(), null, harness.getMetadataPageManagerFactory(), false,
                ITracer.NONE, NoOpCompressorDecompressorFactory.INSTANCE,
                TypeTraitProvider.INSTANCE.getTypeTrait(BuiltinType.ANULL), NullIntrospector.INSTANCE,
                columnManagerFactory, false, NoOpDiskCacheMonitoringService.INSTANCE);
        lsmBtree.create();
        lsmBtree.activate();
        accessor = lsmBtree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
    }

    /** Inserts each JSON record, keyed by its {@code id}, into the in-memory component. */
    public void insert(List<String> records) throws Exception {
        insert(records, null);
    }

    /** Inserts each JSON record with the JSON meta record at the same position of {@code metas}. */
    public void insert(List<String> records, List<String> metas) throws Exception {
        List<IValueReference> parsed = ColumnFlushHarness.parse(records, ColumnFlushHarness.KEYED_TYPE);
        List<IValueReference> parsedMetas = metas == null ? null : ColumnFlushHarness.parse(metas, metaType);
        for (int i = 0; i < records.size(); i++) {
            tupleBuilder.reset();
            writeTaggedInt64(tupleBuilder.getDataOutput(), mapper.readTree(records.get(i)).get("id").asLong());
            tupleBuilder.addFieldEndOffset();
            IValueReference record = parsed.get(i);
            tupleBuilder.addField(record.getByteArray(), record.getStartOffset(), record.getLength());
            if (parsedMetas != null) {
                IValueReference meta = parsedMetas.get(i);
                tupleBuilder.addField(meta.getByteArray(), meta.getStartOffset(), meta.getLength());
            }
            tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
            accessor.upsert(tuple);
        }
    }

    /** Deletes the records keyed {@code ids}, leaving antimatter that hides older components' versions. */
    public void delete(int... ids) throws HyracksDataException {
        ArrayTupleBuilder keyBuilder = new ArrayTupleBuilder(1);
        ArrayTupleReference key = new ArrayTupleReference();
        for (int id : ids) {
            keyBuilder.reset();
            writeTaggedInt64(keyBuilder.getDataOutput(), id);
            keyBuilder.addFieldEndOffset();
            key.reset(keyBuilder.getFieldEndOffsets(), keyBuilder.getByteArray());
            accessor.delete(key);
        }
    }

    /** Writes the in-memory component to a new disk component. */
    public void flush() throws HyracksDataException {
        throwIfFailed(accessor.scheduleFlush());
    }

    /**
     * Deactivates and reactivates the index, as a node restart does: the in-memory component is flushed, and the flush
     * metadata is rebuilt from the newest disk component's schema, without what the old metadata had counted.
     */
    public void restart() throws HyracksDataException {
        accessor.destroy();
        lsmBtree.deactivate();
        lsmBtree.activate();
        accessor = lsmBtree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
    }

    /** Merges every disk component into one. */
    public void mergeAll() throws HyracksDataException {
        throwIfFailed(accessor.scheduleFullMerge());
    }

    // The scheduler is synchronous, so the operation is done; a failed one only records its failure.
    private static void throwIfFailed(ILSMIOOperation op) throws HyracksDataException {
        if (op.getStatus() == ILSMIOOperation.LSMIOOperationStatus.FAILURE) {
            throw HyracksDataException.create(op.getFailure());
        }
    }

    /** The disk components, newest first. */
    public List<ILSMDiskComponent> diskComponents() {
        return lsmBtree.getDiskComponents();
    }

    /** The newest disk component's schema, as a flush would continue it. */
    public FlushColumnMetadata newestSchema() throws HyracksDataException {
        Mutable<IColumnWriteMultiPageOp> multiPageOpRef = new MutableObject<>();
        return FlushColumnMetadata.create(ColumnFlushHarness.KEYED_TYPE, metaType, 1, Collections.singletonList(0),
                new ColumnValuesWriterFactory(multiPageOpRef), multiPageOpRef,
                ColumnUtil.getColumnMetadataCopy(diskComponents().get(0).getMetadata()), CollapseSettings.current());
    }

    /** Each disk component's columns annotations, newest first; null for one without. */
    public List<IValueReference> annotations() throws HyracksDataException {
        List<IValueReference> annotations = new ArrayList<>();
        for (ILSMDiskComponent component : diskComponents()) {
            annotations.add(ColumnUtil.getColumnsAnnotationsCopy(component.getMetadata()));
        }
        return annotations;
    }

    /** Every record, as JSON in key order, read whole. */
    public List<String> scan() throws IOException {
        return scan(ProjectionFiltrationTypeUtil.ALL_FIELDS_TYPE);
    }

    /** Every record, as JSON in key order, read through a projection of {@code requested}. */
    public List<String> scan(ARecordType requested) throws IOException {
        return scan(requested, null);
    }

    /** The records keyed {@code from} and up, as JSON in key order, as a key-range search reads them. */
    public List<String> scanFrom(long from) throws IOException {
        return scan(ProjectionFiltrationTypeUtil.ALL_FIELDS_TYPE, from);
    }

    private List<String> scan(ARecordType requested, Long from) throws IOException {
        IndexAccessParameters iap =
                new IndexAccessParameters(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        QueryColumnTupleProjector projector = metaType == null
                ? new QueryColumnTupleProjector(ColumnFlushHarness.KEYED_TYPE, 1, requested, Collections.emptyMap(),
                        NoOpColumnFilterEvaluatorFactory.INSTANCE, NoOpColumnFilterEvaluatorFactory.INSTANCE,
                        NoOpWarningCollector.INSTANCE, null, ColumnProjectorType.QUERY)
                : new QueryColumnWithMetaTupleProjector(ColumnFlushHarness.KEYED_TYPE, metaType, 1, requested,
                        Collections.emptyMap(), ProjectionFiltrationTypeUtil.ALL_FIELDS_TYPE,
                        NoOpColumnFilterEvaluatorFactory.INSTANCE, NoOpColumnFilterEvaluatorFactory.INSTANCE,
                        NoOpWarningCollector.INSTANCE, null, ColumnProjectorType.QUERY);
        iap.getParameters().put(HyracksConstants.TUPLE_PROJECTOR, projector);
        ILSMIndexAccessor searchAccessor = lsmBtree.createAccessor(iap);
        IIndexCursor cursor = searchAccessor.createSearchCursor(false);
        List<String> out = new ArrayList<>();
        ArrayTupleBuilder projected = new ArrayTupleBuilder(metaType == null ? 2 : 3);
        try {
            ArrayTupleReference low = null;
            if (from != null) {
                ArrayTupleBuilder lowBuilder = new ArrayTupleBuilder(1);
                writeTaggedInt64(lowBuilder.getDataOutput(), from);
                lowBuilder.addFieldEndOffset();
                low = new ArrayTupleReference();
                low.reset(lowBuilder.getFieldEndOffsets(), lowBuilder.getByteArray());
            }
            MultiComparator lowCmp = low == null ? null : MultiComparator.create(KEY_COMPARATORS);
            searchAccessor.search(cursor, new RangePredicate(low, null, true, true, lowCmp, null));
            while (cursor.hasNext()) {
                cursor.next();
                // As the scan operator does: the cursor yields the columnar tuple, the projector assembles it.
                projected.reset();
                if (projector.project(cursor.getTuple(), projected.getDataOutput(), projected) == null) {
                    continue;
                }
                int[] ends = projected.getFieldEndOffsets();
                ArrayBackedValueStorage record = new ArrayBackedValueStorage();
                record.append(projected.getByteArray(), ends[0], ends[1] - ends[0]);
                // The key is stored as the primary-key column, not in the record: put it back, first.
                ObjectNode json = mapper.createObjectNode();
                json.put("id", LongPointable.getLong(projected.getByteArray(), 1));
                json.setAll((ObjectNode) mapper.readTree(ColumnFlushHarness.json(record)));
                if (metaType != null) {
                    ArrayBackedValueStorage meta = new ArrayBackedValueStorage();
                    meta.append(projected.getByteArray(), ends[1], ends[2] - ends[1]);
                    json.set(META, mapper.readTree(ColumnFlushHarness.json(meta)));
                }
                out.add(json.toString());
            }
        } finally {
            cursor.close();
            cursor.destroy();
            searchAccessor.destroy();
        }
        return out;
    }

    @Override
    public void close() throws HyracksDataException {
        try {
            accessor.destroy();
            lsmBtree.deactivate();
            lsmBtree.destroy();
        } finally {
            try {
                columnBufferPool.close();
                diskBufferCache.close();
            } finally {
                harness.tearDown();
            }
        }
    }

    private static void writeTaggedInt64(DataOutput out, long value) throws HyracksDataException {
        try {
            out.writeByte(ATypeTag.SERIALIZED_INT64_TYPE_TAG);
            out.writeLong(value);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }
}
