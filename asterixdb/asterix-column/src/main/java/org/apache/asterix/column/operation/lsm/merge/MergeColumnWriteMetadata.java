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
package org.apache.asterix.column.operation.lsm.merge;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.column.metadata.AbstractColumnImmutableMetadata;
import org.apache.asterix.column.metadata.PromotedKeys;
import org.apache.asterix.column.metadata.PromotionBoundaries;
import org.apache.asterix.column.metadata.PromotionBoundaries.Boundary;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.asterix.column.values.IColumnValuesWriter;
import org.apache.asterix.column.values.IColumnValuesWriterFactory;
import org.apache.asterix.column.values.writer.ColumnValuesWriterFactory;
import org.apache.asterix.om.types.ARecordType;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnTupleIterator;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

/**
 * Merge column write metadata belongs to write a new merge {@link ILSMDiskComponent}
 * This is for writing a new on-disk component by merging two or more on disk components. The final schema for this
 * component will the most recent schema, which belongs to the newest merged component. The schema here is immutable
 * and cannot be changed.
 */
public final class MergeColumnWriteMetadata extends AbstractColumnImmutableMetadata {
    private final Mutable<IColumnWriteMultiPageOp> multiPageOpRef;
    private final List<IColumnValuesWriter> columnWriters;
    private final int requiredTemporaryBuffersCount;
    private final List<IColumnTupleIterator> componentsTuples;
    private final List<PromotedKeys.Node> collapsedNodes;
    private final List<PromotedKeys.Key> movableKeys;
    /** Each input's promotion boundaries; null for an input without them. */
    private final List<Int2ObjectMap<Boundary>> inputsBoundaries;
    /** Movable keys the merge could not fully move into their columns. */
    private final boolean[] leftInMap;

    /**
     * For LSM Merge
     */
    private MergeColumnWriteMetadata(ARecordType datasetType, ARecordType metaType, int numberOfPrimaryKeys,
            Mutable<IColumnWriteMultiPageOp> multiPageOpRef, List<IColumnValuesWriter> columnWriters,
            int requiredTemporaryBuffersCount, IValueReference serializedMetadata,
            List<Int2ObjectMap<Boundary>> inputsBoundaries, List<IColumnTupleIterator> componentsTuples)
            throws HyracksDataException {
        super(datasetType, metaType, numberOfPrimaryKeys, serializedMetadata, columnWriters.size());
        this.multiPageOpRef = multiPageOpRef;
        this.columnWriters = columnWriters;
        this.requiredTemporaryBuffersCount = requiredTemporaryBuffersCount;
        this.componentsTuples = componentsTuples;
        this.inputsBoundaries = inputsBoundaries;
        collapsedNodes = PromotedKeys.of(serializedMetadata);
        movableKeys = PromotedKeys.movable(collapsedNodes);
        leftInMap = new boolean[movableKeys.size()];
    }

    /** The promoted keys this merge moves out of its inputs' maps. */
    public List<PromotedKeys.Key> getMovableKeys() {
        return movableKeys;
    }

    /**
     * Whether the merge must fill {@code key}'s column record by record for input {@code componentIndex}: its column
     * there may lack values or levels. True also for an input that predates the node's map: it has no values for the
     * key, but the merged column still needs its levels.
     */
    public boolean mayHoldInMap(int componentIndex, PromotedKeys.Key key) {
        Int2ObjectMap<Boundary> boundaries = inputsBoundaries.get(componentIndex);
        Boundary boundary = boundaries == null ? null : boundaries.get(key.node.mapKeyColumn);
        return boundary == null || !boundary.holds(key.childIndex);
    }

    /**
     * Marks movable key {@code i} as not fully in its merged column: a value of another type stayed in a map, or a
     * record's level could not be determined.
     */
    public void leftInMap(int i) {
        leftInMap[i] = true;
    }

    /**
     * The merged component's boundaries: per node, the limit is the first promoted key that may still be in a map
     * (not movable, or not fully moved); otherwise all children. Called after every column is written.
     */
    @Override
    public IValueReference serializeColumnsAnnotations() throws HyracksDataException {
        Int2ObjectMap<Boundary> boundaries = new Int2ObjectOpenHashMap<>();
        for (PromotedKeys.Node node : collapsedNodes) {
            int limit = node.numberOfChildren;
            for (PromotedKeys.Key key : node.keys) {
                if (!moved(key) && heldInAnyMap(key)) {
                    limit = key.childIndex;
                    break;
                }
            }
            boundaries.put(node.mapKeyColumn, new Boundary(limit));
        }
        return PromotionBoundaries.serialize(boundaries);
    }

    private boolean moved(PromotedKeys.Key key) {
        int i = movableKeys.indexOf(key);
        return i >= 0 && !leftInMap[i];
    }

    private boolean heldInAnyMap(PromotedKeys.Key key) {
        for (int i = 0; i < inputsBoundaries.size(); i++) {
            if (mayHoldInMap(i, key)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Set {@link IColumnWriteMultiPageOp} for {@link IColumnValuesWriter}
     *
     * @param multiPageOp multi-buffer allocator
     */
    public void init(IColumnWriteMultiPageOp multiPageOp) throws HyracksDataException {
        multiPageOpRef.setValue(multiPageOp);

        //Reset writer for the first write
        for (int i = 0; i < columnWriters.size(); i++) {
            columnWriters.get(i).reset();
        }
    }

    public Mutable<IColumnWriteMultiPageOp> getMultiPageOpRef() {
        return multiPageOpRef;
    }

    public IColumnValuesWriter getWriter(int columnIndex) {
        return columnWriters.get(columnIndex);
    }

    public void close() {
        for (int i = 0; i < columnWriters.size(); i++) {
            columnWriters.get(i).close();
        }
        multiPageOpRef.setValue(null);
    }

    public static MergeColumnWriteMetadata create(ARecordType datasetType, ARecordType metaType,
            int numberOfPrimaryKeys, Mutable<IColumnWriteMultiPageOp> multiPageOpRef,
            IValueReference serializedMetadata, List<IValueReference> inputsAnnotations,
            List<IColumnTupleIterator> componentsTuples) throws IOException {
        byte[] bytes = serializedMetadata.getByteArray();
        int offset = serializedMetadata.getStartOffset();
        int length = serializedMetadata.getLength();

        int writersOffset = offset + IntegerPointable.getInteger(bytes, offset + WRITERS_POINTER);
        DataInput input = new DataInputStream(new ByteArrayInputStream(bytes, writersOffset, length));

        IColumnValuesWriterFactory writerFactory = new ColumnValuesWriterFactory(multiPageOpRef);
        List<IColumnValuesWriter> writers = new ArrayList<>();
        int requiredTemporaryBuffers = FlushColumnMetadata.deserializeWriters(input, writers, writerFactory);

        return new MergeColumnWriteMetadata(datasetType, metaType, numberOfPrimaryKeys, multiPageOpRef, writers,
                requiredTemporaryBuffers, serializedMetadata, readBoundaries(inputsAnnotations), componentsTuples);
    }

    private static List<Int2ObjectMap<Boundary>> readBoundaries(List<IValueReference> inputsAnnotations)
            throws HyracksDataException {
        List<Int2ObjectMap<Boundary>> boundaries = new ArrayList<>(inputsAnnotations.size());
        for (IValueReference annotations : inputsAnnotations) {
            boundaries.add(PromotionBoundaries.read(annotations));
        }
        return boundaries;
    }

    public int getRequiredTemporaryBuffersCount() {
        return requiredTemporaryBuffersCount;
    }

    public List<IColumnTupleIterator> getComponentsTuples() {
        return componentsTuples;
    }
}
