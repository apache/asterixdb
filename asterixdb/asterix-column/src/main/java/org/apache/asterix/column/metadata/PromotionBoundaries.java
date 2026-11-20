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
package org.apache.asterix.column.metadata;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.column.metadata.schema.AbstractSchemaNode;
import org.apache.asterix.column.metadata.schema.ObjectSchemaNode;
import org.apache.asterix.column.metadata.schema.UnionSchemaNode;
import org.apache.asterix.column.metadata.schema.collection.AbstractCollectionSchemaNode;
import org.apache.asterix.column.metadata.schema.collection.MapSchemaNode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.storage.am.lsm.btree.column.utils.ColumnUtil;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.LSMComponentType;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

/**
 * Per component: which promoted keys have all their values in their own column, so readers can skip the map.
 * <p>
 * After a promotion, older components still hold the key in the map. For each collapsed node, a component stores a
 * {@link Boundary#limit}: promoted children below it are only in their columns; children at or above it may also be
 * in the map. One limit per node (not per key) keeps this simple; promotions are rare.
 * <p>
 * The schema can't carry this (a merge writes one schema for all its inputs), so it is stored under a separate
 * component-metadata key that older binaries ignore. A node is identified by its map's key column, which is the same
 * in every component. A component without the entry is treated as "every promoted key may be in the map".
 */
public final class PromotionBoundaries {
    /** Boundaries read lazily: only a query on a promoted key needs them. */
    @FunctionalInterface
    public interface Source {
        /** @return the boundaries; null when unknown */
        Int2ObjectMap<Boundary> get() throws HyracksDataException;
    }

    /**
     * One node's limit in one component. Promoted children below {@code limit} are only in their columns. Children at
     * or above it were promoted during or after the flush, or a merge could not move all of their (or an earlier
     * key's) values out of the map.
     */
    public record Boundary(int limit) {
        /** Whether promoted child {@code childIndex} holds every value of its key in its column. */
        public boolean holds(int childIndex) {
            return childIndex < limit;
        }

        /** Limit for two components read together: the lower one. */
        static Boundary lowest(Boundary a, Boundary b) {
            return a.limit <= b.limit ? a : b;
        }
    }

    private PromotionBoundaries() {
    }

    /** Boundaries for the component written by flush {@code flushNumber}. */
    public static IValueReference collect(int flushNumber, ObjectSchemaNode... roots) throws HyracksDataException {
        Int2ObjectMap<Boundary> boundaries = new Int2ObjectOpenHashMap<>();
        for (ObjectSchemaNode root : roots) {
            if (root != null) {
                collect(root, flushNumber, boundaries);
            }
        }
        return serialize(boundaries);
    }

    /**
     * Boundaries for {@code inputs} read together: the lowest limit per node. An input without a node had no map
     * there, so it doesn't lower the limit. If any input has no boundaries at all, the result has none.
     */
    public static IValueReference merge(List<IValueReference> inputs) throws HyracksDataException {
        Int2ObjectMap<Boundary> merged = new Int2ObjectOpenHashMap<>();
        for (IValueReference input : inputs) {
            if (input == null) {
                return null;
            }
            for (Int2ObjectMap.Entry<Boundary> entry : read(input).int2ObjectEntrySet()) {
                merged.merge(entry.getIntKey(), entry.getValue(), Boundary::lowest);
            }
        }
        return serialize(merged);
    }

    /**
     * Boundaries for all disk components a search reads; null if any lacks them. In-memory components are skipped:
     * they hold rows, not columns.
     */
    public static Int2ObjectMap<Boundary> ofComponents(List<ILSMComponent> components) throws HyracksDataException {
        List<IValueReference> annotations = new ArrayList<>(components.size());
        for (ILSMComponent component : components) {
            if (component.getType() == LSMComponentType.DISK) {
                annotations.add(ColumnUtil.getColumnsAnnotationsCopy(component.getMetadata()));
            }
        }
        return read(merge(annotations));
    }

    /** Limits by map key column; null input (no entry) gives null. */
    public static Int2ObjectMap<Boundary> read(IValueReference boundaries) throws HyracksDataException {
        if (boundaries == null) {
            return null;
        }
        DataInput input = new DataInputStream(new ByteArrayInputStream(boundaries.getByteArray(),
                boundaries.getStartOffset(), boundaries.getLength()));
        try {
            int count = input.readInt();
            Int2ObjectMap<Boundary> map = new Int2ObjectOpenHashMap<>(count);
            for (int i = 0; i < count; i++) {
                int mapKeyColumn = input.readInt();
                map.put(mapKeyColumn, new Boundary(input.readInt()));
            }
            return map;
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    private static void collect(AbstractSchemaNode node, int flushNumber, Int2ObjectMap<Boundary> boundaries) {
        if (node instanceof ObjectSchemaNode object) {
            MapSchemaNode map = object.getMapNode();
            if (map != null) {
                // A flush moves nothing out of a map: keys promoted during the flush stay at or above the limit.
                boundaries.put(map.getKeyColumnIndex(), new Boundary(object.getPromotionBoundary(flushNumber)));
            }
            for (AbstractSchemaNode child : object.getChildren()) {
                collect(child, flushNumber, boundaries);
            }
        } else if (node instanceof AbstractCollectionSchemaNode collection) {
            // Also visits collapsed nodes inside a map's values.
            if (collection.getItemNode() != null) {
                collect(collection.getItemNode(), flushNumber, boundaries);
            }
        } else if (node instanceof UnionSchemaNode union) {
            for (AbstractSchemaNode child : union.getChildren().values()) {
                collect(child, flushNumber, boundaries);
            }
        }
    }

    /** Writes boundaries in the format {@link #read} expects. */
    public static IValueReference serialize(Int2ObjectMap<Boundary> boundaries) throws HyracksDataException {
        ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
        DataOutput output = storage.getDataOutput();
        try {
            output.writeInt(boundaries.size());
            for (Int2ObjectMap.Entry<Boundary> entry : boundaries.int2ObjectEntrySet()) {
                output.writeInt(entry.getIntKey());
                output.writeInt(entry.getValue().limit());
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
        return storage;
    }
}
