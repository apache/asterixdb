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
package org.apache.asterix.column.values.reader.filter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.column.metadata.schema.ObjectSchemaNode;
import org.apache.asterix.column.metadata.schema.primitive.PrimitiveSchemaNode;
import org.apache.asterix.column.metadata.schema.visitor.PathExtractorVisitor;
import org.apache.asterix.column.metadata.schema.visitor.SchemaClipperVisitor;
import org.apache.asterix.column.values.reader.filter.value.ColumnFilterValueAccessor;
import org.apache.asterix.column.values.reader.filter.value.NoOpColumnFilterValueAccessor;
import org.apache.asterix.column.values.writer.filters.AbstractColumnFilterWriter;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class FilterAccessorProvider {
    private final ObjectSchemaNode root;
    private final ObjectSchemaNode metaRoot;
    private final SchemaClipperVisitor clipperVisitor;
    private final PathExtractorVisitor pathExtractorVisitor;
    private final Map<ARecordType, PrimitiveSchemaNode> cachedNodes;
    private final List<IColumnFilterValueAccessor> filterAccessors;

    public FilterAccessorProvider(ObjectSchemaNode root, SchemaClipperVisitor clipperVisitor) {
        this(root, null, clipperVisitor);
    }

    public FilterAccessorProvider(ObjectSchemaNode root, ObjectSchemaNode metaRoot,
            SchemaClipperVisitor clipperVisitor) {
        this.root = root;
        this.metaRoot = metaRoot;
        this.clipperVisitor = clipperVisitor;
        this.pathExtractorVisitor = new PathExtractorVisitor();
        cachedNodes = new HashMap<>();
        filterAccessors = new ArrayList<>();
    }

    public IColumnFilterValueAccessor createColumnFilterValueAccessor(ARecordType path, boolean min)
            throws HyracksDataException {
        PrimitiveSchemaNode node = getNode(path);
        ATypeTag typeTag = node.getTypeTag();
        if (typeTag == ATypeTag.MISSING) {
            return NoOpColumnFilterValueAccessor.INSTANCE;
        }
        IColumnFilterValueAccessor accessor = new ColumnFilterValueAccessor(node.getColumnIndex(), typeTag, min);
        filterAccessors.add(accessor);
        return accessor;
    }

    public List<IColumnFilterValueAccessor> getFilterAccessors() {
        return filterAccessors;
    }

    public static void setFilterValues(List<IColumnFilterValueAccessor> filterValueAccessors, ByteBuffer pageZero,
            int numberOfColumns) {
        for (int i = 0; i < filterValueAccessors.size(); i++) {
            ColumnFilterValueAccessor accessor = (ColumnFilterValueAccessor) filterValueAccessors.get(i);
            int columnIndex = accessor.getColumnIndex();
            long normalizedValue;
            if (columnIndex < numberOfColumns) {
                int filterOffset = pageZero.position() + columnIndex * AbstractColumnFilterWriter.FILTER_SIZE;
                normalizedValue =
                        accessor.isMin() ? pageZero.getLong(filterOffset) : pageZero.getLong(filterOffset + Long.BYTES);
            } else {
                // Column is missing
                normalizedValue = accessor.isMin() ? Long.MAX_VALUE : Long.MIN_VALUE;
            }
            accessor.setNormalizedValue(normalizedValue);
        }
    }

    private PrimitiveSchemaNode getNode(ARecordType path) throws HyracksDataException {
        PrimitiveSchemaNode node = cachedNodes.get(path);
        if (node == null) {
            ObjectSchemaNode dataPath = (ObjectSchemaNode) path.accept(clipperVisitor, root);
            node = (PrimitiveSchemaNode) dataPath.accept(pathExtractorVisitor, null);
            if (node.getTypeTag() == ATypeTag.MISSING && metaRoot != null) {
                //Try meta
                ObjectSchemaNode metaPath = (ObjectSchemaNode) path.accept(clipperVisitor, metaRoot);
                node = (PrimitiveSchemaNode) metaPath.accept(pathExtractorVisitor, null);
            }
            cachedNodes.put(path, node);
        }
        return node;
    }
}
