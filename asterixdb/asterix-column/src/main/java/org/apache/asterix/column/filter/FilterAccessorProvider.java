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
package org.apache.asterix.column.filter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.column.assembler.value.IValueGetter;
import org.apache.asterix.column.assembler.value.IValueGetterFactory;
import org.apache.asterix.column.filter.iterable.accessor.ColumnFilterValueAccessorEvaluator;
import org.apache.asterix.column.filter.iterable.accessor.MissingEvaluator;
import org.apache.asterix.column.filter.iterable.accessor.UnionColumnFilterValueAccessorEvaluator;
import org.apache.asterix.column.filter.range.IColumnRangeFilterValueAccessor;
import org.apache.asterix.column.filter.range.accessor.ColumnRangeFilterValueAccessor;
import org.apache.asterix.column.filter.range.accessor.MissingColumnRangeFilterValueAccessor;
import org.apache.asterix.column.filter.range.accessor.NoOpColumnRangeFilterValueAccessor;
import org.apache.asterix.column.metadata.schema.AbstractSchemaNode;
import org.apache.asterix.column.metadata.schema.ObjectSchemaNode;
import org.apache.asterix.column.metadata.schema.primitive.MissingFieldSchemaNode;
import org.apache.asterix.column.metadata.schema.primitive.PrimitiveSchemaNode;
import org.apache.asterix.column.metadata.schema.visitor.PathExtractorVisitor;
import org.apache.asterix.column.metadata.schema.visitor.SchemaClipperVisitor;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.asterix.column.values.IColumnValuesReaderFactory;
import org.apache.asterix.column.values.writer.filters.AbstractColumnFilterWriter;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class FilterAccessorProvider {
    private final ObjectSchemaNode root;
    private final ObjectSchemaNode metaRoot;
    private final SchemaClipperVisitor clipperVisitor;
    private final PathExtractorVisitor pathExtractorVisitor;
    private final Map<ARecordType, PrimitiveSchemaNode> cachedNodes;
    private final List<IColumnRangeFilterValueAccessor> filterAccessors;
    private final List<IColumnValuesReader> filterColumnReaders;
    private final IValueGetterFactory valueGetterFactory;

    public FilterAccessorProvider(ObjectSchemaNode root, SchemaClipperVisitor clipperVisitor,
            IColumnValuesReaderFactory readerFactory, IValueGetterFactory valueGetterFactory) {
        this(root, null, clipperVisitor, readerFactory, valueGetterFactory);
    }

    public FilterAccessorProvider(ObjectSchemaNode root, ObjectSchemaNode metaRoot, SchemaClipperVisitor clipperVisitor,
            IColumnValuesReaderFactory readerFactory, IValueGetterFactory valueGetterFactory) {
        this.root = root;
        this.metaRoot = metaRoot;
        this.clipperVisitor = clipperVisitor;
        this.valueGetterFactory = valueGetterFactory;
        pathExtractorVisitor = new PathExtractorVisitor(readerFactory);
        cachedNodes = new HashMap<>();
        filterAccessors = new ArrayList<>();
        filterColumnReaders = new ArrayList<>();
    }

    public void reset() {
        cachedNodes.clear();
    }

    public IColumnRangeFilterValueAccessor createRangeFilterValueAccessor(ARecordType path, boolean min)
            throws HyracksDataException {
        PrimitiveSchemaNode node = cachedNodes.get(path);
        if (node == null) {
            AbstractSchemaNode pathNode = getNode(path);
            node = (PrimitiveSchemaNode) (pathNode.isNested() ? MissingFieldSchemaNode.INSTANCE : pathNode);
            cachedNodes.put(path, node);
        }

        if (node.isPrimaryKey()) {
            return NoOpColumnRangeFilterValueAccessor.INSTANCE;
        }

        ATypeTag typeTag = node.getTypeTag();
        if (typeTag == ATypeTag.MISSING) {
            return MissingColumnRangeFilterValueAccessor.INSTANCE;
        }
        IColumnRangeFilterValueAccessor accessor =
                new ColumnRangeFilterValueAccessor(node.getColumnIndex(), typeTag, min);
        filterAccessors.add(accessor);
        return accessor;
    }

    public IScalarEvaluator createColumnAccessEvaluator(ARecordType path) throws HyracksDataException {
        List<IColumnValuesReader> readers = createReaders(path);
        if (readers.isEmpty()) {
            return MissingEvaluator.INSTANCE;
        } else if (readers.size() == 1) {
            IColumnValuesReader reader = readers.get(0);
            IValueGetter valueGetter = valueGetterFactory.createValueGetter(reader.getTypeTag());
            return new ColumnFilterValueAccessorEvaluator(reader, valueGetter);
        }
        // Union readers
        IColumnValuesReader[] unionReaders = new IColumnValuesReader[readers.size()];
        IValueGetter[] valueGetters = new IValueGetter[readers.size()];
        for (int i = 0; i < readers.size(); i++) {
            IColumnValuesReader reader = readers.get(i);
            unionReaders[i] = reader;
            valueGetters[i] = valueGetterFactory.createValueGetter(reader.getTypeTag());
        }
        return new UnionColumnFilterValueAccessorEvaluator(unionReaders, valueGetters);
    }

    public List<IColumnRangeFilterValueAccessor> getFilterAccessors() {
        return filterAccessors;
    }

    public List<IColumnValuesReader> getFilterColumnReaders() {
        return filterColumnReaders;
    }

    public static void setFilterValues(List<IColumnRangeFilterValueAccessor> filterValueAccessors, ByteBuffer pageZero,
            int numberOfColumns) {
        for (int i = 0; i < filterValueAccessors.size(); i++) {
            ColumnRangeFilterValueAccessor accessor = (ColumnRangeFilterValueAccessor) filterValueAccessors.get(i);
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

    private AbstractSchemaNode getNode(ARecordType path) throws HyracksDataException {
        ObjectSchemaNode dataPath = (ObjectSchemaNode) path.accept(clipperVisitor, root);
        AbstractSchemaNode node = dataPath.accept(pathExtractorVisitor, null);
        if (node.getTypeTag() == ATypeTag.MISSING && metaRoot != null) {
            //Try meta
            ObjectSchemaNode metaPath = (ObjectSchemaNode) path.accept(clipperVisitor, metaRoot);
            node = metaPath.accept(pathExtractorVisitor, null);
        }

        return node;
    }

    private List<IColumnValuesReader> createReaders(ARecordType path) throws HyracksDataException {
        List<IColumnValuesReader> readers = Collections.emptyList();
        if (!cachedNodes.containsKey(path)) {
            ObjectSchemaNode dataPath = (ObjectSchemaNode) path.accept(clipperVisitor, root);
            readers = pathExtractorVisitor.getOrCreateReaders(dataPath, filterColumnReaders);
            if (readers.isEmpty() && metaRoot != null) {
                //Try meta
                ObjectSchemaNode metaPath = (ObjectSchemaNode) path.accept(clipperVisitor, metaRoot);
                readers = pathExtractorVisitor.getOrCreateReaders(metaPath, filterColumnReaders);
            }

            if (readers.isEmpty()) {
                cachedNodes.put(path, (PrimitiveSchemaNode) MissingFieldSchemaNode.INSTANCE);
            }
        }

        return readers;
    }
}
