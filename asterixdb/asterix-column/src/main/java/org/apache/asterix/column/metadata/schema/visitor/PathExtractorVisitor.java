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
package org.apache.asterix.column.metadata.schema.visitor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.column.metadata.schema.AbstractSchemaNode;
import org.apache.asterix.column.metadata.schema.ISchemaNodeVisitor;
import org.apache.asterix.column.metadata.schema.ObjectSchemaNode;
import org.apache.asterix.column.metadata.schema.UnionSchemaNode;
import org.apache.asterix.column.metadata.schema.collection.AbstractCollectionSchemaNode;
import org.apache.asterix.column.metadata.schema.primitive.MissingFieldSchemaNode;
import org.apache.asterix.column.metadata.schema.primitive.PrimitiveSchemaNode;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.asterix.column.values.IColumnValuesReaderFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

public class PathExtractorVisitor implements ISchemaNodeVisitor<AbstractSchemaNode, Void> {
    private final IColumnValuesReaderFactory readerFactory;
    private final IntList delimiters;
    private final Int2ObjectMap<IColumnValuesReader> cachedReaders;
    private int level;

    public PathExtractorVisitor(IColumnValuesReaderFactory readerFactory) {
        this.readerFactory = readerFactory;
        cachedReaders = new Int2ObjectOpenHashMap<>();
        delimiters = new IntArrayList();
    }

    public List<IColumnValuesReader> getOrCreateReaders(ObjectSchemaNode path, List<IColumnValuesReader> readers)
            throws HyracksDataException {
        level = 0;
        delimiters.clear();
        AbstractSchemaNode node = path.accept(this, null);
        ATypeTag typeTag = node.getTypeTag();
        if (typeTag == ATypeTag.MISSING) {
            return Collections.emptyList();
        } else if (typeTag == ATypeTag.UNION) {
            UnionSchemaNode unionNode = (UnionSchemaNode) node;
            Collection<AbstractSchemaNode> children = unionNode.getChildren().values();
            List<IColumnValuesReader> unionReaders = new ArrayList<>();
            for (AbstractSchemaNode child : children) {
                if (child.isNested()) {
                    // ignore nested nodes as we only compare flat types
                    continue;
                }
                IColumnValuesReader reader = getOrCreate(child, readers);
                unionReaders.add(reader);
            }
            return unionReaders;
        }
        return Collections.singletonList(getOrCreate(node, readers));
    }

    @Override
    public AbstractSchemaNode visit(ObjectSchemaNode objectNode, Void arg) throws HyracksDataException {
        IntList fieldNameIndexes = objectNode.getChildrenFieldNameIndexes();
        int fieldNameIndex = fieldNameIndexes.isEmpty() ? -1 : objectNode.getChildrenFieldNameIndexes().getInt(0);
        if (fieldNameIndex < 0) {
            return MissingFieldSchemaNode.INSTANCE;
        }
        level++;
        return objectNode.getChild(fieldNameIndex).accept(this, null);
    }

    @Override
    public AbstractSchemaNode visit(AbstractCollectionSchemaNode collectionNode, Void arg) throws HyracksDataException {
        AbstractSchemaNode itemNode = collectionNode.getItemNode();
        if (itemNode == null) {
            return MissingFieldSchemaNode.INSTANCE;
        }
        delimiters.add(level - 1);
        level++;
        return collectionNode.getItemNode().accept(this, null);
    }

    @Override
    public AbstractSchemaNode visit(UnionSchemaNode unionNode, Void arg) throws HyracksDataException {
        Collection<AbstractSchemaNode> children = unionNode.getChildren().values();
        if (children.size() == 1) {
            // A specific type was requested. Get the requested type from union
            for (AbstractSchemaNode node : children) {
                return node.accept(this, null);
            }
        }

        // Multiple types were requested, return the union
        return unionNode;
    }

    @Override
    public AbstractSchemaNode visit(PrimitiveSchemaNode primitiveNode, Void arg) throws HyracksDataException {
        //Missing column index is -1
        return primitiveNode;
    }

    private IColumnValuesReader getOrCreate(AbstractSchemaNode node, List<IColumnValuesReader> readers) {
        PrimitiveSchemaNode primitiveNode = (PrimitiveSchemaNode) node;
        int columnIndex = primitiveNode.getColumnIndex();
        return cachedReaders.computeIfAbsent(columnIndex, k -> createReader(primitiveNode, readers));
    }

    private IColumnValuesReader createReader(PrimitiveSchemaNode primitiveNode, List<IColumnValuesReader> readers) {
        IColumnValuesReader reader;
        if (delimiters.isEmpty()) {
            reader = readerFactory.createValueReader(primitiveNode.getTypeTag(), primitiveNode.getColumnIndex(), level,
                    primitiveNode.isPrimaryKey());
        } else {
            // array
            reader = readerFactory.createValueReader(primitiveNode.getTypeTag(), primitiveNode.getColumnIndex(), level,
                    getReversedDelimiters());
        }
        readers.add(reader);
        return reader;
    }

    private int[] getReversedDelimiters() {
        Collections.reverse(delimiters);
        return delimiters.toIntArray();
    }
}
