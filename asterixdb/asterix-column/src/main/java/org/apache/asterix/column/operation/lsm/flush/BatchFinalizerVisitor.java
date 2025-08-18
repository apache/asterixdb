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
package org.apache.asterix.column.operation.lsm.flush;

import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.asterix.column.metadata.schema.AbstractSchemaNestedNode;
import org.apache.asterix.column.metadata.schema.AbstractSchemaNode;
import org.apache.asterix.column.metadata.schema.ISchemaNodeVisitor;
import org.apache.asterix.column.metadata.schema.ObjectSchemaNode;
import org.apache.asterix.column.metadata.schema.UnionSchemaNode;
import org.apache.asterix.column.metadata.schema.collection.AbstractCollectionSchemaNode;
import org.apache.asterix.column.metadata.schema.primitive.PrimitiveSchemaNode;
import org.apache.asterix.column.values.IColumnBatchWriter;
import org.apache.asterix.column.values.IColumnValuesWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.IColumnPageZeroWriter;

public final class BatchFinalizerVisitor implements ISchemaNodeVisitor<Void, AbstractSchemaNestedNode> {
    private final FlushColumnMetadata columnSchemaMetadata;
    private final IColumnValuesWriter[] primaryKeyWriters;
    private final PriorityQueue<IColumnValuesWriter> orderedColumns;
    private BitSet presentColumnsIndex;
    private IColumnPageZeroWriter columnPageZeroWriter;
    private int level;
    private boolean needAllColumns;

    public BatchFinalizerVisitor(FlushColumnMetadata columnSchemaMetadata) {
        this.columnSchemaMetadata = columnSchemaMetadata;
        orderedColumns = new PriorityQueue<>(Comparator.comparingInt(x -> -x.getEstimatedSize()));
        int numberOfPrimaryKeys = columnSchemaMetadata.getNumberOfPrimaryKeys();
        primaryKeyWriters = new IColumnValuesWriter[numberOfPrimaryKeys];
        for (int i = 0; i < numberOfPrimaryKeys; i++) {
            primaryKeyWriters[i] = columnSchemaMetadata.getWriter(i);
        }
        level = -1;
    }

    public void finalizeBatchColumns(FlushColumnMetadata columnMetadata, BitSet presentColumnsIndexes,
            IColumnPageZeroWriter pageZeroWriter) throws HyracksDataException {
        orderedColumns.clear();
        this.presentColumnsIndex = presentColumnsIndexes;
        this.needAllColumns = false;
        this.columnPageZeroWriter = pageZeroWriter;

        // is this needed to parse the whole schema??
        columnMetadata.getRoot().accept(this, null);
        if (columnMetadata.getMetaRoot() != null) {
            columnMetadata.getMetaRoot().accept(this, null);
        }
    }

    public int finalizeBatch(IColumnBatchWriter batchWriter) throws HyracksDataException {
        batchWriter.writePrimaryKeyColumns(primaryKeyWriters);
        return batchWriter.writeColumns(orderedColumns);
    }

    @Override
    public Void visit(ObjectSchemaNode objectNode, AbstractSchemaNestedNode arg) throws HyracksDataException {
        level++;
        boolean previousNeedAllColumns = needAllColumns;
        needAllColumns = needAllColumns | objectNode.needAllColumns();
        columnSchemaMetadata.flushDefinitionLevels(level - 1, arg, objectNode);
        List<AbstractSchemaNode> children = objectNode.getChildren();
        for (int i = 0; i < children.size(); i++) {
            children.get(i).accept(this, objectNode);
        }
        objectNode.setCounter(0);
        objectNode.setNumberOfVisitedColumnsInBatch(0);
        columnSchemaMetadata.clearDefinitionLevels(objectNode);
        objectNode.needAllColumns(false);
        needAllColumns = previousNeedAllColumns;
        level--;
        return null;
    }

    @Override
    public Void visit(AbstractCollectionSchemaNode collectionNode, AbstractSchemaNestedNode arg)
            throws HyracksDataException {
        level++;
        boolean previousNeedAllColumns = needAllColumns;
        needAllColumns = needAllColumns | collectionNode.needAllColumns();
        columnSchemaMetadata.flushDefinitionLevels(level - 1, arg, collectionNode);
        collectionNode.getItemNode().accept(this, collectionNode);
        collectionNode.setCounter(0);
        collectionNode.setNumberOfVisitedColumnsInBatch(0);
        columnSchemaMetadata.clearDefinitionLevels(collectionNode);
        collectionNode.needAllColumns(false);
        needAllColumns = previousNeedAllColumns;
        level--;
        return null;
    }

    @Override
    public Void visit(UnionSchemaNode unionNode, AbstractSchemaNestedNode arg) throws HyracksDataException {
        boolean previousNeedAllColumns = needAllColumns;
        needAllColumns = needAllColumns | unionNode.needAllColumns();
        columnSchemaMetadata.flushDefinitionLevels(level, arg, unionNode);
        for (AbstractSchemaNode node : unionNode.getChildren().values()) {
            node.accept(this, unionNode);
        }
        unionNode.setCounter(0);
        unionNode.setNumberOfVisitedColumnsInBatch(0);
        columnSchemaMetadata.clearDefinitionLevels(unionNode);
        unionNode.needAllColumns(false);
        needAllColumns = previousNeedAllColumns;
        return null;
    }

    @Override
    public Void visit(PrimitiveSchemaNode primitiveNode, AbstractSchemaNestedNode arg) throws HyracksDataException {
        columnSchemaMetadata.flushDefinitionLevels(level, arg, primitiveNode);
        if (needAllColumns) {
            presentColumnsIndex.set(primitiveNode.getColumnIndex());
        }
        // in case of DefaultWriter all non-primary columns should be included
        if (!primitiveNode.isPrimaryKey() && columnPageZeroWriter.includeOrderedColumn(presentColumnsIndex,
                primitiveNode.getColumnIndex(), needAllColumns)) {
            orderedColumns.add(columnSchemaMetadata.getWriter(primitiveNode.getColumnIndex()));
        }

        //Prepare for the next batch
        primitiveNode.setCounter(0);
        primitiveNode.setNumberOfVisitedColumnsInBatch(0);
        return null;
    }
}
