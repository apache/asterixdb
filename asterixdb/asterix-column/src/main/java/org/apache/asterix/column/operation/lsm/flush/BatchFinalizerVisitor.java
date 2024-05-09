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

public final class BatchFinalizerVisitor implements ISchemaNodeVisitor<Void, AbstractSchemaNestedNode> {
    private final FlushColumnMetadata columnSchemaMetadata;
    private final IColumnValuesWriter[] primaryKeyWriters;
    private final PriorityQueue<IColumnValuesWriter> orderedColumns;
    private int level;

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

    public int finalizeBatch(IColumnBatchWriter batchWriter, FlushColumnMetadata columnMetadata)
            throws HyracksDataException {
        orderedColumns.clear();

        columnMetadata.getRoot().accept(this, null);
        if (columnMetadata.getMetaRoot() != null) {
            columnMetadata.getMetaRoot().accept(this, null);
        }

        batchWriter.writePrimaryKeyColumns(primaryKeyWriters);
        return batchWriter.writeColumns(orderedColumns);
    }

    @Override
    public Void visit(ObjectSchemaNode objectNode, AbstractSchemaNestedNode arg) throws HyracksDataException {
        level++;
        columnSchemaMetadata.flushDefinitionLevels(level, arg, objectNode);
        List<AbstractSchemaNode> children = objectNode.getChildren();
        for (int i = 0; i < children.size(); i++) {
            children.get(i).accept(this, objectNode);
        }
        objectNode.setCounter(0);
        columnSchemaMetadata.clearDefinitionLevels(objectNode);
        level--;
        return null;
    }

    @Override
    public Void visit(AbstractCollectionSchemaNode collectionNode, AbstractSchemaNestedNode arg)
            throws HyracksDataException {
        level++;
        columnSchemaMetadata.flushDefinitionLevels(level, arg, collectionNode);
        collectionNode.getItemNode().accept(this, collectionNode);
        collectionNode.setCounter(0);
        columnSchemaMetadata.clearDefinitionLevels(collectionNode);
        level--;
        return null;
    }

    @Override
    public Void visit(UnionSchemaNode unionNode, AbstractSchemaNestedNode arg) throws HyracksDataException {
        columnSchemaMetadata.flushDefinitionLevels(level, arg, unionNode);
        for (AbstractSchemaNode node : unionNode.getChildren().values()) {
            node.accept(this, unionNode);
        }
        unionNode.setCounter(0);
        columnSchemaMetadata.clearDefinitionLevels(unionNode);
        return null;
    }

    @Override
    public Void visit(PrimitiveSchemaNode primitiveNode, AbstractSchemaNestedNode arg) throws HyracksDataException {
        columnSchemaMetadata.flushDefinitionLevels(level, arg, primitiveNode);
        if (!primitiveNode.isPrimaryKey()) {
            orderedColumns.add(columnSchemaMetadata.getWriter(primitiveNode.getColumnIndex()));
        }

        //Prepare for the next batch
        primitiveNode.setCounter(0);
        return null;
    }
}
