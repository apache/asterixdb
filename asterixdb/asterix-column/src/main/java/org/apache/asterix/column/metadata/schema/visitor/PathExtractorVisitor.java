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

import org.apache.asterix.column.metadata.schema.AbstractSchemaNode;
import org.apache.asterix.column.metadata.schema.ISchemaNodeVisitor;
import org.apache.asterix.column.metadata.schema.ObjectSchemaNode;
import org.apache.asterix.column.metadata.schema.UnionSchemaNode;
import org.apache.asterix.column.metadata.schema.collection.AbstractCollectionSchemaNode;
import org.apache.asterix.column.metadata.schema.primitive.MissingFieldSchemaNode;
import org.apache.asterix.column.metadata.schema.primitive.PrimitiveSchemaNode;
import org.apache.hyracks.api.exceptions.HyracksDataException;

import it.unimi.dsi.fastutil.ints.IntList;

public class PathExtractorVisitor implements ISchemaNodeVisitor<AbstractSchemaNode, Void> {
    @Override
    public AbstractSchemaNode visit(ObjectSchemaNode objectNode, Void arg) throws HyracksDataException {
        IntList fieldNameIndexes = objectNode.getChildrenFieldNameIndexes();
        int fieldNameIndex = fieldNameIndexes.isEmpty() ? -1 : objectNode.getChildrenFieldNameIndexes().getInt(0);
        if (fieldNameIndex < 0) {
            return MissingFieldSchemaNode.INSTANCE;
        }
        return objectNode.getChild(fieldNameIndex).accept(this, null);
    }

    @Override
    public AbstractSchemaNode visit(AbstractCollectionSchemaNode collectionNode, Void arg) throws HyracksDataException {
        AbstractSchemaNode itemNode = collectionNode.getItemNode();
        if (itemNode == null) {
            return MissingFieldSchemaNode.INSTANCE;
        }
        return collectionNode.getItemNode().accept(this, null);
    }

    @Override
    public AbstractSchemaNode visit(UnionSchemaNode unionNode, Void arg) throws HyracksDataException {
        for (AbstractSchemaNode node : unionNode.getChildren().values()) {
            // Using 'for-loop' is the only get the child out of a collection
            return node.accept(this, null);
        }
        return MissingFieldSchemaNode.INSTANCE;
    }

    @Override
    public AbstractSchemaNode visit(PrimitiveSchemaNode primitiveNode, Void arg) throws HyracksDataException {
        //Missing column index is -1
        return primitiveNode;
    }
}
