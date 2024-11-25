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

import org.apache.asterix.column.metadata.schema.AbstractSchemaNestedNode;
import org.apache.asterix.column.metadata.schema.AbstractSchemaNode;
import org.apache.asterix.column.metadata.schema.ObjectSchemaNode;
import org.apache.asterix.column.metadata.schema.UnionSchemaNode;
import org.apache.asterix.column.metadata.schema.collection.AbstractCollectionSchemaNode;
import org.apache.asterix.om.lazy.AbstractLazyVisitablePointable;
import org.apache.asterix.om.lazy.AbstractListLazyVisitablePointable;
import org.apache.asterix.om.lazy.FlatLazyVisitablePointable;
import org.apache.asterix.om.lazy.ILazyVisitablePointableVisitor;
import org.apache.asterix.om.lazy.RecordLazyVisitablePointable;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;

public class NoWriteColumnTransformer
        implements ILazyVisitablePointableVisitor<AbstractSchemaNode, AbstractSchemaNode> {

    private final NoWriteFlushColumnMetadata columnMetadata;
    private final ObjectSchemaNode root;
    private AbstractSchemaNestedNode currentParent;

    public NoWriteColumnTransformer(NoWriteFlushColumnMetadata columnMetadata, ObjectSchemaNode root) {
        this.columnMetadata = columnMetadata;
        this.root = root;
    }

    /**
     * Transform a tuple in row format into columns
     *
     * @param pointable record pointable
     * @return the estimated size (possibly overestimated) of the primary key(s) columns
     */
    public int transform(RecordLazyVisitablePointable pointable) throws HyracksDataException {
        pointable.accept(this, root);
        return 0;
    }

    @Override
    public AbstractSchemaNode visit(RecordLazyVisitablePointable pointable, AbstractSchemaNode arg)
            throws HyracksDataException {
        AbstractSchemaNestedNode previousParent = currentParent;

        ObjectSchemaNode objectNode = (ObjectSchemaNode) arg;
        currentParent = objectNode;
        for (int i = 0; i < pointable.getNumberOfChildren(); i++) {
            pointable.nextChild();
            IValueReference fieldName = pointable.getFieldName();
            ATypeTag childTypeTag = pointable.getChildTypeTag();
            if (childTypeTag != ATypeTag.MISSING) {
                AbstractSchemaNode childNode = objectNode.getOrCreateChild(fieldName, childTypeTag, columnMetadata);
                acceptActualNode(pointable.getChildVisitablePointable(), childNode);
            }
        }

        if (pointable.getNumberOfChildren() == 0) {
            // Set as empty object
            objectNode.setEmptyObject(columnMetadata);
        }

        currentParent = previousParent;
        return null;
    }

    @Override
    public AbstractSchemaNode visit(AbstractListLazyVisitablePointable pointable, AbstractSchemaNode arg)
            throws HyracksDataException {
        AbstractSchemaNestedNode previousParent = currentParent;

        AbstractCollectionSchemaNode collectionNode = (AbstractCollectionSchemaNode) arg;
        currentParent = collectionNode;

        int numberOfChildren = pointable.getNumberOfChildren();
        for (int i = 0; i < numberOfChildren; i++) {
            pointable.nextChild();
            ATypeTag childTypeTag = pointable.getChildTypeTag();
            AbstractSchemaNode childNode = collectionNode.getOrCreateItem(childTypeTag, columnMetadata);
            acceptActualNode(pointable.getChildVisitablePointable(), childNode);
        }

        // Add missing as a last element of the array to help indicate empty arrays
        collectionNode.getOrCreateItem(ATypeTag.MISSING, columnMetadata);
        currentParent = previousParent;
        return null;
    }

    @Override
    public AbstractSchemaNode visit(FlatLazyVisitablePointable pointable, AbstractSchemaNode arg)
            throws HyracksDataException {
        return null;
    }

    private void acceptActualNode(AbstractLazyVisitablePointable pointable, AbstractSchemaNode node)
            throws HyracksDataException {
        if (node.getTypeTag() == ATypeTag.UNION) {
            AbstractSchemaNestedNode previousParent = currentParent;

            UnionSchemaNode unionNode = (UnionSchemaNode) node;
            currentParent = unionNode;

            ATypeTag childTypeTag = pointable.getTypeTag();

            if (childTypeTag == ATypeTag.NULL || childTypeTag == ATypeTag.MISSING) {
                /*
                 * NULL and MISSING are tracked since the start to be written in the originalType (i.e., the type
                 * before injecting a union between the parent and the original node).
                 */
                AbstractSchemaNode actualNode = unionNode.getOriginalType();
                acceptActualNode(pointable, actualNode);
            } else {
                AbstractSchemaNode actualNode = unionNode.getOrCreateChild(pointable.getTypeTag(), columnMetadata);
                pointable.accept(this, actualNode);
            }

            currentParent = previousParent;
        } else if (pointable.getTypeTag() == ATypeTag.NULL && node.isNested()) {
            columnMetadata.addNestedNull(currentParent, (AbstractSchemaNestedNode) node);
        } else {
            pointable.accept(this, node);
        }
    }
}