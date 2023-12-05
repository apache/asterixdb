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
import org.apache.asterix.column.metadata.schema.primitive.PrimitiveSchemaNode;
import org.apache.asterix.column.util.RunLengthIntArray;
import org.apache.asterix.column.values.IColumnValuesWriter;
import org.apache.asterix.om.lazy.AbstractLazyVisitablePointable;
import org.apache.asterix.om.lazy.AbstractListLazyVisitablePointable;
import org.apache.asterix.om.lazy.FlatLazyVisitablePointable;
import org.apache.asterix.om.lazy.ILazyVisitablePointableVisitor;
import org.apache.asterix.om.lazy.RecordLazyVisitablePointable;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.storage.am.lsm.btree.tuples.LSMBTreeTupleReference;

public class ColumnTransformer implements ILazyVisitablePointableVisitor<AbstractSchemaNode, AbstractSchemaNode> {
    private final FlushColumnMetadata columnMetadata;
    private final VoidPointable nonTaggedValue;
    private final ObjectSchemaNode root;
    private AbstractSchemaNestedNode currentParent;
    private int primaryKeysLength;
    /**
     * Hack-alert! This tracks the total length of all strings (as they're not as encodable as numerics)
     * The total length can be used by {@link FlushColumnTupleWriter} to stop writing tuples to the current mega
     * leaf node to avoid having a single column that spans to megabytes of pages.
     */
    private int stringLengths;

    public ColumnTransformer(FlushColumnMetadata columnMetadata, ObjectSchemaNode root) {
        this.columnMetadata = columnMetadata;
        this.root = root;
        nonTaggedValue = new VoidPointable();
        stringLengths = 0;
    }

    public int getStringLengths() {
        return stringLengths;
    }

    public void resetStringLengths() {
        stringLengths = 0;
    }

    /**
     * Transform a tuple in row format into columns
     *
     * @param pointable record pointable
     * @return the estimated size (possibly overestimated) of the primary key(s) columns
     */
    public int transform(RecordLazyVisitablePointable pointable) throws HyracksDataException {
        primaryKeysLength = 0;
        pointable.accept(this, root);
        return primaryKeysLength;
    }

    public int writeAntiMatter(LSMBTreeTupleReference tuple) throws HyracksDataException {
        int pkSize = 0;
        for (int i = 0; i < columnMetadata.getNumberOfPrimaryKeys(); i++) {
            byte[] bytes = tuple.getFieldData(i);
            int start = tuple.getFieldStart(i);
            ATypeTag tag = ATypeTag.VALUE_TYPE_MAPPING[bytes[start]];
            nonTaggedValue.set(bytes, start + 1, tuple.getFieldLength(i) - 1);
            IColumnValuesWriter writer = columnMetadata.getWriter(i);
            writer.writeAntiMatter(tag, nonTaggedValue);
            pkSize += writer.getEstimatedSize();
        }
        return pkSize;
    }

    @Override
    public AbstractSchemaNode visit(RecordLazyVisitablePointable pointable, AbstractSchemaNode arg)
            throws HyracksDataException {
        columnMetadata.enterNode(currentParent, arg);
        AbstractSchemaNestedNode previousParent = currentParent;

        ObjectSchemaNode objectNode = (ObjectSchemaNode) arg;
        currentParent = objectNode;
        for (int i = 0; i < pointable.getNumberOfChildren(); i++) {
            pointable.nextChild();
            IValueReference fieldName = pointable.getFieldName();
            ATypeTag childTypeTag = pointable.getChildTypeTag();
            if (childTypeTag != ATypeTag.MISSING) {
                //Only write actual field values (including NULL) but ignore MISSING fields
                AbstractSchemaNode childNode = objectNode.getOrCreateChild(fieldName, childTypeTag, columnMetadata);
                acceptActualNode(pointable.getChildVisitablePointable(), childNode);
            }
        }

        if (pointable.getNumberOfChildren() == 0) {
            // Set as empty object
            objectNode.setEmptyObject(columnMetadata);
        }

        columnMetadata.exitNode(arg);
        currentParent = previousParent;
        return null;
    }

    @Override
    public AbstractSchemaNode visit(AbstractListLazyVisitablePointable pointable, AbstractSchemaNode arg)
            throws HyracksDataException {
        columnMetadata.enterNode(currentParent, arg);
        AbstractSchemaNestedNode previousParent = currentParent;

        AbstractCollectionSchemaNode collectionNode = (AbstractCollectionSchemaNode) arg;
        RunLengthIntArray defLevels = columnMetadata.getDefinitionLevels(collectionNode);
        //the level at which an item is missing
        int missingLevel = columnMetadata.getLevel();
        currentParent = collectionNode;

        int numberOfChildren = pointable.getNumberOfChildren();
        for (int i = 0; i < numberOfChildren; i++) {
            pointable.nextChild();
            ATypeTag childTypeTag = pointable.getChildTypeTag();
            AbstractSchemaNode childNode = collectionNode.getOrCreateItem(childTypeTag, columnMetadata);
            acceptActualNode(pointable.getChildVisitablePointable(), childNode);
            /*
             * The array item may change (e.g., BIGINT --> UNION). Thus, new items would be considered as missing
             */
            defLevels.add(missingLevel);
        }

        // Add missing as a last element of the array to help indicate empty arrays
        collectionNode.getOrCreateItem(ATypeTag.MISSING, columnMetadata);
        defLevels.add(missingLevel);

        columnMetadata.exitCollectionNode(collectionNode, numberOfChildren);
        currentParent = previousParent;
        return null;
    }

    @Override
    public AbstractSchemaNode visit(FlatLazyVisitablePointable pointable, AbstractSchemaNode arg)
            throws HyracksDataException {
        columnMetadata.enterNode(currentParent, arg);
        ATypeTag valueTypeTag = pointable.getTypeTag();
        PrimitiveSchemaNode node = (PrimitiveSchemaNode) arg;
        IColumnValuesWriter writer = columnMetadata.getWriter(node.getColumnIndex());
        if (valueTypeTag == ATypeTag.MISSING) {
            writer.writeLevel(columnMetadata.getLevel());
        } else if (valueTypeTag == ATypeTag.NULL) {
            writer.writeNull(columnMetadata.getLevel());
        } else if (pointable.isTagged()) {
            //Remove type tag
            nonTaggedValue.set(pointable.getByteArray(), pointable.getStartOffset() + 1, pointable.getLength() - 1);
            writer.writeValue(pointable.getTypeTag(), nonTaggedValue);
        } else {
            writer.writeValue(pointable.getTypeTag(), pointable);
        }
        if (node.isPrimaryKey()) {
            primaryKeysLength += writer.getEstimatedSize();
        } else if (node.getTypeTag() == ATypeTag.STRING) {
            stringLengths += pointable.getLength();
        }
        columnMetadata.exitNode(arg);
        return null;
    }

    private void acceptActualNode(AbstractLazyVisitablePointable pointable, AbstractSchemaNode node)
            throws HyracksDataException {
        if (node.getTypeTag() == ATypeTag.UNION) {
            columnMetadata.enterNode(currentParent, node);
            AbstractSchemaNestedNode previousParent = currentParent;

            UnionSchemaNode unionNode = (UnionSchemaNode) node;
            currentParent = unionNode;

            ATypeTag childTypeTag = pointable.getTypeTag();
            AbstractSchemaNode actualNode;
            if (childTypeTag == ATypeTag.NULL || childTypeTag == ATypeTag.MISSING) {
                actualNode = unionNode.getOriginalType();
            } else {
                actualNode = unionNode.getOrCreateChild(pointable.getTypeTag(), columnMetadata);
            }
            pointable.accept(this, actualNode);

            currentParent = previousParent;
            columnMetadata.exitNode(node);
        } else if (pointable.getTypeTag() == ATypeTag.NULL && node.isNested()) {
            columnMetadata.addNestedNull(currentParent, (AbstractSchemaNestedNode) node);
        } else {
            pointable.accept(this, node);
        }
    }
}
