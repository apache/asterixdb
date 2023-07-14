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
package org.apache.asterix.om.lazy;

import org.apache.asterix.om.RowMetadata;
import org.apache.asterix.om.lazy.metadata.schema.AbstractRowSchemaNestedNode;
import org.apache.asterix.om.lazy.metadata.schema.AbstractRowSchemaNode;
import org.apache.asterix.om.lazy.metadata.schema.ObjectRowSchemaNode;
import org.apache.asterix.om.lazy.metadata.schema.collection.AbstractRowCollectionSchemaNode;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.utils.RunRowLengthIntArray;
// import org.apache.asterix.om.values.IRowValuesWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.VoidPointable;

public class RowTransformer implements ILazyVisitablePointableVisitor<AbstractRowSchemaNode, AbstractRowSchemaNode> {

    private final RowMetadata columnMetadata;
    private final VoidPointable nonTaggedValue;
    private final ObjectRowSchemaNode root;
    private AbstractRowSchemaNestedNode currentParent;
    private int primaryKeysLength;

    public RowTransformer(RowMetadata columnMetadata, ObjectRowSchemaNode root) {
        this.columnMetadata = columnMetadata;
        this.root = root;
        nonTaggedValue = new VoidPointable();
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

    @Override
    public AbstractRowSchemaNode visit(RecordLazyVisitablePointable pointable, AbstractRowSchemaNode arg)
            throws HyracksDataException {
        columnMetadata.enterNode(currentParent, arg);
        AbstractRowSchemaNestedNode previousParent = currentParent;

        ObjectRowSchemaNode objectNode = (ObjectRowSchemaNode) arg;
        currentParent = objectNode;
        for (int i = 0; i < pointable.getNumberOfChildren(); i++) {
            pointable.nextChild();
            IValueReference fieldName = pointable.getFieldName();
            ATypeTag childTypeTag = pointable.getChildTypeTag();
            if (childTypeTag != ATypeTag.MISSING) {
                //Only write actual field values (including NULL) but ignore MISSING fields
                AbstractRowSchemaNode childNode = objectNode.getOrCreateChild(fieldName, childTypeTag, columnMetadata);
                // Writing into Columnar format               acceptActualNode(pointable.getChildVisitablePointable(), childNode);
            }
        }

        columnMetadata.exitNode(arg);
        currentParent = previousParent;
        return null;
    }

    @Override
    public AbstractRowSchemaNode visit(AbstractListLazyVisitablePointable pointable, AbstractRowSchemaNode arg)
            throws HyracksDataException {
        columnMetadata.enterNode(currentParent, arg);
        AbstractRowSchemaNestedNode previousParent = currentParent;

        AbstractRowCollectionSchemaNode collectionNode = (AbstractRowCollectionSchemaNode) arg;
        RunRowLengthIntArray defLevels = columnMetadata.getDefinitionLevels(collectionNode);
        //the level at which an item is missing
        int missingLevel = columnMetadata.getLevel();
        currentParent = collectionNode;

        int numberOfChildren = pointable.getNumberOfChildren();
        for (int i = 0; i < numberOfChildren; i++) {
            pointable.nextChild();
            ATypeTag childTypeTag = pointable.getChildTypeTag();
            AbstractRowSchemaNode childNode = collectionNode.getOrCreateItem(childTypeTag, columnMetadata);
            //  Writing into Columnar format        acceptActualNode(pointable.getChildVisitablePointable(), childNode);
            /*
             * The array item may change (e.g., BIGINT --> UNION). Thus, new items would be considered as missing
             */
            defLevels.add(missingLevel);
        }

        columnMetadata.exitCollectionNode(collectionNode, numberOfChildren);
        currentParent = previousParent;
        return null;
    }

    @Override
    public AbstractRowSchemaNode visit(FlatLazyVisitablePointable pointable, AbstractRowSchemaNode arg)
            throws HyracksDataException {
        columnMetadata.enterNode(currentParent, arg);
        //        ATypeTag valueTypeTag = pointable.getTypeTag();
        //        PrimitiveRowSchemaNode node = (PrimitiveRowSchemaNode) arg;
        //        IRowValuesWriter writer = columnMetadata.getWriter(node.getColumnIndex());
        //        if (valueTypeTag == ATypeTag.MISSING) {
        //            writer.writeLevel(columnMetadata.getLevel());
        //        } else if (valueTypeTag == ATypeTag.NULL) {
        //            writer.writeNull(columnMetadata.getLevel());
        //        } else if (pointable.isTagged()) {
        //            //Remove type tag
        //            nonTaggedValue.set(pointable.getByteArray(), pointable.getStartOffset() + 1, pointable.getLength() - 1);
        //            writer.writeValue(pointable.getTypeTag(), nonTaggedValue);
        //        } else {
        //            writer.writeValue(pointable.getTypeTag(), pointable);
        //        }
        //        if (node.isPrimaryKey()) {
        //            primaryKeysLength += writer.getEstimatedSize();
        //        }
        columnMetadata.exitNode(arg);
        return null;
    }

    //    private void acceptActualNode(AbstractLazyVisitablePointable pointable, AbstractRowSchemaNode node)
    //            throws HyracksDataException {
    //        if (node.getTypeTag() == ATypeTag.UNION) {
    //            columnMetadata.enterNode(currentParent, node);
    //            AbstractRowSchemaNestedNode previousParent = currentParent;
    //
    //            UnionRowSchemaNode unionNode = (UnionRowSchemaNode) node;
    //            currentParent = unionNode;
    //
    //            ATypeTag childTypeTag = pointable.getTypeTag();
    //            AbstractRowSchemaNode actualNode;
    //            if (childTypeTag == ATypeTag.NULL || childTypeTag == ATypeTag.MISSING) {
    //                actualNode = unionNode.getOriginalType();
    //            } else {
    //                actualNode = unionNode.getOrCreateChild(pointable.getTypeTag(), columnMetadata);
    //            }
    //            pointable.accept(this, actualNode);
    //
    //            currentParent = previousParent;
    //            columnMetadata.exitNode(node);
    //        } else if (pointable.getTypeTag() == ATypeTag.NULL && node.isNested()) {
    //            columnMetadata.addNestedNull(currentParent, (AbstractRowSchemaNestedNode) node);
    //        } else {
    //            pointable.accept(this, node);
    //        }
    //    }
}
