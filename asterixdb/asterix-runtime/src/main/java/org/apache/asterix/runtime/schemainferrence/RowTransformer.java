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
package org.apache.asterix.runtime.schemainferrence;

import org.apache.asterix.om.lazy.AbstractLazyVisitablePointable;
import org.apache.asterix.om.lazy.AbstractListLazyVisitablePointable;
import org.apache.asterix.om.lazy.FlatLazyVisitablePointable;
import org.apache.asterix.om.lazy.ILazyVisitablePointableVisitor;
import org.apache.asterix.om.lazy.RecordLazyVisitablePointable;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.utils.RunRowLengthIntArray;
import org.apache.asterix.runtime.schemainferrence.collection.AbstractRowCollectionSchemaNode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public class RowTransformer implements ILazyVisitablePointableVisitor<AbstractRowSchemaNode, AbstractRowSchemaNode> {

    private final RowMetadata rowMetadata;
    private final VoidPointable nonTaggedValue;
    private final ObjectRowSchemaNode root;
    private AbstractRowSchemaNestedNode currentParent;
    private int primaryKeysLength;

    public ObjectRowSchemaNode getRoot() {
        return root;
    }

    public RowTransformer(RowMetadata rowMetadata, ObjectRowSchemaNode root) {
        this.rowMetadata = rowMetadata;
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
        rowMetadata.enterNode(currentParent, arg);
        AbstractRowSchemaNestedNode previousParent = currentParent;

        ObjectRowSchemaNode objectNode = (ObjectRowSchemaNode) arg;
        currentParent = objectNode;
        for (int i = 0; i < pointable.getNumberOfChildren(); i++) {
            pointable.nextChild();
            IValueReference fieldName = pointable.getFieldName();
            ArrayBackedValueStorage fieldNameProp = new ArrayBackedValueStorage(fieldName.getLength());
            fieldNameProp.append(fieldName);
            ATypeTag childTypeTag = pointable.getChildTypeTag();
            if (childTypeTag != ATypeTag.MISSING) {
                //Only write actual field values (including NULL) but ignore MISSING fields
                AbstractRowSchemaNode childNode = objectNode.getOrCreateChild(fieldNameProp, childTypeTag, rowMetadata);
                acceptActualNode(pointable.getChildVisitablePointable(), childNode, fieldNameProp);
            }
        }
        rowMetadata.printRootSchema(objectNode, rowMetadata.getFieldNamesDictionary());
        rowMetadata.exitNode(arg);
        currentParent = previousParent;
        return null;
    }

    @Override
    public AbstractRowSchemaNode visit(AbstractListLazyVisitablePointable pointable, AbstractRowSchemaNode arg)
            throws HyracksDataException {
        rowMetadata.enterNode(currentParent, arg);
        AbstractRowSchemaNestedNode previousParent = currentParent;

        AbstractRowCollectionSchemaNode collectionNode = (AbstractRowCollectionSchemaNode) arg;
        RunRowLengthIntArray defLevels = rowMetadata.getDefinitionLevels(collectionNode);
        //the level at which an item is missing
        int missingLevel = rowMetadata.getLevel();
        currentParent = collectionNode;

        int numberOfChildren = pointable.getNumberOfChildren();
        for (int i = 0; i < numberOfChildren; i++) {
            pointable.nextChild();
            ATypeTag childTypeTag = pointable.getChildTypeTag();
            IValueReference fieldName = new ArrayBackedValueStorage(1); //TODO CALVIN_DANI add correct fieldName
            AbstractRowSchemaNode childNode = collectionNode.getOrCreateItem(childTypeTag, rowMetadata, null);
            acceptActualNode(pointable.getChildVisitablePointable(), childNode, null);
            /*
             * The array item may change (e.g., BIGINT --> UNION). Thus, new items would be considered as missing
             */
            defLevels.add(missingLevel);
        }

        rowMetadata.exitCollectionNode(collectionNode, numberOfChildren);
        currentParent = previousParent;
        return null;
    }

    @Override
    public AbstractRowSchemaNode visit(FlatLazyVisitablePointable pointable, AbstractRowSchemaNode arg)
            throws HyracksDataException {
        //        rowMetadata.enterNode(currentParent, arg);
        //                ATypeTag valueTypeTag = pointable.getTypeTag();
        //                PrimitiveRowSchemaNode node = (PrimitiveRowSchemaNode) arg;
        //                IRowValuesWriter writer = rowMetadata.getWriter(node.getColumnIndex()); //TODO CALVIN_DANI : Writer issue to be debugged
        //                if (valueTypeTag == ATypeTag.MISSING) {
        //                    writer.writeLevel(rowMetadata.getLevel());
        //                } else if (valueTypeTag == ATypeTag.NULL) {
        //                    writer.writeNull(rowMetadata.getLevel());
        //                } else if (pointable.isTagged()) {
        //                    //Remove type tag
        //                    nonTaggedValue.set(pointable.getByteArray(), pointable.getStartOffset() + 1, pointable.getLength() - 1);
        //                    writer.writeValue(pointable.getTypeTag(), nonTaggedValue);
        //                } else {
        //                    writer.writeValue(pointable.getTypeTag(), pointable);
        //                }
        //                if (node.isPrimaryKey()) {
        //                    primaryKeysLength += writer.getEstimatedSize();
        //                }
        //        rowMetadata.exitNode(arg);
        return null;
    }

    private void acceptActualNode(AbstractLazyVisitablePointable pointable, AbstractRowSchemaNode node,
            IValueReference fieldName) throws HyracksDataException {
        if (node.getTypeTag() == ATypeTag.UNION) {
            rowMetadata.enterNode(currentParent, node);
            AbstractRowSchemaNestedNode previousParent = currentParent;

            UnionRowSchemaNode unionNode = (UnionRowSchemaNode) node;
            currentParent = unionNode;

            ATypeTag childTypeTag = pointable.getTypeTag();
            AbstractRowSchemaNode actualNode;
            if (childTypeTag == ATypeTag.NULL || childTypeTag == ATypeTag.MISSING) {
                actualNode = unionNode.getOriginalType();
            } else {
                actualNode = unionNode.getOrCreateChild(pointable.getTypeTag(), rowMetadata, fieldName);
            }
            pointable.accept(this, actualNode);

            currentParent = previousParent;
            rowMetadata.exitNode(node);
        } else if (pointable.getTypeTag() == ATypeTag.NULL && node.isNested()) {
            rowMetadata.addNestedNull(currentParent, (AbstractRowSchemaNestedNode) node);
        } else {
            pointable.accept(this, node);
        }
    }
}
