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

import java.util.ArrayList;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.utils.RunRowLengthIntArray;
import org.apache.asterix.runtime.schemainferrence.collection.AbstractRowCollectionSchemaNode;
import org.apache.asterix.runtime.schemainferrence.collection.ArrayRowSchemaNode;
import org.apache.asterix.runtime.schemainferrence.collection.GenericListRowSchemaNode;
import org.apache.asterix.runtime.schemainferrence.collection.MultisetRowSchemaNode;
// import org.
import org.apache.asterix.runtime.schemainferrence.lazy.IObjectRowSchemaNodeVisitor;
import org.apache.asterix.runtime.schemainferrence.primitive.PrimitiveRowSchemaNode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.VoidPointable;

import it.unimi.dsi.fastutil.ints.IntList;

public class RowSchemaTransformer implements IObjectRowSchemaNodeVisitor<AbstractRowSchemaNode, AbstractRowSchemaNode> {

    private final RowMetadata rowMetadata;
    private final VoidPointable nonTaggedValue;
    private final ObjectRowSchemaNode root;
    private AbstractRowSchemaNestedNode currentParent;
    private int primaryKeysLength;

    public ObjectRowSchemaNode getRoot() {
        return root;
    }

    public RowSchemaTransformer(RowMetadata rowMetadata, ObjectRowSchemaNode root) {
        this.rowMetadata = rowMetadata;
        this.root = root;
        nonTaggedValue = new VoidPointable();
    }

    /**
     * Transform a tuple in row format into columns
     *
     * @param toMergeRoot record pointable
     * @return the estimated size (possibly overestimated) of the primary key(s) columns
     */

    public int transform(ObjectRowSchemaNode toMergeRoot) throws HyracksDataException {
        primaryKeysLength = 0;
        toMergeRoot.accept(this, root);
        return primaryKeysLength;
    }

    @Override
    public AbstractRowSchemaNode visit(ObjectRowSchemaNode toMergeRoot, AbstractRowSchemaNode mainRoot)
            throws HyracksDataException {
        rowMetadata.enterNode(currentParent, mainRoot);
        AbstractRowSchemaNestedNode previousParent = currentParent;

        ObjectRowSchemaNode objectNode = (ObjectRowSchemaNode) mainRoot;
        rowMetadata.printRootSchema(objectNode, rowMetadata.getFieldNamesDictionary());
        //        rowMetadata.printRootSchema(toMergeRoot, rowMetadata.getFieldNamesDictionary(),"MERGER SCHEMA BY SCHEMA BEFORE");
        currentParent = objectNode;
        IntList fieldNameIndexes = toMergeRoot.getChildrenFieldNameIndexes();
        for (int i = 0; i < toMergeRoot.getNumberOfChildren(); i++) {
            int index = fieldNameIndexes.getInt(i);
            AbstractRowSchemaNode child = toMergeRoot.getChild(index);
            //            ObjectRowSchemaNode objectMergeNode = (ObjectRowSchemaNode) child;
            IValueReference fieldName = child.getFieldName();
            ATypeTag childTypeTag = child.getTypeTag();
            if (childTypeTag == ATypeTag.UNION) {
                UnionRowSchemaNode unionChild = (UnionRowSchemaNode) child;
                unionChild.accept(this, mainRoot);
            } else if (childTypeTag != ATypeTag.MISSING) {
                //Only write actual field values (including NULL) but ignore MISSING fields
                AbstractRowSchemaNode childNode = objectNode.getOrCreateChild(fieldName, childTypeTag, rowMetadata);
                acceptActualNode(new GenericListRowSchemaNode(childTypeTag, child), childNode);
            }
        }
        rowMetadata.printRootSchema(objectNode, rowMetadata.getFieldNamesDictionary());
        rowMetadata.exitNode(mainRoot);
        currentParent = previousParent;
        return null;
    }

    @Override
    public AbstractRowSchemaNode visit(MultisetRowSchemaNode toMergeRoot, AbstractRowSchemaNode mainRoot)
            throws HyracksDataException {
        rowMetadata.enterNode(currentParent, mainRoot);
        AbstractRowSchemaNestedNode previousParent = currentParent;

        AbstractRowCollectionSchemaNode collectionNode = (AbstractRowCollectionSchemaNode) mainRoot;
        RunRowLengthIntArray defLevels = rowMetadata.getDefinitionLevels(collectionNode);
        //the level at which an item is missing
        int missingLevel = rowMetadata.getLevel();
        currentParent = collectionNode;
        int numberOfChildren = 0;

        if (toMergeRoot.getItemTypeTag() == ATypeTag.UNION) {
            AbstractRowSchemaNode unionItem = toMergeRoot.getItemNode();
            numberOfChildren = unionItem.getNumberOfChildren();
            ArrayList<AbstractRowSchemaNode> unionChildren = ((UnionRowSchemaNode) unionItem).getChildrenList();

            for (AbstractRowSchemaNode unionChildNode : unionChildren) {
                AbstractRowSchemaNode child = unionChildNode;
                ATypeTag childTypeTag = child.getTypeTag();
                IValueReference fieldName = child.getFieldName(); //TODO CALVIN_DANI add correct fieldName
                AbstractRowSchemaNode childNode = collectionNode.getOrCreateItem(childTypeTag, rowMetadata, fieldName);
                acceptActualNode(new GenericListRowSchemaNode(childTypeTag, child), childNode);
                /*
                 * The array item may change (e.g., BIGINT --> UNION). Thus, new items would be considered as missing
                 */
                defLevels.add(missingLevel);
            }
        } else {
            numberOfChildren = 1;
            AbstractRowSchemaNode primItem = toMergeRoot.getItemNode();
            ATypeTag childTypeTag = primItem.getTypeTag();
            IValueReference fieldName = primItem.getFieldName(); //TODO CALVIN_DANI add correct fieldName
            AbstractRowSchemaNode childNode = collectionNode.getOrCreateItem(childTypeTag, rowMetadata, fieldName);
            acceptActualNode(new GenericListRowSchemaNode(childTypeTag, primItem), childNode);
        }

        rowMetadata.exitCollectionNode(collectionNode, numberOfChildren);
        currentParent = previousParent;
        return null;
    }

    @Override
    public AbstractRowSchemaNode visit(ArrayRowSchemaNode toMergeRoot, AbstractRowSchemaNode mainRoot)
            throws HyracksDataException {
        rowMetadata.enterNode(currentParent, mainRoot);
        AbstractRowSchemaNestedNode previousParent = currentParent;

        AbstractRowCollectionSchemaNode collectionNode = (AbstractRowCollectionSchemaNode) mainRoot;
        RunRowLengthIntArray defLevels = rowMetadata.getDefinitionLevels(collectionNode);
        //the level at which an item is missing
        int missingLevel = rowMetadata.getLevel();
        currentParent = collectionNode;
        int numberOfChildren = 0;
        if (toMergeRoot.getItemTypeTag() == ATypeTag.UNION) {
            AbstractRowSchemaNode unionItem = toMergeRoot.getItemNode();
            numberOfChildren = unionItem.getNumberOfChildren();
            ArrayList<AbstractRowSchemaNode> unionChildren = ((UnionRowSchemaNode) unionItem).getChildrenList();

            for (AbstractRowSchemaNode unionChildNode : unionChildren) {
                AbstractRowSchemaNode child = unionChildNode;
                ATypeTag childTypeTag = child.getTypeTag();
                IValueReference fieldName = child.getFieldName(); //TODO CALVIN_DANI add correct fieldName
                AbstractRowSchemaNode childNode = collectionNode.getOrCreateItem(childTypeTag, rowMetadata, fieldName);
                acceptActualNode(new GenericListRowSchemaNode(childTypeTag, child), childNode);
                /*
                 * The array item may change (e.g., BIGINT --> UNION). Thus, new items would be considered as missing
                 */
                defLevels.add(missingLevel);
            }
        } else {
            numberOfChildren = 1;
            AbstractRowSchemaNode primItem = toMergeRoot.getItemNode();
            ATypeTag childTypeTag = primItem.getTypeTag();
            IValueReference fieldName = primItem.getFieldName(); //TODO CALVIN_DANI add now correct fieldName
            AbstractRowSchemaNode childNode = collectionNode.getOrCreateItem(childTypeTag, rowMetadata, fieldName);
            acceptActualNode(new GenericListRowSchemaNode(childTypeTag, primItem), childNode);
        }

        rowMetadata.exitCollectionNode(collectionNode, numberOfChildren);
        currentParent = previousParent;
        return null;
    }

    @Override
    public AbstractRowSchemaNode visit(UnionRowSchemaNode toMergeRoot, AbstractRowSchemaNode mainRoot)
            throws HyracksDataException {
        rowMetadata.enterNode(currentParent, mainRoot);
        AbstractRowSchemaNestedNode previousParent = currentParent;

        ObjectRowSchemaNode objectNode = (ObjectRowSchemaNode) mainRoot;
        currentParent = objectNode;
        //
        ArrayList<AbstractRowSchemaNode> unionChildren = toMergeRoot.getChildrenList();

        for (AbstractRowSchemaNode unionChildNode : unionChildren) {
            IValueReference fieldName = toMergeRoot.getFieldName();
            ATypeTag unionChildTypeTag = unionChildNode.getTypeTag();
            if (unionChildTypeTag != ATypeTag.MISSING) {
                AbstractRowSchemaNode childNode =
                        objectNode.getOrCreateChild(fieldName, unionChildTypeTag, rowMetadata);
                acceptActualNode(new GenericListRowSchemaNode(unionChildTypeTag, unionChildNode), childNode);
            }
        }

        //        rowMetadata.printRootSchema(objectNode, rowMetadata.getFieldNamesDictionary());
        rowMetadata.exitNode(mainRoot);
        currentParent = previousParent;
        return null;
    }

    @Override
    public AbstractRowSchemaNode visit(PrimitiveRowSchemaNode toMergeRoot, AbstractRowSchemaNode mainRoot)
            throws HyracksDataException {

        return null;
    }

    @Override
    public AbstractRowSchemaNode visit(AbstractRowCollectionSchemaNode collectionNode, AbstractRowSchemaNode mainRoot)
            throws HyracksDataException {
        return null;
    }

    private void acceptActualNode(AbstractRowSchemaNode nodeToAdd, AbstractRowSchemaNode node)
            throws HyracksDataException {
        if (node.getTypeTag() == ATypeTag.UNION) {
            rowMetadata.enterNode(currentParent, node);
            AbstractRowSchemaNestedNode previousParent = currentParent;

            UnionRowSchemaNode unionNode = (UnionRowSchemaNode) node;
            currentParent = unionNode;

            ATypeTag childTypeTag = nodeToAdd.getTypeTag();
            IValueReference fieldName = nodeToAdd.getFieldName();
            AbstractRowSchemaNode actualNode;
            if (childTypeTag == ATypeTag.NULL || childTypeTag == ATypeTag.MISSING) {
                actualNode = unionNode.getOriginalType();
            } else {
                actualNode = unionNode.getOrCreateChild(childTypeTag, rowMetadata, fieldName);
            }
            //            if (actualNode.getTypeTag() == ATypeTag.MULTISET) {
            //                GenericListRowSchemaNode genericNode = new GenericListRowSchemaNode(nodeToAdd.getTypeTag(),nodeToAdd);
            nodeToAdd.accept(this, actualNode);
            //            }

            currentParent = previousParent;
            rowMetadata.exitNode(node);
        } else if (node.getTypeTag() == ATypeTag.NULL && node.isNested()) {
            //            rowMetadata.addNestedNull(currentParent, (AbstractRowSchemaNestedNode) node);
        } else {
            nodeToAdd.accept(this, node);
        }
    }

}
