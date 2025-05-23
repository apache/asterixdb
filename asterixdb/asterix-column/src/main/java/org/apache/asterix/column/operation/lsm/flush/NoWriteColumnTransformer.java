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
    private final int metaColumnCount;
    private int currentBatchVersion;
    private int beforeTransformColumnsCount = 0;
    private AbstractSchemaNestedNode currentParent;

    public NoWriteColumnTransformer(NoWriteFlushColumnMetadata columnMetadata, ObjectSchemaNode root,
            ObjectSchemaNode metaRoot) {
        this.columnMetadata = columnMetadata;
        this.root = root;
        this.metaColumnCount = metaRoot != null ? metaRoot.getNumberOfColumns() : 0;
        currentBatchVersion = 1;
    }

    /**
     * Transform a tuple in row format into columns
     *
     * @param pointable record pointable
     * @return the estimated size (possibly overestimated) of the primary key(s) columns
     */
    public int transform(RecordLazyVisitablePointable pointable) throws HyracksDataException {
        beforeTransformColumnsCount = getNumberOfVisitedColumnsInBatch();
        pointable.accept(this, root);
        return 0;
    }

    public int getBeforeTransformColumnsCount() {
        return beforeTransformColumnsCount;
    }

    public void reset() {
        currentBatchVersion++;
    }

    @Override
    public AbstractSchemaNode visit(RecordLazyVisitablePointable pointable, AbstractSchemaNode arg)
            throws HyracksDataException {
        AbstractSchemaNestedNode previousParent = currentParent;

        ObjectSchemaNode objectNode = (ObjectSchemaNode) arg;
        currentParent = objectNode;
        int newDiscoveredColumns = 0;
        if (currentParent.getVisitedBatchVersion() != currentBatchVersion) {
            objectNode.setNumberOfVisitedColumnsInBatch(0);
            objectNode.setVisitedBatchVersion(currentBatchVersion);
            objectNode.setMissingInitiallyInBatch(false);
            objectNode.needAllColumns(false);
        }

        for (int i = 0; i < pointable.getNumberOfChildren(); i++) {
            pointable.nextChild();
            IValueReference fieldName = pointable.getFieldName();
            ATypeTag childTypeTag = pointable.getChildTypeTag();
            if (childTypeTag != ATypeTag.MISSING) {
                AbstractSchemaNode childNode = objectNode.getOrCreateChild(fieldName, childTypeTag, columnMetadata);
                acceptActualNode(pointable.getChildVisitablePointable(), childNode);
                int childDiscoveredColumns = childNode.getNewDiscoveredColumns();
                if (childNode.formerChildNullVersion() == currentBatchVersion) {
                    //Missing or NULL contributed to one of the columns
                    childNode.setFormerChildNull(-1);
                    childDiscoveredColumns -= 1;
                } else {
                    childNode.setFormerChildNull(-1);
                }
                newDiscoveredColumns += childDiscoveredColumns;
                childNode.setNewDiscoveredColumns(0);
                currentParent.incrementColumns(childNode.getDeltaColumnsChanged());
            }
        }

        if (pointable.getNumberOfChildren() == 0) {
            // Set as empty object
            AbstractSchemaNode missingChild = objectNode.setEmptyObject(columnMetadata);
            if (!objectNode.isMissingInitiallyInBatch() && objectNode.isEmptyObject()) {
                objectNode.needAllColumns(true); // to include the missing column, while finalizing the batch.
                objectNode.setMissingInitiallyInBatch(true);
                if (missingChild != null) {
                    currentParent.incrementColumns(missingChild.getDeltaColumnsChanged());
                }
                newDiscoveredColumns += 1;
            }
        } else if (objectNode.isMissingInitiallyInBatch()) {
            objectNode.setMissingInitiallyInBatch(false);
            objectNode.needAllColumns(false);
        }

        if (objectNode.needAllColumns()) {
            // parent is not array, but objectNode need all columns, because this node used to be null
            int previousContribution = currentParent.getNumberOfVisitedColumnsInBatch();
            newDiscoveredColumns = 0; // reset the new discovered columns
            newDiscoveredColumns -= previousContribution;
            newDiscoveredColumns += currentParent.getNumberOfColumns();
            currentParent.setNewDiscoveredColumns(newDiscoveredColumns);
            currentParent.setNumberOfVisitedColumnsInBatch(currentParent.getNumberOfColumns());
        } else {
            currentParent.setNewDiscoveredColumns(newDiscoveredColumns);
            currentParent.setNumberOfVisitedColumnsInBatch(
                    currentParent.getNumberOfVisitedColumnsInBatch() + newDiscoveredColumns);
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

        if (currentParent.getVisitedBatchVersion() != currentBatchVersion) {
            currentParent.setVisitedBatchVersion(currentBatchVersion);
            currentParent.setNumberOfVisitedColumnsInBatch(0);
            currentParent.setMissingInitiallyInBatch(false);
            currentParent.needAllColumns(false);
        }

        currentParent.needAllColumns(true);

        int newDiscoveredColumns = 0;
        int numberOfChildren = pointable.getNumberOfChildren();
        for (int i = 0; i < numberOfChildren; i++) {
            pointable.nextChild();
            ATypeTag childTypeTag = pointable.getChildTypeTag();
            AbstractSchemaNode childNode = collectionNode.getOrCreateItem(childTypeTag, columnMetadata);
            acceptActualNode(pointable.getChildVisitablePointable(), childNode);
            currentParent.incrementColumns(childNode.getDeltaColumnsChanged());
            int childDiscoveredColumns = childNode.getNewDiscoveredColumns();
            if (childNode.formerChildNullVersion() == currentBatchVersion) {
                //Missing or NULL contributed to one of the columns
                childNode.setFormerChildNull(-1);
                childDiscoveredColumns -= 1;
            } else {
                childNode.setFormerChildNull(-1);
            }
            newDiscoveredColumns += childDiscoveredColumns;
            childNode.setNewDiscoveredColumns(0);
        }

        // Add missing as a last element of the array to help indicate empty arrays
        collectionNode.getOrCreateItem(ATypeTag.MISSING, columnMetadata);

        // need all the columns
        newDiscoveredColumns = 0;
        newDiscoveredColumns -= currentParent.getNumberOfVisitedColumnsInBatch();
        newDiscoveredColumns += currentParent.getNumberOfColumns();
        currentParent.setNewDiscoveredColumns(newDiscoveredColumns);
        currentParent.setNumberOfVisitedColumnsInBatch(currentParent.getNumberOfColumns());
        currentParent = previousParent;
        return null;
    }

    @Override
    public AbstractSchemaNode visit(FlatLazyVisitablePointable pointable, AbstractSchemaNode arg)
            throws HyracksDataException {
        PrimitiveSchemaNode node = (PrimitiveSchemaNode) arg;

        if (node.getVisitedBatchVersion() != currentBatchVersion) {
            //First time in this batch
            node.setNumberOfVisitedColumnsInBatch(1);
            node.setNewDiscoveredColumns(1);
            node.setVisitedBatchVersion(currentBatchVersion);
        }

        return null;
    }

    private void acceptActualNode(AbstractLazyVisitablePointable pointable, AbstractSchemaNode node)
            throws HyracksDataException {
        if (node.getTypeTag() == ATypeTag.UNION) {
            AbstractSchemaNestedNode previousParent = currentParent;

            UnionSchemaNode unionNode = (UnionSchemaNode) node;
            currentParent = unionNode;

            ATypeTag childTypeTag = pointable.getTypeTag();

            AbstractSchemaNode actualNode;
            if (childTypeTag == ATypeTag.NULL || childTypeTag == ATypeTag.MISSING) {
                /*
                 * NULL and MISSING are tracked since the start to be written in the originalType (i.e., the type
                 * before injecting a union between the parent and the original node).
                 */
                actualNode = unionNode.getOriginalType();
                acceptActualNode(pointable, actualNode);
            } else {
                actualNode = unionNode.getOrCreateChild(pointable.getTypeTag(), columnMetadata);
                pointable.accept(this, actualNode);
            }
            unionNode.setNewDiscoveredColumns(actualNode.getNewDiscoveredColumns());
            unionNode.setNumberOfVisitedColumnsInBatch(
                    unionNode.getNumberOfVisitedColumnsInBatch() + actualNode.getNewDiscoveredColumns());
            actualNode.setNewDiscoveredColumns(0);
            currentParent.incrementColumns(actualNode.getDeltaColumnsChanged());
            currentParent = previousParent;
        } else if (pointable.getTypeTag() == ATypeTag.NULL && node.isNested()) {
            node.needAllColumns(true);
            int previousContribution = node.getNumberOfVisitedColumnsInBatch();
            int netContribution = node.getNumberOfColumns() - previousContribution;
            node.setNewDiscoveredColumns(netContribution);
            node.setNumberOfVisitedColumnsInBatch(node.getNumberOfColumns());
            columnMetadata.addNestedNull(currentParent, (AbstractSchemaNestedNode) node, false);
        } else {
            pointable.accept(this, node);
        }
    }

    public int getNumberOfVisitedColumnsInBatch() {
        //In case of batch of anti-matters, the current batch version is not equal to the root's visited batch version.
        if (currentBatchVersion != root.getVisitedBatchVersion()) {
            return columnMetadata.getNumberOfPrimaryKeys();
        }
        return root.getNumberOfVisitedColumnsInBatch() + metaColumnCount;
    }
}
