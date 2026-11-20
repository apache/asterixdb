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

import org.apache.asterix.column.metadata.schema.AbstractSchemaNestedNode;
import org.apache.asterix.column.metadata.schema.AbstractSchemaNode;
import org.apache.asterix.column.metadata.schema.ObjectSchemaNode;
import org.apache.asterix.column.metadata.schema.UnionSchemaNode;
import org.apache.asterix.column.metadata.schema.collection.AbstractCollectionSchemaNode;
import org.apache.asterix.column.metadata.schema.collection.MapSchemaNode;
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
    private final BitSet presentColumnsIndexes;
    private AbstractSchemaNestedNode currentParent;
    private int primaryKeysLength;
    /**
     * Estimated encoded size of all non-key values in the current batch, used by {@link FlushColumnTupleWriter} to
     * stop writing tuples to the current mega leaf node before it exceeds the max leaf node size. The encoded size
     * (not the raw value length) is tracked so that well-encoded columns (e.g., delta-packed integers) do not cut
     * leaves short, while poorly-encoded ones (e.g., plain doubles of an embedding array) are not ignored.
     */
    private int valuesEstimatedSize;
    private int currentBatchVersion;

    public ColumnTransformer(FlushColumnMetadata columnMetadata, ObjectSchemaNode root, BitSet presentColumnsIndexes) {
        this.columnMetadata = columnMetadata;
        this.root = root;
        this.presentColumnsIndexes = presentColumnsIndexes;
        nonTaggedValue = new VoidPointable();
        valuesEstimatedSize = 0;
        currentBatchVersion = 1;
    }

    public int getValuesEstimatedSize() {
        return valuesEstimatedSize;
    }

    public void resetValuesEstimatedSize() {
        valuesEstimatedSize = 0;
        currentBatchVersion++;
    }

    /**
     * Transform a tuple in row format into columns
     *
     * @param pointable record pointable
     * @return the estimated size (possibly overestimated) of the primary key(s) columns
     */
    public int transform(RecordLazyVisitablePointable pointable) throws HyracksDataException {
        // clear the last present column indexes.
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
            // include the primary key column
            presentColumnsIndexes.set(i);
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

        if (currentParent.getVisitedBatchVersion() != currentBatchVersion) {
            currentParent.needAllColumns(false);
            currentParent.setMissingInitiallyInBatch(false);
            currentParent.setVisitedBatchVersion(currentBatchVersion);
        }

        objectNode.onDocumentVisited(columnMetadata);
        // The map is resolved on first use, not here: the node can collapse partway through this very document, and
        // the map itself is only built by the first key routed to it. Kept to few locals: this method recurses once
        // per level of a document, and every local it declares is on the stack at each level.
        int mapEntries = 0;
        int mapMissingLevel = 0;
        RunLengthIntArray mapDefLevels = null;

        for (int i = 0; i < pointable.getNumberOfChildren(); i++) {
            pointable.nextChild();
            IValueReference fieldName = pointable.getFieldName();
            ATypeTag childTypeTag = pointable.getChildTypeTag();
            if (childTypeTag != ATypeTag.MISSING) {
                //Only write actual field values (including NULL) but ignore MISSING fields
                if (objectNode.routesToMap(fieldName, columnMetadata)) {
                    if (mapDefLevels == null) {
                        // Entered once per record, not once per entry: all of this record's map entries form a single
                        // repeated group, exactly as a collection's items do.
                        MapSchemaNode mapNode = objectNode.getOrCreateMapNode(columnMetadata);
                        columnMetadata.enterNode(currentParent, mapNode);
                        mapDefLevels = columnMetadata.getDefinitionLevels(mapNode);
                        mapMissingLevel = columnMetadata.getLevel();
                        mapNode.needAllColumns(true);
                    }
                    objectNode.recordCollapsedKey(fieldName, childTypeTag, columnMetadata);
                    writeMapEntry(objectNode.getMapNode(), fieldName, pointable.getChildVisitablePointable(),
                            objectNode);
                    mapDefLevels.add(mapMissingLevel);
                    mapEntries++;
                } else {
                    /*
                     * Once a map entry of this record has been written, the map's repeated group stays open until the
                     * record ends. A column field (promoted, or from before the collapse) that follows it in the
                     * document must step back out to the object's level: written inside the group, the column would
                     * record the map's level and repetition, and its level stream would no longer match its place in
                     * the schema.
                     */
                    if (mapDefLevels != null) {
                        columnMetadata.exitLevel(objectNode.getMapNode());
                    }
                    int childrenBefore = objectNode.getChildren().size();
                    int columnsBefore = columnMetadata.getNumberOfColumns();
                    AbstractSchemaNode childNode = objectNode.getOrCreateChild(fieldName, childTypeTag, columnMetadata);
                    // A child counts towards collapsing this node when this visit starts it growing. Check before the
                    // visit: a child created by it may be born collapsed (from its template), and checked afterwards
                    // it would wrongly look like it was already growing.
                    boolean childWasGrowing = columnMetadata.isCollapseDetectorEnabled()
                            && objectNode.getChildren().size() == childrenBefore && isGrowingObject(childNode);
                    acceptActualNode(pointable.getChildVisitablePointable(), childNode);
                    if (columnMetadata.isCollapseDetectorEnabled() && !childWasGrowing && isGrowingObject(childNode)) {
                        objectNode.onChildGrowing(fieldName, columnMetadata);
                    }
                    if (columnMetadata.isCollapseDetectorEnabled()
                            && objectNode.getChildren().size() > childrenBefore) {
                        objectNode.onNewChildColumns(columnMetadata.getNumberOfColumns() - columnsBefore,
                                columnMetadata);
                    }
                    if (mapDefLevels != null) {
                        columnMetadata.enterLevel(objectNode.getMapNode());
                    }
                }
            }
        }

        if (mapDefLevels != null) {
            // Close the record's group with a trailing missing entry, mirroring how a collection terminates its
            // items. The key column has to receive that level too: it is repeated in lockstep with the value node,
            // so if only one of the two records the terminator their level streams drift apart and the reader stops
            // being able to tell which entry belongs to which record.
            objectNode.getMapNode().getOrCreateItem(ATypeTag.MISSING, columnMetadata);
            mapDefLevels.add(mapMissingLevel);
            columnMetadata.exitCollectionNode(objectNode.getMapNode(), mapEntries);
            currentParent = objectNode;
        } else if (objectNode.isCollapsed() && objectNode.getMapNode() == null) {
            /*
             * The node collapsed, but no key has routed to the map yet. The decision itself is not serialized, only the
             * map is, so build it now, entry-less: a flush whose new key arrives in its first document and is decided
             * on a few documents later has no key left to build it with, and would otherwise lose the collapse. This
             * record is not entered, so it reads the map as missing, as any record without map entries does.
             */
            MapSchemaNode emptyMap = objectNode.getOrCreateMapNode(columnMetadata);
            columnMetadata.enterLevel(emptyMap);
            emptyMap.getOrCreateItem(ATypeTag.MISSING, columnMetadata);
            columnMetadata.exitLevel(emptyMap);
        }

        if (pointable.getNumberOfChildren() == 0) {
            // Set as empty object
            objectNode.setEmptyObject(columnMetadata);
            if (!objectNode.isMissingInitiallyInBatch() && objectNode.isEmptyMissingObject()) {
                objectNode.needAllColumns(true);
                objectNode.setMissingInitiallyInBatch(true);
                PrimitiveSchemaNode missingNode = (PrimitiveSchemaNode) objectNode.getChildren().get(0);
                presentColumnsIndexes.set(missingNode.getColumnIndex());
            }
        } else {
            if (objectNode.isMissingInitiallyInBatch()) {
                objectNode.setMissingInitiallyInBatch(false);
                objectNode.needAllColumns(false);
            }
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

        if (currentParent.getVisitedBatchVersion() != currentBatchVersion) {
            currentParent.needAllColumns(false);
            currentParent.setMissingInitiallyInBatch(false);
            currentParent.setVisitedBatchVersion(currentBatchVersion);
        }

        // If it's an array, all column will be needed as anyone of them, will be a delegate.
        currentParent.needAllColumns(true);

        int numberOfChildren = pointable.getNumberOfChildren();
        int newDiscoveredColumns = 0;
        for (int i = 0; i < numberOfChildren; i++) {
            pointable.nextChild();
            ATypeTag childTypeTag = pointable.getChildTypeTag();
            AbstractSchemaNode childNode = collectionNode.getOrCreateItem(childTypeTag, columnMetadata);
            acceptActualNode(pointable.getChildVisitablePointable(), childNode);
            currentParent.incrementColumns(childNode.getDeltaColumnsChanged());
            newDiscoveredColumns += childNode.getNewDiscoveredColumns();
            childNode.setNewDiscoveredColumns(0);
            /*
             * The array item may change (e.g., BIGINT --> UNION). Thus, new items would be considered as missing
             */
            defLevels.add(missingLevel);
        }

        currentParent.setNewDiscoveredColumns(newDiscoveredColumns);
        currentParent.setNumberOfVisitedColumnsInBatch(
                currentParent.getNumberOfVisitedColumnsInBatch() + newDiscoveredColumns);
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
        presentColumnsIndexes.set(node.getColumnIndex());
        IColumnValuesWriter writer = columnMetadata.getWriter(node.getColumnIndex());
        int sizeBeforeWrite = writer.getEstimatedSize();
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
        } else {
            valuesEstimatedSize += writer.getEstimatedSize() - sizeBeforeWrite;
        }
        columnMetadata.exitNode(arg);
        return null;
    }

    /**
     * Writes one collapsed field: its name into the map's key column, its value into the shared value node.
     * The name is written as a value and is deliberately never interned, so the field-name dictionary stops growing.
     */
    private void writeMapEntry(MapSchemaNode mapNode, IValueReference fieldName, AbstractLazyVisitablePointable value,
            ObjectSchemaNode objectNode) throws HyracksDataException {
        AbstractSchemaNestedNode previousParent = currentParent;
        currentParent = mapNode;

        // Written through enterNode/exitNode exactly as an ordinary flat value is. Bypassing them leaves the key
        // node's counter at zero, and flushDefinitionLevels uses that counter to decide how many missing levels to
        // backfill — so the key column's level stream would no longer align with the value node's.
        PrimitiveSchemaNode keyNode = mapNode.getKeyNode();
        columnMetadata.enterNode(mapNode, keyNode);
        presentColumnsIndexes.set(keyNode.getColumnIndex());
        IColumnValuesWriter keyWriter = columnMetadata.getWriter(keyNode.getColumnIndex());
        int sizeBeforeWrite = keyWriter.getEstimatedSize();
        keyWriter.writeValue(ATypeTag.STRING, fieldName);
        // The key column is an ordinary non-primary-key column, so its growth counts towards the flush leaf size cap
        // exactly as any other value does.
        valuesEstimatedSize += keyWriter.getEstimatedSize() - sizeBeforeWrite;
        columnMetadata.exitNode(keyNode);

        AbstractSchemaNode itemBefore = mapNode.getItemNode();
        AbstractSchemaNode itemNode = mapNode.getOrCreateItem(value.getTypeTag(), columnMetadata);
        if (itemNode != itemBefore) {
            objectNode.onMapValueCreated(itemNode, columnMetadata);
        }
        acceptActualNode(value, itemNode);
        objectNode.incrementColumns(itemNode.getDeltaColumnsChanged());

        currentParent = previousParent;
    }

    private void acceptActualNode(AbstractLazyVisitablePointable pointable, AbstractSchemaNode node)
            throws HyracksDataException {
        if (node.getTypeTag() == ATypeTag.UNION) {
            columnMetadata.enterNode(currentParent, node);
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
                currentParent.incrementColumns(actualNode.getDeltaColumnsChanged());
            } else {
                AbstractSchemaNode actualNode = unionNode.getOrCreateChild(pointable.getTypeTag(), columnMetadata);
                pointable.accept(this, actualNode);
            }

            currentParent = previousParent;
            columnMetadata.exitNode(node);
        } else if (pointable.getTypeTag() == ATypeTag.NULL && node.isNested()) {
            node.needAllColumns(true);
            columnMetadata.addNestedNull(currentParent, (AbstractSchemaNestedNode) node, true);
        } else {
            pointable.accept(this, node);
        }
    }

    private static boolean isGrowingObject(AbstractSchemaNode node) {
        return node instanceof ObjectSchemaNode object && object.isGrowing();
    }
}
