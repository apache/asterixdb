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
package org.apache.asterix.column.metadata.schema;

import static org.apache.asterix.om.dictionary.AbstractFieldNamesDictionary.DUMMY_FIELD_NAME_INDEX;
import static org.apache.asterix.om.dictionary.AbstractFieldNamesDictionary.MAP_FIELD_NAME_INDEX;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.asterix.column.metadata.PathInfoSerializer;
import org.apache.asterix.column.metadata.schema.collection.MapSchemaNode;
import org.apache.asterix.column.metadata.schema.primitive.MissingFieldSchemaNode;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.asterix.column.util.RunLengthIntArray;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.util.annotations.CriticalPath;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap.Entry;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntImmutableList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntUnaryOperator;

public final class ObjectSchemaNode extends AbstractSchemaNestedNode {
    private static final Logger LOGGER = LogManager.getLogger();

    private final Int2IntMap fieldNameIndexToChildIndexMap;
    private final List<AbstractSchemaNode> children;
    private IntUnaryOperator nextIndex;

    /**
     * Cached view of the collapsed child, which is stored among {@link #children} under
     * {@link org.apache.asterix.om.dictionary.AbstractFieldNamesDictionary#MAP_FIELD_NAME_INDEX} like any other child
     * — so it serializes and is walked by every visitor without special handling. Non-null once this node's
     * field-name set was judged unbounded. Children discovered before that point stay as ordinary columns forever
     * (bounded, and required for merge: column indexes are append-only, so nothing may be withdrawn from an
     * already-written component). Everything discovered after lands in the map.
     */
    private MapSchemaNode mapNode;
    /**
     * Decides whether this node collapses, while a flush with collapsing enabled visits it; null on every node a query,
     * a merge or a clipper builds, once the node has collapsed, and after a rolled-back flush, whose counts it would
     * otherwise carry over. See {@link CollapseDetector}.
     */
    private CollapseDetector detector;

    public ObjectSchemaNode() {
        fieldNameIndexToChildIndexMap = new Int2IntOpenHashMap();
        children = new ArrayList<>();
        nextIndex = this::nextIndex;
    }

    ObjectSchemaNode(DataInput input, Map<AbstractSchemaNestedNode, RunLengthIntArray> definitionLevels)
            throws IOException {
        if (definitionLevels != null) {
            definitionLevels.put(this, new RunLengthIntArray());
        }
        int numberOfChildren = input.readInt();

        fieldNameIndexToChildIndexMap = new Int2IntOpenHashMap();
        deserializeFieldNameIndexToChildIndex(input, fieldNameIndexToChildIndexMap, numberOfChildren);
        if (fieldNameIndexToChildIndexMap.containsKey(DUMMY_FIELD_NAME_INDEX)) {
            nextIndex = this::emptyColumnIndex;
        } else {
            nextIndex = this::nextIndex;
        }

        children = new ArrayList<>();
        numberOfColumns = deserializeChildren(input, children, numberOfChildren, definitionLevels);
        restoreCollapsedChild();
        previousNumberOfColumns = numberOfColumns;
    }

    public AbstractSchemaNode getOrCreateChild(IValueReference fieldName, ATypeTag childTypeTag,
            FlushColumnMetadata columnMetadata) throws HyracksDataException {
        int numberOfChildren = children.size();
        int fieldNameIndex = columnMetadata.getFieldNamesDictionary().getOrCreateFieldNameIndex(fieldName);
        boolean previouslyMissing = isEmptyMissingObject();
        int childIndex = fieldNameIndexToChildIndexMap.getOrDefault(fieldNameIndex, nextIndex.apply(fieldNameIndex));
        AbstractSchemaNode currentChild = childIndex == numberOfChildren ? null : children.get(childIndex);
        AbstractSchemaNode newChild = columnMetadata.getOrCreateChild(currentChild, childTypeTag, previouslyMissing);
        CollapseDetector collapseDetector = detector(columnMetadata);
        if (collapseDetector != null) {
            collapseDetector.onChildPresent(childIndex, currentChild != null);
        }
        if (currentChild == null) {
            children.add(childIndex, newChild);
            fieldNameIndexToChildIndexMap.put(fieldNameIndex, childIndex);
            if (collapseDetector != null) {
                collapseDetector.onNewName(columnMetadata);
            }
        } else if (currentChild != newChild) {
            numberOfColumns -= currentChild.getNumberOfColumns();
            children.set(childIndex, newChild);
            newChild.getDeltaColumnsChanged();
            numberOfColumns += newChild.getNumberOfColumns();
        } else {
            // If the child is the same, we just update the number of columns
            numberOfColumns += currentChild.getDeltaColumnsChanged();
        }

        return newChild;
    }

    public boolean isMapified() {
        return mapNode != null;
    }

    /**
     * Set when the detector decides to collapse, before the map exists. The map is built by the first key routed to it
     * ({@link #getOrCreateMapNode}), or, if the document that decided routed none, as that document's visit of this
     * node ends -- never at the decision itself, which can come mid-document, outside the level context the map is
     * built in. Not serialized: the map is what records the collapse.
     */
    private boolean collapsePending;

    /** True once this node collapsed, including the window between the decision and the map's first entry. */
    public boolean isCollapsed() {
        return mapNode != null || collapsePending;
    }

    /** The map, created on first use in the enclosing object's level context -- the context it is always built in. */
    public MapSchemaNode getOrCreateMapNode(FlushColumnMetadata columnMetadata) throws HyracksDataException {
        if (mapNode == null) {
            mapNode = columnMetadata.createMapNode();
            addChild(MAP_FIELD_NAME_INDEX, mapNode);
            numberOfColumns += mapNode.getNumberOfColumns();
            collapsePending = false;
        }
        return mapNode;
    }

    public MapSchemaNode getMapNode() {
        return mapNode;
    }

    /**
     * This node's detector, built on first use by a flush that collapses; null when the flush does not, or once this
     * node has collapsed -- the detector's work is then finished.
     */
    private CollapseDetector detector(FlushColumnMetadata columnMetadata) {
        if (detector == null && !isCollapsed() && columnMetadata.isCollapseDetectorEnabled()) {
            detector = new CollapseDetector(this, columnMetadata.getCollapseDetectorSettings());
        }
        return isCollapsed() ? null : detector;
    }

    /** Called once per document that contains this node; see {@link CollapseDetector#onDocumentVisited}. */
    public void onDocumentVisited(FlushColumnMetadata columnMetadata) throws HyracksDataException {
        CollapseDetector collapseDetector = detector(columnMetadata);
        if (collapseDetector != null) {
            collapseDetector.onDocumentVisited(columnMetadata);
        }
    }

    /** Called with the columns the subtree of a name this node has just minted created. */
    public void onNewChildColumns(int columns, FlushColumnMetadata columnMetadata) throws HyracksDataException {
        CollapseDetector collapseDetector = detector(columnMetadata);
        if (collapseDetector != null) {
            collapseDetector.onNewChildColumns(columns, columnMetadata);
        }
    }

    /** Called when the child {@code fieldName} has just started growing ({@link #isGrowing}). */
    public void onChildGrowing(IValueReference fieldName, FlushColumnMetadata columnMetadata)
            throws HyracksDataException {
        CollapseDetector collapseDetector = detector(columnMetadata);
        if (collapseDetector != null) {
            int fieldNameIndex = columnMetadata.getFieldNamesDictionary().getFieldNameIndex(fieldName);
            collapseDetector.onChildGrowing(fieldNameIndexToChildIndexMap.get(fieldNameIndex), columnMetadata);
        }
    }

    /** Called when a batch closes. */
    public void resetBatchStatistics() {
        if (detector != null) {
            detector.resetBatchStatistics();
        }
    }

    /**
     * Whether this node has shown a key set growing: it collapsed, kept minting names after its first visit, or has a
     * child that grew. A record mints its fields on its first visit and few after; a node keyed by event ID keeps
     * minting on every one. Weaker evidence than a collapse -- a record whose optional fields trickle in also counts --
     * but a node visited by only a few dozen documents never sees enough windows to collapse. The child case carries
     * growth up through a fixed field: a day holding its event IDs under {@code events} mints no names of its own.
     * Without a detector, only what the schema records: a collapse at or under this node.
     */
    public boolean isGrowing() {
        if (isCollapsed()) {
            return true;
        }
        if (detector != null) {
            return detector.isGrowing();
        }
        for (AbstractSchemaNode child : children) {
            if (child instanceof ObjectSchemaNode object && object.isGrowing()) {
                return true;
            }
        }
        return false;
    }

    /** Called by the detector once it decides this node collapses. */
    void markCollapsed(FlushColumnMetadata columnMetadata, String trigger) {
        collapsePending = true;
        detector = null;
        columnMetadata.onRoutingChanged();
        // Logged above trace: a collapse is permanent, and changes the node's storage format. Key names are user data.
        LOGGER.info("collapsing an object node with {} children ({} trigger); names first seen from now on route to "
                + "its map", children.size(), trigger);
    }

    /**
     * @return true when this field must be written through the map rather than getting a column of its own. Resolved
     *         without interning the name, so map keys never enter the field-name dictionary.
     */
    public boolean routesToMap(IValueReference fieldName, FlushColumnMetadata columnMetadata)
            throws HyracksDataException {
        if (!isCollapsed()) {
            return false;
        }
        int fieldNameIndex = columnMetadata.getFieldNamesDictionary().getFieldNameIndex(fieldName);
        return fieldNameIndex < 0 || !fieldNameIndexToChildIndexMap.containsKey(fieldNameIndex);
    }

    public void addChild(int fieldNameIndex, AbstractSchemaNode child) {
        int childIndex = children.size();
        fieldNameIndexToChildIndexMap.put(fieldNameIndex, childIndex);
        children.add(child);
        if (fieldNameIndex == MAP_FIELD_NAME_INDEX) {
            // Keeps the collapsed-child cache true of any node that has one, including the clipped copies the
            // clipper builds -- those are assembled through addChild and never deserialized, so without this
            // isCollapsed() reads false for them and the assembler's delegate rule silently stops applying.
            cacheCollapsedChild(child);
        }
    }

    public AbstractSchemaNode setEmptyObject(FlushColumnMetadata columnMetadata) throws HyracksDataException {
        if (!children.isEmpty()) {
            return null;
        }

        AbstractSchemaNode emptyChild = columnMetadata.getOrCreateChild(null, ATypeTag.MISSING, false);
        addChild(DUMMY_FIELD_NAME_INDEX, emptyChild);
        nextIndex = this::emptyColumnIndex;
        return emptyChild;
    }

    public boolean isEmptyMissingObject() {
        return children.size() == 1 && fieldNameIndexToChildIndexMap.containsKey(DUMMY_FIELD_NAME_INDEX);
    }

    public AbstractSchemaNode getChild(int fieldNameIndex) {
        if (fieldNameIndexToChildIndexMap.containsKey(fieldNameIndex)) {
            return children.get(fieldNameIndexToChildIndexMap.get(fieldNameIndex));
        }
        return MissingFieldSchemaNode.INSTANCE;
    }

    public List<AbstractSchemaNode> getChildren() {
        return children;
    }

    /**
     * Should not be used in a {@link CriticalPath}
     */
    public IntList getChildrenFieldNameIndexes() {
        return IntImmutableList.toList(fieldNameIndexToChildIndexMap.int2IntEntrySet().stream()
                .sorted(Comparator.comparingInt(Entry::getIntValue)).mapToInt(Entry::getIntKey));
    }

    @Override
    public ATypeTag getTypeTag() {
        return ATypeTag.OBJECT;
    }

    @Override
    public boolean isObjectOrCollection() {
        return true;
    }

    @Override
    public boolean isCollection() {
        return false;
    }

    @Override
    public <R, T> R accept(ISchemaNodeVisitor<R, T> visitor, T arg) throws HyracksDataException {
        return visitor.visit(this, arg);
    }

    @Override
    public void serialize(DataOutput output, PathInfoSerializer pathInfoSerializer) throws IOException {
        output.write(ATypeTag.OBJECT.serialize());
        output.writeInt(children.size());
        for (Int2IntMap.Entry fieldNameIndexChildIndex : fieldNameIndexToChildIndexMap.int2IntEntrySet()) {
            int fieldNameIndex = fieldNameIndexChildIndex.getIntKey();
            output.writeInt(fieldNameIndex);
            output.writeInt(fieldNameIndexChildIndex.getIntValue());
        }
        pathInfoSerializer.enter(this);
        for (AbstractSchemaNode child : children) {
            child.serialize(output, pathInfoSerializer);
        }
        pathInfoSerializer.exit(this);
    }

    public void abort(DataInputStream input, Map<AbstractSchemaNestedNode, RunLengthIntArray> definitionLevels)
            throws IOException {
        input.readByte(); // Skip the type tag, see ObjectSchemaNode#serialize
        if (definitionLevels != null) {
            definitionLevels.put(this, new RunLengthIntArray());
        }

        int numberOfChildren = input.readInt();

        fieldNameIndexToChildIndexMap.clear();
        deserializeFieldNameIndexToChildIndex(input, fieldNameIndexToChildIndexMap, numberOfChildren);

        children.clear();
        deserializeChildren(input, children, numberOfChildren, definitionLevels);
        restoreCollapsedChild();
        collapsePending = false;
        detector = null;
    }

    /** Restores the collapsed-child cache after the children have been read back. */
    private void restoreCollapsedChild() {
        mapNode = null;
        if (!fieldNameIndexToChildIndexMap.containsKey(MAP_FIELD_NAME_INDEX)) {
            return;
        }
        cacheCollapsedChild(children.get(fieldNameIndexToChildIndexMap.get(MAP_FIELD_NAME_INDEX)));
    }

    /** Points the collapsed-child cache at {@code child}. */
    private void cacheCollapsedChild(AbstractSchemaNode child) {
        mapNode = (MapSchemaNode) child;
    }

    private static void deserializeFieldNameIndexToChildIndex(DataInput input, Int2IntMap fieldNameIndexToChildIndexMap,
            int numberOfChildren) throws IOException {
        for (int i = 0; i < numberOfChildren; i++) {
            int fieldNameIndex = input.readInt();
            int childIndex = input.readInt();
            fieldNameIndexToChildIndexMap.put(fieldNameIndex, childIndex);
        }
    }

    private static int deserializeChildren(DataInput input, List<AbstractSchemaNode> children, int numberOfChildren,
            Map<AbstractSchemaNestedNode, RunLengthIntArray> definitionLevels) throws IOException {
        int numberOfColumns = 0;
        for (int i = 0; i < numberOfChildren; i++) {
            AbstractSchemaNode child = AbstractSchemaNode.deserialize(input, definitionLevels);
            numberOfColumns += child.getNumberOfColumns();
            children.add(child);
        }
        return numberOfColumns;
    }

    private int nextIndex(int fieldNameIndex) {
        return children.size();
    }

    private int emptyColumnIndex(int fieldNameIndex) {
        nextIndex = this::nextIndex;
        fieldNameIndexToChildIndexMap.remove(DUMMY_FIELD_NAME_INDEX);
        fieldNameIndexToChildIndexMap.put(fieldNameIndex, 0);
        return 0;
    }
}
