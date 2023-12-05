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

import static org.apache.asterix.column.util.ColumnValuesUtil.getNormalizedTypeTag;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.column.metadata.AbstractColumnMetadata;
import org.apache.asterix.column.metadata.FieldNamesDictionary;
import org.apache.asterix.column.metadata.PathInfoSerializer;
import org.apache.asterix.column.metadata.schema.AbstractSchemaNestedNode;
import org.apache.asterix.column.metadata.schema.AbstractSchemaNode;
import org.apache.asterix.column.metadata.schema.ObjectSchemaNode;
import org.apache.asterix.column.metadata.schema.UnionSchemaNode;
import org.apache.asterix.column.metadata.schema.collection.AbstractCollectionSchemaNode;
import org.apache.asterix.column.metadata.schema.collection.ArraySchemaNode;
import org.apache.asterix.column.metadata.schema.collection.MultisetSchemaNode;
import org.apache.asterix.column.metadata.schema.primitive.PrimitiveSchemaNode;
import org.apache.asterix.column.metadata.schema.visitor.SchemaBuilderFromIATypeVisitor;
import org.apache.asterix.column.util.ColumnValuesUtil;
import org.apache.asterix.column.util.RunLengthIntArray;
import org.apache.asterix.column.util.SchemaStringBuilderVisitor;
import org.apache.asterix.column.values.IColumnValuesWriter;
import org.apache.asterix.column.values.IColumnValuesWriterFactory;
import org.apache.asterix.column.values.writer.AbstractColumnValuesWriter;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;
import org.apache.hyracks.util.LogRedactionUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.ints.IntArrayList;

/**
 * Flush column metadata belongs to a flushing {@link ILSMMemoryComponent}
 * The schema here is mutable and can change according to the flushed records
 */
public final class FlushColumnMetadata extends AbstractColumnMetadata {
    private static final Logger LOGGER = LogManager.getLogger();
    private final Map<AbstractSchemaNestedNode, RunLengthIntArray> definitionLevels;
    private final Mutable<IColumnWriteMultiPageOp> multiPageOpRef;
    private final FieldNamesDictionary fieldNamesDictionary;
    private final ObjectSchemaNode root;
    private final ObjectSchemaNode metaRoot;
    private final IColumnValuesWriterFactory columnWriterFactory;
    private final List<IColumnValuesWriter> columnWriters;
    private final ArrayBackedValueStorage serializedMetadata;
    private final PathInfoSerializer pathInfoSerializer;
    private final IntArrayList nullWriterIndexes;
    private final boolean metaContainsKeys;
    private boolean changed;
    private int level;
    private int repeated;

    public FlushColumnMetadata(ARecordType datasetType, ARecordType metaType, List<List<String>> primaryKeys,
            List<Integer> keySourceIndicator, IColumnValuesWriterFactory columnWriterFactory,
            Mutable<IColumnWriteMultiPageOp> multiPageOpRef) throws HyracksDataException {
        super(datasetType, metaType, primaryKeys.size());
        this.multiPageOpRef = multiPageOpRef;
        this.columnWriterFactory = columnWriterFactory;
        definitionLevels = new HashMap<>();
        columnWriters = new ArrayList<>();
        level = -1;
        repeated = 0;
        fieldNamesDictionary = new FieldNamesDictionary();
        root = new ObjectSchemaNode();
        metaRoot = metaType != null ? new ObjectSchemaNode() : null;
        pathInfoSerializer = new PathInfoSerializer();
        nullWriterIndexes = new IntArrayList();
        //Add definition levels for the root
        addDefinitionLevelsAndGet(root);
        SchemaBuilderFromIATypeVisitor builder = new SchemaBuilderFromIATypeVisitor(this, primaryKeys);
        //Ensure all primary keys take the first column indexes
        metaContainsKeys = metaType != null && keySourceIndicator.get(0) == 1;
        if (metaContainsKeys) {
            addDefinitionLevelsAndGet(metaRoot);
            metaType.accept(builder, metaRoot);
            datasetType.accept(builder, root);
        } else {
            datasetType.accept(builder, root);
            if (metaRoot != null) {
                addDefinitionLevelsAndGet(metaRoot);
                metaType.accept(builder, metaRoot);
            }
        }

        serializedMetadata = new ArrayBackedValueStorage();
        changed = true;
        serializeColumnsMetadata();
    }

    private FlushColumnMetadata(ARecordType datasetType, ARecordType metaType, List<List<String>> primaryKeys,
            boolean metaContainsKeys, IColumnValuesWriterFactory columnWriterFactory,
            Mutable<IColumnWriteMultiPageOp> multiPageOpRef, List<IColumnValuesWriter> columnWriters,
            FieldNamesDictionary fieldNamesDictionary, ObjectSchemaNode root, ObjectSchemaNode metaRoot,
            Map<AbstractSchemaNestedNode, RunLengthIntArray> definitionLevels,
            ArrayBackedValueStorage serializedMetadata) {
        super(datasetType, metaType, primaryKeys.size());
        this.multiPageOpRef = multiPageOpRef;
        this.columnWriterFactory = columnWriterFactory;
        this.definitionLevels = definitionLevels;
        this.columnWriters = columnWriters;
        level = -1;
        repeated = 0;
        this.fieldNamesDictionary = fieldNamesDictionary;
        this.root = root;
        this.metaRoot = metaRoot;
        this.metaContainsKeys = metaContainsKeys;
        pathInfoSerializer = new PathInfoSerializer();
        nullWriterIndexes = new IntArrayList();
        //Add definition levels for the root
        addDefinitionLevelsAndGet(root);
        this.serializedMetadata = serializedMetadata;
        changed = false;
    }

    public FieldNamesDictionary getFieldNamesDictionary() {
        return fieldNamesDictionary;
    }

    public ObjectSchemaNode getRoot() {
        return root;
    }

    public ObjectSchemaNode getMetaRoot() {
        return metaRoot;
    }

    public Mutable<IColumnWriteMultiPageOp> getMultiPageOpRef() {
        return multiPageOpRef;
    }

    @Override
    public IValueReference serializeColumnsMetadata() throws HyracksDataException {
        if (changed) {
            try {
                serializeChanges();
                logSchema(root, metaRoot, fieldNamesDictionary);
                changed = false;
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
        }
        return serializedMetadata;
    }

    private void serializeChanges() throws IOException {
        serializedMetadata.reset();
        DataOutput output = serializedMetadata.getDataOutput();

        int writersOffsetPointer = reserveInt(output);
        int fieldNamesOffsetPointer = reserveInt(output);
        int schemaOffsetPointer = reserveInt(output);
        int metaSchemaOffsetPointer = reserveInt(output);
        int pathInfoOffsetPointer = reserveInt(output);

        //ColumnWriterInformation
        setOffset(writersOffsetPointer);
        output.writeInt(columnWriters.size());
        for (IColumnValuesWriter writer : columnWriters) {
            writer.serialize(output);
        }

        //FieldNames
        setOffset(fieldNamesOffsetPointer);
        fieldNamesDictionary.serialize(output);

        //Schema
        pathInfoSerializer.reset();
        setOffset(schemaOffsetPointer);
        root.serialize(output, pathInfoSerializer);
        if (metaRoot != null) {
            //Meta schema
            setOffset(metaSchemaOffsetPointer);
            metaRoot.serialize(output, pathInfoSerializer);
        }

        //Path info
        setOffset(pathInfoOffsetPointer);
        pathInfoSerializer.serialize(output, getNumberOfColumns());
    }

    private int reserveInt(DataOutput output) throws IOException {
        int offset = serializedMetadata.getLength();
        output.writeInt(-1);
        return offset;
    }

    private void setOffset(int pointer) {
        int offset = serializedMetadata.getLength();
        IntegerPointable.setInteger(serializedMetadata.getByteArray(), pointer, offset);
    }

    public static FlushColumnMetadata create(ARecordType datasetType, ARecordType metaType,
            List<List<String>> primaryKeys, List<Integer> keySourceIndicator,
            IColumnValuesWriterFactory columnWriterFactory, Mutable<IColumnWriteMultiPageOp> multiPageOpRef,
            IValueReference serializedMetadata) throws HyracksDataException {
        boolean metaContainsKeys = metaType != null && keySourceIndicator.get(0) == 1;
        try {
            return createMutableMetadata(datasetType, metaType, primaryKeys, metaContainsKeys, columnWriterFactory,
                    multiPageOpRef, serializedMetadata);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    private static FlushColumnMetadata createMutableMetadata(ARecordType datasetType, ARecordType metaType,
            List<List<String>> primaryKeys, boolean metaContainsKeys, IColumnValuesWriterFactory columnWriterFactory,
            Mutable<IColumnWriteMultiPageOp> multiPageOpRef, IValueReference serializedMetadata) throws IOException {
        DataInput input = new DataInputStream(new ByteArrayInputStream(serializedMetadata.getByteArray(),
                serializedMetadata.getStartOffset(), serializedMetadata.getLength()));
        //Skip offsets
        input.skipBytes(OFFSETS_SIZE);

        //ColumnWriter
        List<IColumnValuesWriter> writers = new ArrayList<>();
        deserializeWriters(input, writers, columnWriterFactory);

        //FieldNames
        FieldNamesDictionary fieldNamesDictionary = FieldNamesDictionary.deserialize(input);

        //Schema
        Map<AbstractSchemaNestedNode, RunLengthIntArray> definitionLevels = new HashMap<>();
        ObjectSchemaNode root = (ObjectSchemaNode) AbstractSchemaNode.deserialize(input, definitionLevels);
        ObjectSchemaNode metaRoot = null;
        if (metaType != null) {
            metaRoot = (ObjectSchemaNode) AbstractSchemaNode.deserialize(input, definitionLevels);
        }

        ArrayBackedValueStorage schemaStorage = new ArrayBackedValueStorage(serializedMetadata.getLength());
        schemaStorage.append(serializedMetadata);
        logSchema(root, metaRoot, fieldNamesDictionary);
        return new FlushColumnMetadata(datasetType, metaType, primaryKeys, metaContainsKeys, columnWriterFactory,
                multiPageOpRef, writers, fieldNamesDictionary, root, metaRoot, definitionLevels, schemaStorage);
    }

    @Override
    public void abort() throws HyracksDataException {
        DataInputStream input = new DataInputStream(new ByteArrayInputStream(serializedMetadata.getByteArray()));
        try {
            abort(input);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    private void abort(DataInputStream input) throws IOException {
        level = -1;
        repeated = 0;
        changed = false;

        columnWriters.clear();
        deserializeWriters(input, columnWriters, columnWriterFactory);

        fieldNamesDictionary.abort(input);
        definitionLevels.clear();
        root.abort(input, definitionLevels);
    }

    public static void deserializeWriters(DataInput input, List<IColumnValuesWriter> writers,
            IColumnValuesWriterFactory columnWriterFactory) throws IOException {
        int numberOfWriters = input.readInt();
        for (int i = 0; i < numberOfWriters; i++) {
            writers.add(AbstractColumnValuesWriter.deserialize(input, columnWriterFactory));
        }
    }

    /* ********************************************************
     * Column values related methods
     * ********************************************************
     */

    /**
     * Set {@link IColumnWriteMultiPageOp} for {@link IColumnValuesWriter}
     *
     * @param multiPageOp multi-buffer allocator
     */
    public void init(IColumnWriteMultiPageOp multiPageOp) throws HyracksDataException {
        multiPageOpRef.setValue(multiPageOp);

        //Reset writer for the first write
        for (int i = 0; i < columnWriters.size(); i++) {
            columnWriters.get(i).reset();
        }
    }

    public IColumnValuesWriter getWriter(int columnIndex) {
        return columnWriters.get(columnIndex);
    }

    /* ********************************************************
     * Schema related methods
     * ********************************************************
     */

    public int getLevel() {
        return level;
    }

    @Override
    public int getNumberOfColumns() {
        return columnWriters.size();
    }

    public AbstractSchemaNode getOrCreateChild(AbstractSchemaNode child, ATypeTag childTypeTag)
            throws HyracksDataException {
        AbstractSchemaNode currentChild = child;
        ATypeTag normalizedTypeTag = getNormalizedTypeTag(childTypeTag);
        if (currentChild == null || normalizedTypeTag != ATypeTag.MISSING && normalizedTypeTag != ATypeTag.NULL
                && currentChild.getTypeTag() != ATypeTag.UNION
                && getNormalizedTypeTag(currentChild.getTypeTag()) != normalizedTypeTag) {
            //Create a new child or union type if required type is different from the current child type
            currentChild = createChild(child, childTypeTag);
            //Flag that the schema has changed
            changed = true;
        }
        return currentChild;
    }

    public void enterLevel(AbstractSchemaNestedNode node) {
        level++;
        if (node.isCollection()) {
            repeated++;
        }
    }

    public void exitLevel(AbstractSchemaNestedNode node) {
        level--;
        if (node.isCollection()) {
            repeated--;
        }
    }

    public void enterNode(AbstractSchemaNestedNode parent, AbstractSchemaNode node) throws HyracksDataException {
        //Flush all definition levels from parent to child
        flushDefinitionLevels(level, parent, node);
        if (node.isObjectOrCollection()) {
            //Enter one more level for object, array, and multiset
            level++;
            if (node.isCollection()) {
                //Tells nested values that they are repeated
                repeated++;
            }
        }
    }

    public void exitNode(AbstractSchemaNode node) {
        if (node.isNested()) {
            //Add the nested node's level for all missing children (i.e., not entered for a record)
            definitionLevels.get((AbstractSchemaNestedNode) node).add(level);
            if (node.isObjectOrCollection()) {
                //Union nodes should not change the level as they are logical nodes
                level--;
            }
        }
        node.incrementCounter();
    }

    public void exitCollectionNode(AbstractCollectionSchemaNode collectionNode, int numberOfItems) {
        RunLengthIntArray collectionDefLevels = definitionLevels.get(collectionNode);
        //Add delimiter
        collectionDefLevels.add(level - 1);
        level--;
        repeated--;
        collectionNode.incrementCounter();
    }

    /**
     * Needed by {@link AbstractCollectionSchemaNode} to add the definition level for each item
     *
     * @param collectionSchemaNode collection node
     * @return collection node's definition level
     */
    public RunLengthIntArray getDefinitionLevels(AbstractCollectionSchemaNode collectionSchemaNode) {
        return definitionLevels.get(collectionSchemaNode);
    }

    public void clearDefinitionLevels(AbstractSchemaNestedNode nestedNode) {
        definitionLevels.get(nestedNode).reset();
    }

    public void flushDefinitionLevels(int level, AbstractSchemaNestedNode parent, AbstractSchemaNode node)
            throws HyracksDataException {
        if (parent != null) {
            RunLengthIntArray parentDefLevels = definitionLevels.get(parent);
            if (node.getCounter() < parentDefLevels.getSize()) {
                int parentMask = ColumnValuesUtil.getNullMask(level);
                int childMask = ColumnValuesUtil.getNullMask(level + 1);
                flushDefinitionLevels(parentMask, childMask, parentDefLevels, node);
            }
        }
    }

    public void addNestedNull(AbstractSchemaNestedNode parent, AbstractSchemaNestedNode node)
            throws HyracksDataException {
        //Flush all definition levels from parent to the current node
        flushDefinitionLevels(level, parent, node);
        //Add null value (+2) to say that both the parent and the child are present
        definitionLevels.get(node).add(ColumnValuesUtil.getNullMask(level + 2) | level);
        node.incrementCounter();
    }

    public void close() {
        //Dereference multiPageOp
        multiPageOpRef.setValue(null);
        for (int i = 0; i < columnWriters.size(); i++) {
            columnWriters.get(i).close();
        }
    }

    private void flushDefinitionLevels(int parentMask, int childMask, RunLengthIntArray parentDefLevels,
            AbstractSchemaNode node) throws HyracksDataException {
        int startIndex = node.getCounter();
        if (node.isNested()) {
            RunLengthIntArray childDefLevels = definitionLevels.get((AbstractSchemaNestedNode) node);
            flushNestedDefinitionLevel(parentMask, childMask, startIndex, parentDefLevels, childDefLevels);
        } else {
            IColumnValuesWriter writer = columnWriters.get(((PrimitiveSchemaNode) node).getColumnIndex());
            flushWriterDefinitionLevels(parentMask, childMask, startIndex, parentDefLevels, writer);
        }
        node.setCounter(parentDefLevels.getSize());
    }

    private void flushNestedDefinitionLevel(int parentMask, int childMask, int startIndex,
            RunLengthIntArray parentDefLevels, RunLengthIntArray childDefLevels) {
        if (parentDefLevels.getSize() == 0) {
            return;
        }
        //First, handle the first block as startIndex might be at the middle of a block
        //Get which block that startIndex resides
        int blockIndex = parentDefLevels.getBlockIndex(startIndex);
        //Get the remaining of the first block starting from startIndex
        int remainingValues = parentDefLevels.getBlockSize(blockIndex, startIndex);

        int firstBlockValue =
                ColumnValuesUtil.getChildValue(parentMask, childMask, parentDefLevels.getBlockValue(blockIndex));
        //Batch add all the remaining values
        childDefLevels.add(firstBlockValue, remainingValues);

        //Add other blocks as batches
        for (int i = blockIndex + 1; i < parentDefLevels.getNumberOfBlocks(); i++) {
            int blockValue = ColumnValuesUtil.getChildValue(parentMask, childMask, parentDefLevels.getBlockValue(i));
            childDefLevels.add(blockValue, parentDefLevels.getBlockSize(i));
        }
    }

    private void flushWriterDefinitionLevels(int parentMask, int childMask, int startIndex,
            RunLengthIntArray parentDefLevels, IColumnValuesWriter writer) throws HyracksDataException {
        if (parentDefLevels.getSize() == 0) {
            return;
        }
        /*
         * We might need only a fraction of the first block. Hence, we first determine how many definition level
         * values we need. Then, we write those definition levels.
         */
        int blockIndex = parentDefLevels.getBlockIndex(startIndex);
        int remainingValues = parentDefLevels.getBlockSize(blockIndex, startIndex);
        int firstBlockValue =
                ColumnValuesUtil.getChildValue(parentMask, childMask, parentDefLevels.getBlockValue(blockIndex));
        writer.writeLevels(firstBlockValue, remainingValues);

        //Write remaining definition levels from the remaining blocks
        for (int i = blockIndex + 1; i < parentDefLevels.getNumberOfBlocks(); i++) {
            int blockValue = ColumnValuesUtil.getChildValue(parentMask, childMask, parentDefLevels.getBlockValue(i));
            writer.writeLevels(blockValue, parentDefLevels.getBlockSize(i));
        }
    }

    private AbstractSchemaNode createChild(AbstractSchemaNode child, ATypeTag childTypeTag)
            throws HyracksDataException {
        AbstractSchemaNode createdChild;
        ATypeTag normalizedTypeTag = getNormalizedTypeTag(childTypeTag);
        if (child != null) {
            if (child.getTypeTag() == ATypeTag.NULL) {
                //The previous child was a NULL. The new child needs to inherit the NULL definition levels
                int columnIndex = ((PrimitiveSchemaNode) child).getColumnIndex();
                RunLengthIntArray defLevels = columnWriters.get(columnIndex).getDefinitionLevelsIntArray();
                //Add the column index to be garbage collected
                nullWriterIndexes.add(columnIndex);
                createdChild = createChild(normalizedTypeTag);
                int mask = ColumnValuesUtil.getNullMask(level);
                flushDefinitionLevels(mask, mask, defLevels, createdChild);
            } else {
                //Different type. Make union
                createdChild = addDefinitionLevelsAndGet(new UnionSchemaNode(child, createChild(normalizedTypeTag)));
            }
        } else {
            createdChild = createChild(childTypeTag);
        }
        return createdChild;
    }

    private AbstractSchemaNode createChild(ATypeTag childTypeTag) throws HyracksDataException {
        switch (childTypeTag) {
            case OBJECT:
                return addDefinitionLevelsAndGet(new ObjectSchemaNode());
            case ARRAY:
                return addDefinitionLevelsAndGet(new ArraySchemaNode());
            case MULTISET:
                return addDefinitionLevelsAndGet(new MultisetSchemaNode());
            case NULL:
            case MISSING:
            case BOOLEAN:
            case FLOAT:
            case DOUBLE:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case STRING:
            case UUID:
                int columnIndex = nullWriterIndexes.isEmpty() ? columnWriters.size() : nullWriterIndexes.removeInt(0);
                boolean primaryKey = columnIndex < getNumberOfPrimaryKeys();
                ATypeTag normalizedTypeTag = primaryKey ? childTypeTag : getNormalizedTypeTag(childTypeTag);
                boolean writeAlways = primaryKey || repeated > 0;
                boolean filtered = !primaryKey;
                int maxLevel = primaryKey ? 1 : level + 1;
                IColumnValuesWriter writer = columnWriterFactory.createValueWriter(normalizedTypeTag, columnIndex,
                        maxLevel, writeAlways, filtered);
                if (multiPageOpRef.getValue() != null) {
                    writer.reset();
                }
                addColumn(columnIndex, writer);
                return new PrimitiveSchemaNode(columnIndex, normalizedTypeTag, primaryKey);
            default:
                throw new IllegalStateException("Unsupported type " + childTypeTag);

        }
    }

    private void addColumn(int index, IColumnValuesWriter writer) {
        if (index == columnWriters.size()) {
            columnWriters.add(writer);
        } else {
            columnWriters.set(index, writer);
        }
    }

    private AbstractSchemaNode addDefinitionLevelsAndGet(AbstractSchemaNestedNode nestedNode) {
        definitionLevels.put(nestedNode, new RunLengthIntArray());
        return nestedNode;
    }

    private static void logSchema(ObjectSchemaNode root, ObjectSchemaNode metaRoot,
            FieldNamesDictionary fieldNamesDictionary) throws HyracksDataException {
        if (!LOGGER.isDebugEnabled()) {
            return;
        }
        // This should be a low frequency object creation
        SchemaStringBuilderVisitor schemaBuilder = new SchemaStringBuilderVisitor(fieldNamesDictionary);
        String recordSchema = LogRedactionUtil.userData(schemaBuilder.build(root));
        LOGGER.debug("Schema for {} has changed: \n {}", SchemaStringBuilderVisitor.RECORD_SCHEMA, recordSchema);
        if (metaRoot != null) {
            String metaRecordSchema = LogRedactionUtil.userData(schemaBuilder.build(metaRoot));
            LOGGER.debug("Schema for {} has changed: \n {}", SchemaStringBuilderVisitor.META_RECORD_SCHEMA,
                    metaRecordSchema);
        }
    }
}
