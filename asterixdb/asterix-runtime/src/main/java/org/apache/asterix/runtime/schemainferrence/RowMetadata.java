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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.om.api.IRowWriteMultiPageOp;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.utils.RowValuesUtil;
import org.apache.asterix.om.utils.RunRowLengthIntArray;
import org.apache.asterix.runtime.schemainferrence.collection.AbstractRowCollectionSchemaNode;
import org.apache.asterix.runtime.schemainferrence.collection.ArrayRowSchemaNode;
import org.apache.asterix.runtime.schemainferrence.collection.MultisetRowSchemaNode;
import org.apache.asterix.runtime.schemainferrence.lazy.metadata.AbstractRowMetadata;
import org.apache.asterix.runtime.schemainferrence.lazy.metadata.PathRowInfoSerializer;
import org.apache.asterix.runtime.schemainferrence.lazy.metadata.RowFieldNamesDictionary;
import org.apache.asterix.runtime.schemainferrence.primitive.PrimitiveRowSchemaNode;
import org.apache.asterix.runtime.schemainferrence.utils.JSONRowSchemaStringBuilderVisitor;
import org.apache.asterix.runtime.schemainferrence.utils.RowSchemaStringBuilderVisitor;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.util.LogRedactionUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.ints.IntArrayList;

/**
 * The schema here is mutable and can change according to the flushed records
 */
public final class RowMetadata extends AbstractRowMetadata {
    private static final Logger LOGGER = LogManager.getLogger();
    private final Map<AbstractRowSchemaNestedNode, RunRowLengthIntArray> definitionLevels;
    private final Mutable<IRowWriteMultiPageOp> multiPageOpRef;
    private final RowFieldNamesDictionary fieldNamesDictionary;
    private final ObjectRowSchemaNode root;
    private final ObjectRowSchemaNode metaRoot;

    private int sizeOfWriters = 0;
    private final ArrayBackedValueStorage serializedMetadata;
    private final PathRowInfoSerializer pathInfoSerializer;
    private final IntArrayList nullWriterIndexes;
    private boolean changed;
    private int level;

    public RowMetadata(Mutable<IRowWriteMultiPageOp> multiPageOpRef) throws HyracksDataException {
        super();
        this.multiPageOpRef = multiPageOpRef;
        definitionLevels = new HashMap<>();
        level = -1;
        fieldNamesDictionary = new RowFieldNamesDictionary();
        ArrayBackedValueStorage initFieldName = new ArrayBackedValueStorage(1);
        root = new ObjectRowSchemaNode(initFieldName);
        metaRoot = null;
        pathInfoSerializer = new PathRowInfoSerializer();
        nullWriterIndexes = new IntArrayList();
        //Add definition levels for the root
        addDefinitionLevelsAndGet(root);
        serializedMetadata = new ArrayBackedValueStorage();
        changed = true;
        serializeColumnsMetadata();
    }

    public RowFieldNamesDictionary getFieldNamesDictionary() {
        return fieldNamesDictionary;
    }

    public ObjectRowSchemaNode getRoot() {
        return root;
    }

    public ObjectRowSchemaNode getMetaRoot() {
        return metaRoot;
    }

    public Mutable<IRowWriteMultiPageOp> getMultiPageOpRef() {
        return multiPageOpRef;
    }

    // Convert Schema to byte array (IValueReference)
    @Override
    public IValueReference serializeColumnsMetadata() throws HyracksDataException {
        if (changed) {
            try {
                serializeChanges();
                //                logSchema(root, metaRoot, fieldNamesDictionary);
                changed = false;
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
        }
        return serializedMetadata;
    }

    // Print the schema structure to the console  for debugging purposes
    public String printRootSchema(ObjectRowSchemaNode rot, RowFieldNamesDictionary fieldNmDic)
            throws HyracksDataException {
        return logSchema(rot, metaRoot, fieldNmDic);
    }

    // Structure of writing the metadata of schema to the byte Array
    private void serializeChanges() throws IOException {
        serializedMetadata.reset();
        DataOutput output = serializedMetadata.getDataOutput();

        int writersOffsetPointer = reserveInt(output);//0
        int fieldNamesOffsetPointer = reserveInt(output);//4
        int schemaOffsetPointer = reserveInt(output);//12
        int metaSchemaOffsetPointer = reserveInt(output);
        int pathInfoOffsetPointer = reserveInt(output);
        int lengthDataOffsetPointer = reserveInt(output);

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
        // Length info
        setOffset(lengthDataOffsetPointer);
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
        changed = false;

        fieldNamesDictionary.abort(input);
        definitionLevels.clear();
        root.abort(input, definitionLevels);
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
        return this.sizeOfWriters;
    }

    // Create schema node
    public AbstractRowSchemaNode getOrCreateChild(AbstractRowSchemaNode child, ATypeTag childTypeTag)
            throws HyracksDataException {
        AbstractRowSchemaNode currentChild = child;
        ATypeTag normalizedTypeTag = getNormalizedTypeTag(childTypeTag);
        if (currentChild == null || normalizedTypeTag != ATypeTag.MISSING && normalizedTypeTag != ATypeTag.NULL
                && currentChild.getTypeTag() != ATypeTag.UNION && currentChild.getTypeTag() != normalizedTypeTag) {
            //Create a new child or union type if required type is different from the current child type
            currentChild = createChild(child, normalizedTypeTag);
            //Flag that the schema has changed
            changed = true;
        }
        return currentChild;
    }

    // Create schema node with fieldName
    public AbstractRowSchemaNode getOrCreateChild(AbstractRowSchemaNode child, ATypeTag childTypeTag,
            IValueReference fieldName) throws HyracksDataException {
        AbstractRowSchemaNode currentChild = child;
        ATypeTag normalizedTypeTag = getNormalizedTypeTag(childTypeTag);
        if (currentChild == null || normalizedTypeTag != ATypeTag.MISSING && normalizedTypeTag != ATypeTag.NULL
                && currentChild.getTypeTag() != ATypeTag.UNION && currentChild.getTypeTag() != normalizedTypeTag) {
            //Create a new child or union type if required type is different from the current child type
            currentChild = createChild(child, normalizedTypeTag, fieldName);
            //Flag that the schema has changed
            changed = true;
        }

        return currentChild;
    }

    public void enterLevel(AbstractRowSchemaNestedNode node) {
        level++;
    }

    public void exitLevel(AbstractRowSchemaNestedNode node) {
        level--;
    }

    // Update schema level by incrementing
    public void enterNode(AbstractRowSchemaNode parent, AbstractRowSchemaNode node) throws HyracksDataException {
        //Flush all definition levels from parent to child
        if (node.isObjectOrCollection()) {
            //Enter one more level for object, array, and multiset
            level++;
        }
    }

    // Update schema level by decrementing
    public void exitNode(AbstractRowSchemaNode node) {
        if (node.isNested()) {
            //Add the nested node's level for all missing children (i.e., not entered for a record)
            if (node.isObjectOrCollection()) {
                //Union nodes should not change the level as they are logical nodes
                level--;
            }
        }
        node.incrementCounter();
    }

    // Collection node level updated and set
    public void exitCollectionNode(AbstractRowCollectionSchemaNode collectionNode, int numberOfItems) {
        RunRowLengthIntArray collectionDefLevels = definitionLevels.get(collectionNode);
        //Add delimiter
        collectionDefLevels.add(level - 1);
        level--;
        collectionNode.incrementCounter();
    }

    /**
     * Needed by {@link AbstractRowCollectionSchemaNode} to add the definition level for each item
     *
     * @param collectionSchemaNode collection node
     * @return collection node's definition level
     */
    public RunRowLengthIntArray getDefinitionLevels(AbstractRowCollectionSchemaNode collectionSchemaNode) {
        return definitionLevels.get(collectionSchemaNode);
    }

    private void flushNestedDefinitionLevel(int parentMask, int childMask, int startIndex,
            RunRowLengthIntArray parentDefLevels, RunRowLengthIntArray childDefLevels) {
        if (parentDefLevels.getSize() == 0) {
            return;
        }
        //First, handle the first block as startIndex might be at the middle of a block
        //Get which block that startIndex resides
        int blockIndex = parentDefLevels.getBlockIndex(startIndex);
        //Get the remaining of the first block starting from startIndex
        int remainingValues = parentDefLevels.getBlockSize(blockIndex, startIndex);

        int firstBlockValue =
                RowValuesUtil.getChildValue(parentMask, childMask, parentDefLevels.getBlockValue(blockIndex));
        //Batch add all the remaining values
        childDefLevels.add(firstBlockValue, remainingValues);

        //Add other blocks as batches
        for (int i = blockIndex + 1; i < parentDefLevels.getNumberOfBlocks(); i++) {
            int blockValue = RowValuesUtil.getChildValue(parentMask, childMask, parentDefLevels.getBlockValue(i));
            childDefLevels.add(blockValue, parentDefLevels.getBlockSize(i));
        }
    }

    // Create union and primitive node with fieldName
    private AbstractRowSchemaNode createChild(AbstractRowSchemaNode child, ATypeTag normalizedTypeTag,
            IValueReference fieldName) throws HyracksDataException {
        AbstractRowSchemaNode createdChild;
        if (child != null) {
            if (child.getTypeTag() == ATypeTag.NULL) {
                createdChild = createChild(normalizedTypeTag, fieldName);
                System.out.println("TO BE REIMPLEMENTED WITH THIS CASE : CALVIN DANI");
            } else {
                //Different type. Make union
                createdChild = addDefinitionLevelsAndGet(
                        new UnionRowSchemaNode(child, createChild(normalizedTypeTag, fieldName)));
            }
        } else {
            createdChild = createChild(normalizedTypeTag, fieldName);
        }
        return createdChild;
    }

    // Create union and primitive, object, array and Multiset  node
    private AbstractRowSchemaNode createChild(AbstractRowSchemaNode child, ATypeTag normalizedTypeTag)
            throws HyracksDataException {
        AbstractRowSchemaNode createdChild;
        ArrayBackedValueStorage initFieldName = new ArrayBackedValueStorage();
        if (child != null) {
            if (child.getTypeTag() == ATypeTag.NULL) {
                createdChild = createChild(normalizedTypeTag, initFieldName);
                System.out.println("TO BE REIMPLEMENTED WITH THIS CASE : CALVIN DANI");
            } else {
                //Different type. Make union
                createdChild = addDefinitionLevelsAndGet(
                        new UnionRowSchemaNode(child, createChild(normalizedTypeTag, initFieldName)));
            }
        } else {
            createdChild = createChild(normalizedTypeTag, initFieldName);
        }
        return createdChild;
    }

    // Create object , array , multiset and primitive node
    private AbstractRowSchemaNode createChild(ATypeTag normalizedTypeTag, IValueReference fieldName)
            throws HyracksDataException {
        switch (normalizedTypeTag) {
            case OBJECT:
                return addDefinitionLevelsAndGet(new ObjectRowSchemaNode(fieldName));
            case ARRAY:
                return addDefinitionLevelsAndGet(new ArrayRowSchemaNode(fieldName));
            case MULTISET:
                return addDefinitionLevelsAndGet(new MultisetRowSchemaNode(fieldName));
            case NULL:
            case MISSING:
            case BOOLEAN:
            case DOUBLE:
            case BIGINT:
            case STRING:
            case UUID:
                int columnIndex = nullWriterIndexes.isEmpty() ? this.sizeOfWriters : nullWriterIndexes.removeInt(0);
                if (columnIndex == this.sizeOfWriters) {
                    this.sizeOfWriters += 1;
                }
                return new PrimitiveRowSchemaNode(columnIndex, normalizedTypeTag, false, fieldName);
            default:
                throw new IllegalStateException("Unsupported type " + normalizedTypeTag);

        }
    }

    // definition level set for schema level
    private AbstractRowSchemaNode addDefinitionLevelsAndGet(AbstractRowSchemaNestedNode nestedNode) {
        definitionLevels.put(nestedNode, new RunRowLengthIntArray());
        return nestedNode;
    }

    // Schema structure logger
    private static String logSchema(ObjectRowSchemaNode root, ObjectRowSchemaNode metaRoot,
            RowFieldNamesDictionary fieldNamesDictionary) throws HyracksDataException {

        JSONRowSchemaStringBuilderVisitor schemaBuilder = new JSONRowSchemaStringBuilderVisitor(fieldNamesDictionary);

        String recordSchema = LogRedactionUtil.userData(schemaBuilder.build(root));

        if (metaRoot != null) {
            String metaRecordSchema = LogRedactionUtil.userData(schemaBuilder.build(metaRoot));
            LOGGER.debug("Schema for {} has changed: \n {}", RowSchemaStringBuilderVisitor.META_RECORD_SCHEMA,
                    metaRecordSchema);
        }
        return recordSchema;
    }

    // Normalize field record type
    public static ATypeTag getNormalizedTypeTag(ATypeTag typeTag) {
        switch (typeTag) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                return ATypeTag.BIGINT;
            case FLOAT:
                return ATypeTag.DOUBLE;
            default:
                return typeTag;
        }
    }

    public void close() {
        //Dereference multiPageOp
        multiPageOpRef.setValue(null);
    }

    public void flushDefinitionLevels(int level, AbstractRowSchemaNestedNode parent, AbstractRowSchemaNode node)
            throws HyracksDataException {
        if (parent != null) {
            RunRowLengthIntArray parentDefLevels = definitionLevels.get(parent);
            if (node.getCounter() < parentDefLevels.getSize()) {
                int parentMask = RowValuesUtil.getNullMask(level);
                int childMask = RowValuesUtil.getNullMask(level + 1);
                flushDefinitionLevels(parentMask, childMask, parentDefLevels, node);
            }
        }
    }

    private void flushDefinitionLevels(int parentMask, int childMask, RunRowLengthIntArray parentDefLevels,
            AbstractRowSchemaNode node) throws HyracksDataException {
        int startIndex = node.getCounter();
        if (node.isNested()) {
            RunRowLengthIntArray childDefLevels = definitionLevels.get((AbstractRowSchemaNestedNode) node);
            flushNestedDefinitionLevel(parentMask, childMask, startIndex, parentDefLevels, childDefLevels);
        }
        node.setCounter(parentDefLevels.getSize());
    }

    public void addNestedNull(AbstractRowSchemaNestedNode parent, AbstractRowSchemaNestedNode node)
            throws HyracksDataException {
        //Flush all definition levels from parent to the current node
        flushDefinitionLevels(level, parent, node);
        //Add null value (+2) to say that both the parent and the child are present
        definitionLevels.get(node).add(RowValuesUtil.getNullMask(level + 2) | level);
        node.incrementCounter();
    }
}
