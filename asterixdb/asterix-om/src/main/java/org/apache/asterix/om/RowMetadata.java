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
package org.apache.asterix.om;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.om.api.IRowWriteMultiPageOp;
import org.apache.asterix.om.lazy.metadata.AbstractRowMetadata;
import org.apache.asterix.om.lazy.metadata.PathRowInfoSerializer;
import org.apache.asterix.om.lazy.metadata.RowFieldNamesDictionary;
import org.apache.asterix.om.lazy.metadata.schema.AbstractRowSchemaNestedNode;
import org.apache.asterix.om.lazy.metadata.schema.AbstractRowSchemaNode;
import org.apache.asterix.om.lazy.metadata.schema.ObjectRowSchemaNode;
import org.apache.asterix.om.lazy.metadata.schema.UnionRowSchemaNode;
import org.apache.asterix.om.lazy.metadata.schema.collection.AbstractRowCollectionSchemaNode;
import org.apache.asterix.om.lazy.metadata.schema.collection.ArrayRowSchemaNode;
import org.apache.asterix.om.lazy.metadata.schema.collection.MultisetRowSchemaNode;
import org.apache.asterix.om.lazy.metadata.schema.primitive.PrimitiveRowSchemaNode;
import org.apache.asterix.om.lazy.metadata.schema.visitor.SchemaRowBuilderFromIATypeVisitor;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.utils.RowSchemaStringBuilderVisitor;
import org.apache.asterix.om.utils.RowValuesUtil;
import org.apache.asterix.om.utils.RunRowLengthIntArray;
import org.apache.asterix.om.values.IRowValuesWriter;
import org.apache.asterix.om.values.IRowValuesWriterFactory;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;
import org.apache.hyracks.util.LogRedactionUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// import org.apache.logging.log4j.LogManager;
// import org.apache.logging.log4j.Logger;
import it.unimi.dsi.fastutil.ints.IntArrayList;

/**
 * Flush column metadata belongs to a flushing {@link ILSMMemoryComponent}
 * The schema here is mutable and can change according to the flushed records
 */
public final class RowMetadata extends AbstractRowMetadata {
    private static final Logger LOGGER = LogManager.getLogger();
    private final Map<AbstractRowSchemaNestedNode, RunRowLengthIntArray> definitionLevels;
    private final Mutable<IRowWriteMultiPageOp> multiPageOpRef;
    private final RowFieldNamesDictionary fieldNamesDictionary;
    private final ObjectRowSchemaNode root;
    private final ObjectRowSchemaNode metaRoot;
//    private final IRowValuesWriterFactory columnWriterFactory;
//    private final List<IRowValuesWriter> columnWriters;

    private int sizeOfWriters = 0;
    private final ArrayBackedValueStorage serializedMetadata;
    private final PathRowInfoSerializer pathInfoSerializer;
    private final IntArrayList nullWriterIndexes;
//    private final boolean metaContainsKeys;
    private boolean changed;
    private int level;
    private int repeated;

    public RowMetadata(
            Mutable<IRowWriteMultiPageOp> multiPageOpRef) throws HyracksDataException {
        //    public RowMetadata(ARecordType datasetType, ARecordType metaType, List<List<String>> primaryKeys,
        //            List<Integer> keySourceIndicator, Mutable<IRowWriteMultiPageOp> multiPageOpRef) throws HyracksDataException {
        super();
        this.multiPageOpRef = multiPageOpRef;
//        this.columnWriterFactory = columnWriterFactory;
        definitionLevels = new HashMap<>();
//        columnWriters = new ArrayList<>();
        level = -1;
        repeated = 0;
        fieldNamesDictionary = new RowFieldNamesDictionary();
        ArrayBackedValueStorage initFieldName = new ArrayBackedValueStorage();
        root = new ObjectRowSchemaNode(initFieldName);
        metaRoot = null;
        pathInfoSerializer = new PathRowInfoSerializer();
        nullWriterIndexes = new IntArrayList();
        //Add definition levels for the root
        addDefinitionLevelsAndGet(root);
//        SchemaRowBuilderFromIATypeVisitor builder = new SchemaRowBuilderFromIATypeVisitor(this, primaryKeys);
        //Ensure all primary keys take the first column indexes
//        metaContainsKeys = metaType != null && keySourceIndicator.get(0) == 1;
//        if (metaContainsKeys) {
//            addDefinitionLevelsAndGet(metaRoot);
//            metaType.accept(builder, metaRoot);
//            datasetType.accept(builder, root);
//        } else {
//            datasetType.accept(builder, root);
//            if (metaRoot != null) {
//                addDefinitionLevelsAndGet(metaRoot);
//                metaType.accept(builder, metaRoot);
//            }
//        }

        serializedMetadata = new ArrayBackedValueStorage();
        changed = true;
        serializeColumnsMetadata();
    }

    private RowMetadata(ARecordType datasetType, ARecordType metaType, List<List<String>> primaryKeys,
            boolean metaContainsKeys,
            Mutable<IRowWriteMultiPageOp> multiPageOpRef,
            RowFieldNamesDictionary fieldNamesDictionary, ObjectRowSchemaNode root, ObjectRowSchemaNode metaRoot,
            Map<AbstractRowSchemaNestedNode, RunRowLengthIntArray> definitionLevels,
            ArrayBackedValueStorage serializedMetadata) {
        super();
        this.multiPageOpRef = multiPageOpRef;
//        this.columnWriterFactory = columnWriterFactory;
        this.definitionLevels = definitionLevels;
//        this.columnWriters = columnWriters;
        this.sizeOfWriters = 0;
//        this.sizeOfWriters = columnWriters.size();
        level = -1;
        repeated = 0;
        this.fieldNamesDictionary = fieldNamesDictionary;
        this.root = root;
        this.metaRoot = metaRoot;
//        this.metaContainsKeys = metaContainsKeys;
        pathInfoSerializer = new PathRowInfoSerializer();
        nullWriterIndexes = new IntArrayList();
        //Add definition levels for the root
        addDefinitionLevelsAndGet(root);
        this.serializedMetadata = serializedMetadata;
        changed = false;
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

    @Override
    public IValueReference serializeColumnsMetadata() throws HyracksDataException {
        if (changed) {
            try {
                //                serializeChanges();
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
        //        setOffset(writersOffsetPointer);
        //        output.writeInt(columnWriters.size());
        //        for (IRowValuesWriter writer : columnWriters) {
        //            writer.serialize(output);
        //        }

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

    //    public static RowMetadata create(ARecordType datasetType, ARecordType metaType, List<List<String>> primaryKeys,
    //            List<Integer> keySourceIndicator, IRowValuesWriterFactory columnWriterFactory,
    //            Mutable<IRowWriteMultiPageOp> multiPageOpRef, IValueReference serializedMetadata)
    //            throws HyracksDataException {
    //        boolean metaContainsKeys = metaType != null && keySourceIndicator.get(0) == 1;
    //        try {
    //            return createMutableMetadata(datasetType, metaType, primaryKeys, metaContainsKeys, columnWriterFactory,
    //                    multiPageOpRef, serializedMetadata);
    //        } catch (IOException e) {
    //            throw HyracksDataException.create(e);
    //        }
    //    }

    //    private static RowMetadata createMutableMetadata(ARecordType datasetType, ARecordType metaType,
    //                                                     List<List<String>> primaryKeys, boolean metaContainsKeys, IRowValuesWriterFactory columnWriterFactory,
    //                                                     Mutable<IRowWriteMultiPageOp> multiPageOpRef, IValueReference serializedMetadata) throws IOException {
    //    private static RowMetadata createMutableMetadata(ARecordType datasetType, ARecordType metaType,
    //            List<List<String>> primaryKeys, boolean metaContainsKeys,
    //            Mutable<IRowWriteMultiPageOp> multiPageOpRef, IValueReference serializedMetadata) throws IOException {
    //        DataInput input = new DataInputStream(new ByteArrayInputStream(serializedMetadata.getByteArray(),
    //                serializedMetadata.getStartOffset(), serializedMetadata.getLength()));
    //        //Skip offsets
    //        input.skipBytes(OFFSETS_SIZE);
    //
    //        //ColumnWriter
    //
    //        //FieldNames
    //        RowFieldNamesDictionary fieldNamesDictionary = RowFieldNamesDictionary.deserialize(input);
    //
    //        //Schema
    //        Map<AbstractRowSchemaNestedNode, RunRowLengthIntArray> definitionLevels = new HashMap<>();
    //        ObjectRowSchemaNode root = (ObjectRowSchemaNode) AbstractRowSchemaNode.deserialize(input, definitionLevels);
    //        ObjectRowSchemaNode metaRoot = null;
    //        if (metaType != null) {
    //            metaRoot = (ObjectRowSchemaNode) AbstractRowSchemaNode.deserialize(input, definitionLevels);
    //        }
    //
    //        ArrayBackedValueStorage schemaStorage = new ArrayBackedValueStorage(serializedMetadata.getLength());
    //        schemaStorage.append(serializedMetadata);
    //        logSchema(root, metaRoot, fieldNamesDictionary);
    ////        return new RowMetadata(datasetType, metaType, primaryKeys, metaContainsKeys, columnWriterFactory,
    ////                multiPageOpRef, writers, fieldNamesDictionary, root, metaRoot, definitionLevels, schemaStorage);
    //        return new RowMetadata(datasetType, metaType, primaryKeys, metaContainsKeys,
    //                multiPageOpRef, fieldNamesDictionary, root, metaRoot, definitionLevels, schemaStorage);
    //    }

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

        //        columnWriters.clear();
        //        deserializeWriters(input, columnWriters, columnWriterFactory);

        fieldNamesDictionary.abort(input);
        definitionLevels.clear();
        root.abort(input, definitionLevels);
    }

    //    public static void deserializeWriters(DataInput input, List<IRowValuesWriter> writers,
    //            IRowValuesWriterFactory columnWriterFactory) throws IOException {
    //        int numberOfWriters = input.readInt();
    //        for (int i = 0; i < numberOfWriters; i++) {
    //            writers.add(AbstractRowValuesWriter.deserialize(input, columnWriterFactory));
    //        }
    //    }

    //    /* ********************************************************
    //     * Column values related methods
    //     * ********************************************************
    //     */
    //
    //    /**
    //     * Set {@link IRowWriteMultiPageOp} for {@link IRowValuesWriter}
    //     *
    //     * @param multiPageOp multi-buffer allocator
    //     */
    //    public void init(IRowWriteMultiPageOp multiPageOp) throws HyracksDataException {
    //        multiPageOpRef.setValue(multiPageOp);
    //
    //        //Reset writer for the first write
    //        for (int i = 0; i < columnWriters.size(); i++) {
    //            columnWriters.get(i).reset();
    //        }
    //    }

    //    public IRowValuesWriter getWriter(int columnIndex) {
    ////        return columnWriters.get(columnIndex);
    //    }

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
        //        return columnWriters.size();
    }

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


    public AbstractRowSchemaNode getOrCreateChild(AbstractRowSchemaNode child, ATypeTag childTypeTag,ArrayBackedValueStorage fieldName)
            throws HyracksDataException {
        AbstractRowSchemaNode currentChild = child;
        ATypeTag normalizedTypeTag = getNormalizedTypeTag(childTypeTag);
        if (currentChild == null || normalizedTypeTag != ATypeTag.MISSING && normalizedTypeTag != ATypeTag.NULL
                && currentChild.getTypeTag() != ATypeTag.UNION && currentChild.getTypeTag() != normalizedTypeTag) {
            //Create a new child or union type if required type is different from the current child type
            currentChild = createChild(child, normalizedTypeTag,fieldName);
            //Flag that the schema has changed
            changed = true;
        }
        return currentChild;
    }

    public void enterLevel(AbstractRowSchemaNestedNode node) {
        level++;
        if (node.isCollection()) {
            repeated++;
        }
    }

    public void exitLevel(AbstractRowSchemaNestedNode node) {
        level--;
        if (node.isCollection()) {
            repeated--;
        }
    }

    public void enterNode(AbstractRowSchemaNestedNode parent, AbstractRowSchemaNode node) throws HyracksDataException {
        //Flush all definition levels from parent to child
//        flushDefinitionLevels(level, parent, node);
        if (node.isObjectOrCollection()) {
            //Enter one more level for object, array, and multiset
            level++;
            if (node.isCollection()) {
                //Tells nested values that they are repeated
                repeated++;
            }
        }
    }

    public void exitNode(AbstractRowSchemaNode node) {
        if (node.isNested()) {
            //Add the nested node's level for all missing children (i.e., not entered for a record)
            definitionLevels.get((AbstractRowSchemaNestedNode) node).add(level);
            if (node.isObjectOrCollection()) {
                //Union nodes should not change the level as they are logical nodes
                level--;
            }
        }
        node.incrementCounter();
    }

    public void exitCollectionNode(AbstractRowCollectionSchemaNode collectionNode, int numberOfItems) {
        RunRowLengthIntArray collectionDefLevels = definitionLevels.get(collectionNode);
        //Add delimiter
        collectionDefLevels.add(level - 1);
        level--;
        repeated--;
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

    public void clearDefinitionLevels(AbstractRowSchemaNestedNode nestedNode) {
        definitionLevels.get(nestedNode).reset();
    }

//    public void flushDefinitionLevels(int level, AbstractRowSchemaNestedNode parent, AbstractRowSchemaNode node)
//            throws HyracksDataException {
//        if (parent != null) {
//            RunRowLengthIntArray parentDefLevels = definitionLevels.get(parent);
//            if (node.getCounter() < parentDefLevels.getSize()) {
//                int parentMask = RowValuesUtil.getNullMask(level);
//                int childMask = RowValuesUtil.getNullMask(level + 1);
//                flushDefinitionLevels(parentMask, childMask, parentDefLevels, node);
//            }
//        }
//    }

//    private void flushDefinitionLevels(int parentMask, int childMask, RunRowLengthIntArray parentDefLevels,
//            AbstractRowSchemaNode node) throws HyracksDataException {
//        int startIndex = node.getCounter();
//        if (node.isNested()) {
//            RunRowLengthIntArray childDefLevels = definitionLevels.get((AbstractRowSchemaNestedNode) node);
//            flushNestedDefinitionLevel(parentMask, childMask, startIndex, parentDefLevels, childDefLevels);
//        } else {
//            IRowValuesWriter writer = columnWriters.get(((PrimitiveRowSchemaNode) node).getColumnIndex());
//            flushWriterDefinitionLevels(parentMask, childMask, startIndex, parentDefLevels, writer);
//            //            flushWriterDefinitionLevels(parentMask, childMask, startIndex, parentDefLevels);
//        }
//        node.setCounter(parentDefLevels.getSize());
//    }

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

    private void flushWriterDefinitionLevels(int parentMask, int childMask, int startIndex,
            RunRowLengthIntArray parentDefLevels, IRowValuesWriter writer) throws HyracksDataException {
        //    private void flushWriterDefinitionLevels(int parentMask, int childMask, int startIndex,
        //            RunRowLengthIntArray parentDefLevels) throws HyracksDataException {
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
                RowValuesUtil.getChildValue(parentMask, childMask, parentDefLevels.getBlockValue(blockIndex));
        writer.writeLevels(firstBlockValue, remainingValues);

        //Write remaining definition levels from the remaining blocks
        for (int i = blockIndex + 1; i < parentDefLevels.getNumberOfBlocks(); i++) {
            int blockValue = RowValuesUtil.getChildValue(parentMask, childMask, parentDefLevels.getBlockValue(i));
            writer.writeLevels(blockValue, parentDefLevels.getBlockSize(i));
        }
    }

    private AbstractRowSchemaNode createChild(AbstractRowSchemaNode child, ATypeTag normalizedTypeTag,ArrayBackedValueStorage fieldName)
            throws HyracksDataException {
        AbstractRowSchemaNode createdChild;
        if (child != null) {
            if (child.getTypeTag() == ATypeTag.NULL) {
                //The previous child was a NULL. The new child needs to inherit the NULL definition levels
//                int columnIndex = ((PrimitiveRowSchemaNode) child).getColumnIndex();
//                RunRowLengthIntArray defLevels = columnWriters.get(columnIndex).getDefinitionLevelsIntArray();
//                //Add the column index to be garbage collected
//                nullWriterIndexes.add(columnIndex);
                createdChild = createChild(normalizedTypeTag,fieldName);
//                int mask = RowValuesUtil.getNullMask(level);
//                flushDefinitionLevels(mask, mask, defLevels, createdChild);
                System.out.println("TO BE REIMPLEMENTED WITH THIS CASE : CALVIN DANI");
            } else {
                //Different type. Make union
                createdChild = addDefinitionLevelsAndGet(new UnionRowSchemaNode(child, createChild(normalizedTypeTag,fieldName)));
            }
        } else {
            createdChild = createChild(normalizedTypeTag,fieldName);
        }
        return createdChild;
    }

    private AbstractRowSchemaNode createChild(AbstractRowSchemaNode child, ATypeTag normalizedTypeTag)
            throws HyracksDataException {
        AbstractRowSchemaNode createdChild;
        ArrayBackedValueStorage initFieldName = new ArrayBackedValueStorage();
        if (child != null) {
            if (child.getTypeTag() == ATypeTag.NULL) {
                //The previous child was a NULL. The new child needs to inherit the NULL definition levels
//                int columnIndex = ((PrimitiveRowSchemaNode) child).getColumnIndex();
//                RunRowLengthIntArray defLevels = columnWriters.get(columnIndex).getDefinitionLevelsIntArray();
//                //Add the column index to be garbage collected
//                nullWriterIndexes.add(columnIndex);

                createdChild = createChild(normalizedTypeTag,initFieldName);
//                int mask = RowValuesUtil.getNullMask(level);
//                flushDefinitionLevels(mask, mask, defLevels, createdChild);
                System.out.println("TO BE REIMPLEMENTED WITH THIS CASE : CALVIN DANI");
            } else {
                //Different type. Make union
                createdChild = addDefinitionLevelsAndGet(new UnionRowSchemaNode(child, createChild(normalizedTypeTag,initFieldName)));
            }
        } else {
            createdChild = createChild(normalizedTypeTag,initFieldName);
        }
        return createdChild;
    }


    private AbstractRowSchemaNode createChild(ATypeTag normalizedTypeTag, ArrayBackedValueStorage fieldName) throws HyracksDataException {
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
                //                int columnIndex = nullWriterIndexes.isEmpty() ? this.sizeOfWriters : nullWriterIndexes.removeInt(0);
//                boolean primaryKey = columnIndex < getNumberOfPrimaryKeys();
//                boolean writeAlways = primaryKey || repeated > 0;
//                boolean filtered = !primaryKey;
//                int maxLevel = primaryKey ? 1 : level + 1;
//                IRowValuesWriter writer = columnWriterFactory.createValueWriter(normalizedTypeTag, columnIndex,
//                        maxLevel, writeAlways, filtered);
//                if (multiPageOpRef.getValue() != null) {
//                    writer.reset();
//                }
                if (columnIndex == this.sizeOfWriters) {
                    this.sizeOfWriters += 1;

                    //            columnWriters.add(writer);
                }
                //                addColumn(columnIndex);
                return new PrimitiveRowSchemaNode(columnIndex, normalizedTypeTag, false, fieldName);
            default:
                throw new IllegalStateException("Unsupported type " + normalizedTypeTag);

        }
    }

//    private AbstractRowSchemaNode createChild(ATypeTag normalizedTypeTag) throws HyracksDataException {
//        switch (normalizedTypeTag) {
//            case OBJECT:
//                return addDefinitionLevelsAndGet(new ObjectRowSchemaNode());
//            case ARRAY:
//                return addDefinitionLevelsAndGet(new ArrayRowSchemaNode());
//            case MULTISET:
////                return addDefinitionLevelsAndGet(new MultisetRowSchemaNode());
//            case NULL:
//            case MISSING:
//            case BOOLEAN:
//            case DOUBLE:
//            case BIGINT:
//            case STRING:
//            case UUID:
//                int columnIndex = nullWriterIndexes.isEmpty() ? this.sizeOfWriters : nullWriterIndexes.removeInt(0);
//                //                int columnIndex = nullWriterIndexes.isEmpty() ? this.sizeOfWriters : nullWriterIndexes.removeInt(0);
////                boolean primaryKey = columnIndex < getNumberOfPrimaryKeys();
////                boolean writeAlways = primaryKey || repeated > 0;
////                boolean filtered = !primaryKey;
////                int maxLevel = primaryKey ? 1 : level + 1;
////                IRowValuesWriter writer = columnWriterFactory.createValueWriter(normalizedTypeTag, columnIndex,
////                        maxLevel, writeAlways, filtered);
////                if (multiPageOpRef.getValue() != null) {
////                    writer.reset();
////                }
//                if (columnIndex == this.sizeOfWriters) {
//                    this.sizeOfWriters += 1;
//
//                    //            columnWriters.add(writer);
//                }
//                //                addColumn(columnIndex);
//                return new PrimitiveRowSchemaNode(columnIndex, normalizedTypeTag, false);
//            default:
//                throw new IllegalStateException("Unsupported type " + normalizedTypeTag);
//
//        }
//    }

    //    private void addColumn(int index) {
    ////    private void addColumn(int index, IRowValuesWriter writer) {
    ////        if (index == columnWriters.size()) {
    //        if (index == this.sizeOfWriters) {
    //            this.sizeOfWriters += 1;
    //
    ////            columnWriters.add(writer);
    //        } else {
    ////            columnWriters.set(index, writer);
    //        }
    //    }

    private AbstractRowSchemaNode addDefinitionLevelsAndGet(AbstractRowSchemaNestedNode nestedNode) {
        definitionLevels.put(nestedNode, new RunRowLengthIntArray());
        return nestedNode;
    }

    private static void logSchema(ObjectRowSchemaNode root, ObjectRowSchemaNode metaRoot,
            RowFieldNamesDictionary fieldNamesDictionary) throws HyracksDataException {
        if (!LOGGER.isDebugEnabled()) {
            return;
        }
        // This should be a low frequency object creation
        RowSchemaStringBuilderVisitor schemaBuilder = new RowSchemaStringBuilderVisitor(fieldNamesDictionary);
        String recordSchema = LogRedactionUtil.userData(schemaBuilder.build(root));
        LOGGER.debug("Schema for {} has changed: \n {}", RowSchemaStringBuilderVisitor.RECORD_SCHEMA, recordSchema);
        if (metaRoot != null) {
            String metaRecordSchema = LogRedactionUtil.userData(schemaBuilder.build(metaRoot));
            LOGGER.debug("Schema for {} has changed: \n {}", RowSchemaStringBuilderVisitor.META_RECORD_SCHEMA,
                    metaRecordSchema);
        }
    }

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
//        for (int i = 0; i < columnWriters.size(); i++) {
//            columnWriters.get(i).close();
//        }
    }

//    public void addNestedNull(AbstractRowSchemaNestedNode parent, AbstractRowSchemaNestedNode node)
//            throws HyracksDataException {
//        //Flush all definition levels from parent to the current node
//        flushDefinitionLevels(level, parent, node);
//        //Add null value (+2) to say that both the parent and the child are present
//        definitionLevels.get(node).add(RowValuesUtil.getNullMask(level + 2) | level);
//        node.incrementCounter();
//    }
}
