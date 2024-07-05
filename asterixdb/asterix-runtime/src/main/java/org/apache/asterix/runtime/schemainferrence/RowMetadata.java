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

    private final Mutable<IRowWriteMultiPageOp> multiPageOpRef;
    private final RowFieldNamesDictionary fieldNamesDictionary;
    private final ObjectRowSchemaNode root;
    private final ObjectRowSchemaNode metaRoot;

    private final ArrayBackedValueStorage serializedMetadata;
    private final IntArrayList nullWriterIndexes;
    private boolean changed;
    private int level;

    public RowMetadata(Mutable<IRowWriteMultiPageOp> multiPageOpRef) throws HyracksDataException {
        super();
        this.multiPageOpRef = multiPageOpRef;

        level = -1;
        fieldNamesDictionary = new RowFieldNamesDictionary();
        ArrayBackedValueStorage initFieldName = new ArrayBackedValueStorage(1);
        root = new ObjectRowSchemaNode(initFieldName);
        metaRoot = null;
        nullWriterIndexes = new IntArrayList();
        //Add definition levels for the root
//        addDefinitionLevelsAndGet(root);
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



    public Mutable<IRowWriteMultiPageOp> getMultiPageOpRef() {
        return multiPageOpRef;
    }

    // Convert Schema to byte array (IValueReference)
    @Override
    public IValueReference serializeColumnsMetadata() throws HyracksDataException {
        if (changed) {
            try {
                serializeChanges();
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
        setOffset(schemaOffsetPointer);
        root.serialize(output);
        if (metaRoot != null) {
            //Meta schema
            setOffset(metaSchemaOffsetPointer);
            metaRoot.serialize(output);
        }

        //Path info
        setOffset(pathInfoOffsetPointer);
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

        root.abort(input);
    }

    /* ********************************************************
     * Schema related methods
     * ********************************************************
     */

    public int getLevel() {
        return level;
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
        level--;
        collectionNode.incrementCounter();
    }

    //TODO : CALVIN_DANI remove overhead


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
                createdChild =
                        new UnionRowSchemaNode(child, createChild(normalizedTypeTag, fieldName));
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
                createdChild =
                        new UnionRowSchemaNode(child, createChild(normalizedTypeTag, initFieldName));
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
                return new ObjectRowSchemaNode(fieldName);
            case ARRAY:
                return new ArrayRowSchemaNode(fieldName);
            case MULTISET:
                return new MultisetRowSchemaNode(fieldName);
            case NULL:
            case MISSING:
            case BOOLEAN:
            case DOUBLE:
            case BIGINT:
            case STRING:
            case UUID:
//                int columnIndex = nullWriterIndexes.isEmpty() ? this.sizeOfWriters : nullWriterIndexes.removeInt(0);

                return new PrimitiveRowSchemaNode( normalizedTypeTag, false);
            default:
                throw new IllegalStateException("Unsupported type " + normalizedTypeTag);

        }
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



    public void addNestedNull(AbstractRowSchemaNestedNode node)
            throws HyracksDataException {
        //Add null value (+2) to say that both the parent and the child are present
        node.incrementCounter();
    }
}
