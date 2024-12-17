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
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.column.metadata.schema.AbstractSchemaNestedNode;
import org.apache.asterix.column.metadata.schema.AbstractSchemaNode;
import org.apache.asterix.column.metadata.schema.ObjectSchemaNode;
import org.apache.asterix.column.metadata.schema.UnionSchemaNode;
import org.apache.asterix.column.metadata.schema.collection.AbstractCollectionSchemaNode;
import org.apache.asterix.column.metadata.schema.collection.ArraySchemaNode;
import org.apache.asterix.column.metadata.schema.collection.MultisetSchemaNode;
import org.apache.asterix.column.metadata.schema.primitive.PrimitiveSchemaNode;
import org.apache.asterix.column.util.RunLengthIntArray;
import org.apache.asterix.column.values.IColumnValuesWriter;
import org.apache.asterix.column.values.IColumnValuesWriterFactory;
import org.apache.asterix.column.values.writer.NoOpColumnValuesWriter;
import org.apache.asterix.om.dictionary.AbstractFieldNamesDictionary;
import org.apache.asterix.om.dictionary.IFieldNamesDictionary;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;

/**
 * Flush column metadata belongs to a flushing {@link ILSMMemoryComponent}
 * The schema here is mutable and can change according to the flushed records
 */
public final class NoWriteFlushColumnMetadata extends FlushColumnMetadata {

    private int numColumns;

    public NoWriteFlushColumnMetadata(ARecordType datasetType, ARecordType metaType, int numPrimaryKeys,
            boolean metaContainsKeys, IColumnValuesWriterFactory columnWriterFactory,
            Mutable<IColumnWriteMultiPageOp> multiPageOpRef, List<IColumnValuesWriter> writers,
            IFieldNamesDictionary fieldNamesDictionary, ObjectSchemaNode root, ObjectSchemaNode metaRoot,
            Map<AbstractSchemaNestedNode, RunLengthIntArray> definitionLevels, ArrayBackedValueStorage schemaStorage) {
        super(datasetType, metaType, numPrimaryKeys, metaContainsKeys, columnWriterFactory, multiPageOpRef, writers,
                fieldNamesDictionary, root, metaRoot, definitionLevels, schemaStorage);
        numColumns = 0;
    }

    public static NoWriteFlushColumnMetadata createMutableMetadata(ARecordType datasetType, ARecordType metaType,
            int numPrimaryKeys, boolean metaContainsKeys, IColumnValuesWriterFactory columnWriterFactory,
            Mutable<IColumnWriteMultiPageOp> multiPageOpRef, IValueReference serializedMetadata) throws IOException {
        DataInput input = new DataInputStream(new ByteArrayInputStream(serializedMetadata.getByteArray(),
                serializedMetadata.getStartOffset(), serializedMetadata.getLength()));
        //Skip offsets
        input.skipBytes(OFFSETS_SIZE);

        //ColumnWriter
        List<IColumnValuesWriter> writers = new ArrayList<>();
        deserializeWriters(input, writers, columnWriterFactory);

        //FieldNames
        IFieldNamesDictionary fieldNamesDictionary = AbstractFieldNamesDictionary.deserialize(input);

        //Schema
        Map<AbstractSchemaNestedNode, RunLengthIntArray> definitionLevels = new HashMap<>();
        ObjectSchemaNode root = (ObjectSchemaNode) AbstractSchemaNode.deserialize(input, definitionLevels);
        ObjectSchemaNode metaRoot = null;
        if (metaType != null) {
            metaRoot = (ObjectSchemaNode) AbstractSchemaNode.deserialize(input, definitionLevels);
        }

        ArrayBackedValueStorage schemaStorage = new ArrayBackedValueStorage(serializedMetadata.getLength());
        schemaStorage.append(serializedMetadata);
        return new NoWriteFlushColumnMetadata(datasetType, metaType, numPrimaryKeys, metaContainsKeys,
                columnWriterFactory, multiPageOpRef, writers, fieldNamesDictionary, root, metaRoot, definitionLevels,
                schemaStorage);
    }

    public void close() {
    }

    @Override
    public void flushDefinitionLevels(int level, AbstractSchemaNestedNode parent, AbstractSchemaNode node)
            throws HyracksDataException {
        //NoOp
    }

    @Override
    protected void flushDefinitionLevels(int parentMask, int childMask, RunLengthIntArray parentDefLevels,
            AbstractSchemaNode node) throws HyracksDataException {
        //NoOp
    }

    @Override
    public void enterLevel(AbstractSchemaNestedNode node) {
        //NoOp
    }

    @Override
    public void exitNode(AbstractSchemaNode node) {
        //NoOp
    }

    @Override
    public void exitLevel(AbstractSchemaNestedNode node) {
        //NoOp
    }

    @Override
    public void exitCollectionNode(AbstractCollectionSchemaNode collectionNode, int numberOfItems) {
        //NoOp
    }

    @Override
    public void addNestedNull(AbstractSchemaNestedNode parent, AbstractSchemaNestedNode node)
            throws HyracksDataException {
        //NoOp
    }

    @Override
    protected AbstractSchemaNode createChild(AbstractSchemaNode child, ATypeTag childTypeTag)
            throws HyracksDataException {
        AbstractSchemaNode createdChild;
        ATypeTag normalizedTypeTag = getNormalizedTypeTag(childTypeTag);
        if (child != null) {
            if (child.getTypeTag() == ATypeTag.NULL) {
                int columnIndex = ((PrimitiveSchemaNode) child).getColumnIndex();
                nullWriterIndexes.add(columnIndex);
                createdChild = createChild(normalizedTypeTag);
            } else {
                createdChild = addDefinitionLevelsAndGet(new UnionSchemaNode(child, createChild(normalizedTypeTag)));
            }
        } else {
            createdChild = createChild(normalizedTypeTag);
        }
        return createdChild;
    }

    @Override
    protected AbstractSchemaNode createChild(ATypeTag childTypeTag) throws HyracksDataException {
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
                if (columnIndex == numColumns) {
                    numColumns++;
                }
                IColumnValuesWriter writer = NoOpColumnValuesWriter.INSTANCE;
                addColumn(columnIndex, writer);
                return new PrimitiveSchemaNode(columnIndex, normalizedTypeTag, primaryKey);
            default:
                throw new IllegalStateException("Unsupported type " + childTypeTag);

        }
    }

    @Override
    protected AbstractSchemaNode addDefinitionLevelsAndGet(AbstractSchemaNestedNode nestedNode) {
        return nestedNode;
    }
}
