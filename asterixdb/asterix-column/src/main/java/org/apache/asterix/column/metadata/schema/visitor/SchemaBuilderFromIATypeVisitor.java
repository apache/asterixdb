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
package org.apache.asterix.column.metadata.schema.visitor;

import static org.apache.asterix.om.typecomputer.impl.TypeComputeUtils.getActualType;

import java.util.List;

import org.apache.asterix.column.metadata.FieldNamesDictionary;
import org.apache.asterix.column.metadata.schema.AbstractSchemaNode;
import org.apache.asterix.column.metadata.schema.ObjectSchemaNode;
import org.apache.asterix.column.metadata.schema.collection.AbstractCollectionSchemaNode;
import org.apache.asterix.column.metadata.schema.primitive.MissingFieldSchemaNode;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.IATypeVisitor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;

public class SchemaBuilderFromIATypeVisitor implements IATypeVisitor<Void, AbstractSchemaNode> {
    private final FlushColumnMetadata columnMetadata;
    private final List<List<String>> primaryKeys;
    private List<String> currentPrimaryKeyPath;
    private int processedPrimaryKeys;
    private int currentPathIndex;

    public SchemaBuilderFromIATypeVisitor(FlushColumnMetadata columnMetadata, List<List<String>> primaryKeys) {
        this.columnMetadata = columnMetadata;
        this.primaryKeys = primaryKeys;
        processedPrimaryKeys = 0;
    }

    @Override
    public Void visit(ARecordType recordType, AbstractSchemaNode arg) {
        ObjectSchemaNode objectNode = (ObjectSchemaNode) arg;
        columnMetadata.enterLevel(objectNode);
        try {
            if (isProcessingPrimaryKeys()) {
                processPrimaryKeys(recordType, objectNode);
            }

            for (int i = 0; i < recordType.getFieldTypes().length; i++) {
                processField(i, recordType, objectNode);
            }
        } catch (HyracksDataException e) {
            throw new IllegalStateException(e);
        }
        columnMetadata.exitLevel(objectNode);
        return null;
    }

    @Override
    public Void visit(AbstractCollectionType collectionType, AbstractSchemaNode arg) {
        AbstractCollectionSchemaNode collectionNode = (AbstractCollectionSchemaNode) arg;
        IAType itemType = getActualType(collectionType.getItemType());
        columnMetadata.enterLevel(collectionNode);
        try {
            AbstractSchemaNode itemNode = collectionNode.getOrCreateItem(itemType.getTypeTag(), columnMetadata);
            itemType.accept(this, itemNode);
        } catch (HyracksDataException e) {
            throw new IllegalStateException(e);
        }
        columnMetadata.exitLevel(collectionNode);
        return null;
    }

    @Override
    public Void visit(AUnionType unionType, AbstractSchemaNode arg) {
        throw new IllegalStateException(unionType.getTypeTag() + " is not a declared type");
    }

    @Override
    public Void visitFlat(IAType flatType, AbstractSchemaNode arg) {
        if (isProcessingPrimaryKeys()) {
            processedPrimaryKeys++;
        }
        return null;
    }

    /*
     * **************************************************************
     * Handling primary keys and record fields conversion
     * **************************************************************
     */

    private boolean isProcessingPrimaryKeys() {
        return processedPrimaryKeys < primaryKeys.size();
    }

    private void processPrimaryKeys(ARecordType recordType, ObjectSchemaNode objectNode) throws HyracksDataException {
        if (objectNode == columnMetadata.getRoot() || objectNode == columnMetadata.getMetaRoot()) {
            while (isProcessingPrimaryKeys()) {
                currentPrimaryKeyPath = primaryKeys.get(processedPrimaryKeys);
                currentPathIndex = 0;
                processPrimaryKeyPath(recordType, objectNode);
            }
        } else {
            currentPathIndex++;
            processPrimaryKeyPath(recordType, objectNode);
        }
    }

    private void processPrimaryKeyPath(ARecordType recordType, ObjectSchemaNode objectNode)
            throws HyracksDataException {
        int fieldIndex = recordType.getFieldIndex(currentPrimaryKeyPath.get(currentPathIndex));
        if (fieldIndex < 0) {
            currentPathIndex--;
            return;
        }
        processField(fieldIndex, recordType, objectNode);
    }

    private void processField(int fieldIndex, ARecordType recordType, ObjectSchemaNode objectNode)
            throws HyracksDataException {
        IAType[] fieldTypes = recordType.getFieldTypes();
        String[] fieldNames = recordType.getFieldNames();
        FieldNamesDictionary dictionary = columnMetadata.getFieldNamesDictionary();

        if (isProcessingPrimaryKeys() && !fieldNames[fieldIndex].equals(currentPrimaryKeyPath.get(currentPathIndex))) {
            // Still processing PKs, do not add any fields to the children until all PKs are processed
            return;
        }

        int fieldNameIndex = dictionary.getOrCreateFieldNameIndex(fieldNames[fieldIndex]);
        AbstractSchemaNode childNode = objectNode.getChild(fieldNameIndex);
        if (!childNode.isNested() && childNode != MissingFieldSchemaNode.INSTANCE) {
            // Avoid processing the flat child twice
            // Can happen if the child is a PK
            return;
        }

        IValueReference fieldName = dictionary.getFieldName(fieldNameIndex);

        IAType fieldType = getActualType(fieldTypes[fieldIndex]);
        AbstractSchemaNode child = objectNode.getOrCreateChild(fieldName, fieldType.getTypeTag(), columnMetadata);

        fieldType.accept(this, child);
    }
}
