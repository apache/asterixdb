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
package org.apache.asterix.om.lazy.metadata.schema.visitor;

import java.util.List;

import org.apache.asterix.om.RowMetadata;
import org.apache.asterix.om.lazy.metadata.RowFieldNamesDictionary;
import org.apache.asterix.om.lazy.metadata.schema.AbstractRowSchemaNode;
import org.apache.asterix.om.lazy.metadata.schema.ObjectRowSchemaNode;
import org.apache.asterix.om.lazy.metadata.schema.collection.ArrayRowSchemaNode;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.IATypeVisitor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;

public class SchemaRowBuilderFromIATypeVisitor implements IATypeVisitor<Void, AbstractRowSchemaNode> {
    private final RowMetadata columnMetadata;
    private final List<List<String>> primaryKeys;
    private List<String> currentPrimaryKeyPath;
    private int processedPrimaryKeys;
    private int currentPathIndex;

    public SchemaRowBuilderFromIATypeVisitor(RowMetadata columnMetadata, List<List<String>> primaryKeys) {
        this.columnMetadata = columnMetadata;
        this.primaryKeys = primaryKeys;
        processedPrimaryKeys = 0;
    }

    @Override
    public Void visit(ARecordType recordType, AbstractRowSchemaNode arg) {
        ObjectRowSchemaNode objectNode = (ObjectRowSchemaNode) arg;
        columnMetadata.enterLevel(objectNode);
        try {
            if (processedPrimaryKeys < primaryKeys.size()) {
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
    public Void visit(AbstractCollectionType collectionType, AbstractRowSchemaNode arg) {
        ArrayRowSchemaNode collectionNode = (ArrayRowSchemaNode) arg;
        IAType itemType = collectionType.getItemType();
        columnMetadata.enterLevel(collectionNode);
        try {
            AbstractRowSchemaNode itemNode = collectionNode.getOrCreateItem(itemType.getTypeTag(), columnMetadata);
            itemType.accept(this, itemNode);
        } catch (HyracksDataException e) {
            throw new IllegalStateException(e);
        }
        columnMetadata.exitLevel(collectionNode);
        return null;
    }

    @Override
    public Void visit(AUnionType unionType, AbstractRowSchemaNode arg) {
        throw new IllegalStateException(unionType.getTypeTag() + " is not a declared type");
    }

    @Override
    public Void visitFlat(IAType flatType, AbstractRowSchemaNode arg) {
        if (processedPrimaryKeys < primaryKeys.size()) {
            processedPrimaryKeys++;
        }
        return null;
    }

    /*
     * **************************************************************
     * Handling primary keys and record fields conversion
     * **************************************************************
     */
    private void processPrimaryKeys(ARecordType recordType, ObjectRowSchemaNode objectNode)
            throws HyracksDataException {
        if (objectNode == columnMetadata.getRoot() || objectNode == columnMetadata.getMetaRoot()) {
            while (processedPrimaryKeys < primaryKeys.size()) {
                currentPrimaryKeyPath = primaryKeys.get(processedPrimaryKeys);
                currentPathIndex = 0;
                processPrimaryKeyPath(recordType, objectNode);
            }
        } else {
            currentPathIndex++;
            processPrimaryKeyPath(recordType, objectNode);
        }
    }

    private void processPrimaryKeyPath(ARecordType recordType, ObjectRowSchemaNode objectNode)
            throws HyracksDataException {
        int fieldIndex = recordType.getFieldIndex(currentPrimaryKeyPath.get(currentPathIndex));
        processField(fieldIndex, recordType, objectNode);
    }

    private void processField(int fieldIndex, ARecordType recordType, ObjectRowSchemaNode objectNode)
            throws HyracksDataException {
        IAType[] fieldTypes = recordType.getFieldTypes();
        String[] fieldNames = recordType.getFieldNames();
        RowFieldNamesDictionary dictionary = columnMetadata.getFieldNamesDictionary();

        int fieldNameIndex = dictionary.getOrCreateFieldNameIndex(fieldNames[fieldIndex]);
        IValueReference fieldName = dictionary.getFieldName(fieldNameIndex);

        IAType fieldType = fieldTypes[fieldIndex];
        AbstractRowSchemaNode child = objectNode.getOrCreateChild(fieldName, fieldType.getTypeTag(), columnMetadata);

        fieldType.accept(this, child);
    }
}
