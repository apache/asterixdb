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

import java.io.IOException;
import java.util.Map;

import org.apache.asterix.column.metadata.FieldNamesDictionary;
import org.apache.asterix.column.metadata.schema.AbstractSchemaNode;
import org.apache.asterix.column.metadata.schema.ObjectSchemaNode;
import org.apache.asterix.column.metadata.schema.UnionSchemaNode;
import org.apache.asterix.column.metadata.schema.collection.AbstractCollectionSchemaNode;
import org.apache.asterix.column.metadata.schema.primitive.MissingFieldSchemaNode;
import org.apache.asterix.column.metadata.schema.primitive.PrimitiveSchemaNode;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.IATypeVisitor;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.projection.FunctionCallInformation;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.Warning;

public class SchemaClipperVisitor implements IATypeVisitor<AbstractSchemaNode, AbstractSchemaNode> {
    private final FieldNamesDictionary fieldNamesDictionary;
    private final IWarningCollector warningCollector;
    private final Map<String, FunctionCallInformation> functionCallInfoMap;
    private boolean ignoreFlatType;

    public SchemaClipperVisitor(FieldNamesDictionary fieldNamesDictionary,
            Map<String, FunctionCallInformation> functionCallInfoMap, IWarningCollector warningCollector) {
        this.fieldNamesDictionary = fieldNamesDictionary;
        this.functionCallInfoMap = functionCallInfoMap;
        this.warningCollector = warningCollector;
        ignoreFlatType = false;
    }

    public void setIgnoreFlatType(boolean ignoreFlatType) {
        this.ignoreFlatType = ignoreFlatType;
    }

    @Override
    public AbstractSchemaNode visit(ARecordType recordType, AbstractSchemaNode arg) {
        if (isNotCompatible(recordType, arg)) {
            return MissingFieldSchemaNode.INSTANCE;
        }

        String[] fieldNames = recordType.getFieldNames();
        IAType[] fieldTypes = recordType.getFieldTypes();
        ObjectSchemaNode objectNode = getActualNode(arg, ATypeTag.OBJECT, ObjectSchemaNode.class);

        ObjectSchemaNode clippedObjectNode = new ObjectSchemaNode();
        try {
            for (int i = 0; i < fieldNames.length; i++) {
                int fieldNameIndex = fieldNamesDictionary.getFieldNameIndex(fieldNames[i]);
                if (fieldNameIndex == -1) {
                    // Missing child
                    continue;
                }
                AbstractSchemaNode child = objectNode.getChild(fieldNameIndex);
                clippedObjectNode.addChild(fieldNameIndex, fieldTypes[i].accept(this, child));
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

        return clippedObjectNode;
    }

    @Override
    public AbstractSchemaNode visit(AbstractCollectionType collectionType, AbstractSchemaNode arg) {
        // We check only if arg is a collection to include both array and multiset
        if (!arg.isCollection() && isNotCompatible(collectionType, arg)) {
            return MissingFieldSchemaNode.INSTANCE;
        }

        ATypeTag typeTag = arg.isCollection() ? arg.getTypeTag() : ATypeTag.ARRAY;
        AbstractCollectionSchemaNode collectionNode = getActualNode(arg, typeTag, AbstractCollectionSchemaNode.class);
        AbstractSchemaNode newItemNode = collectionType.getItemType().accept(this, collectionNode.getItemNode());
        AbstractCollectionSchemaNode clippedCollectionNode =
                AbstractCollectionSchemaNode.create(collectionType.getTypeTag());
        clippedCollectionNode.setItemNode(newItemNode);
        return clippedCollectionNode;
    }

    @Override
    public AbstractSchemaNode visit(AUnionType unionType, AbstractSchemaNode arg) {
        return arg;
    }

    @Override
    public AbstractSchemaNode visitFlat(IAType flatType, AbstractSchemaNode arg) {
        if (ignoreFlatType || flatType.getTypeTag() == ATypeTag.ANY) {
            return arg;
        } else if (isNotCompatible(flatType, arg)) {
            return getNonCompatibleNumericNodeIfAny(flatType, arg);
        }
        return getActualNode(arg, flatType.getTypeTag(), PrimitiveSchemaNode.class);
    }

    private AbstractSchemaNode getNonCompatibleNumericNodeIfAny(IAType flatType, AbstractSchemaNode arg) {
        if (isNumeric(flatType.getTypeTag()) && isNumeric(arg.getTypeTag())) {
            // This will be reconciled by the filter accessor
            return arg;
        } else if (arg.getTypeTag() == ATypeTag.UNION) {
            UnionSchemaNode unionNode = (UnionSchemaNode) arg;
            return unionNode.getNumericChildOrMissing(flatType.getTypeTag());
        }

        return MissingFieldSchemaNode.INSTANCE;
    }

    private <T extends AbstractSchemaNode> T getActualNode(AbstractSchemaNode node, ATypeTag typeTag, Class<T> clazz) {
        if (node.getTypeTag() == typeTag) {
            return clazz.cast(node);
        } else {
            //Then it is a union (as we check for incompatibility before we call this method)
            UnionSchemaNode unionNode = (UnionSchemaNode) node;
            return clazz.cast(unionNode.getChild(typeTag));
        }
    }

    private boolean isNotCompatible(IAType requestedType, AbstractSchemaNode schemaNode) {
        if (schemaNode.getTypeTag() == ATypeTag.MISSING) {
            return true;
        }
        ATypeTag requestedTypeTag = requestedType.getTypeTag();
        if (requestedTypeTag != schemaNode.getTypeTag()) {
            if (schemaNode.getTypeTag() != ATypeTag.UNION) {
                warn(requestedType, schemaNode);
                return true;
            }
            // Handle union
            UnionSchemaNode unionNode = (UnionSchemaNode) schemaNode;
            return notInUnion(requestedType, unionNode)
                    || isNumeric(requestedTypeTag) && unionContainsMultipleNumeric(schemaNode);
        }
        return unionContainsMultipleNumeric(schemaNode);
    }

    private boolean notInUnion(IAType requestedType, UnionSchemaNode unionNode) {
        for (AbstractSchemaNode unionChildNode : unionNode.getChildren().values()) {
            warn(requestedType, unionChildNode);
        }
        return !unionNode.getChildren().containsKey(requestedType.getTypeTag());
    }

    private void warn(IAType requestedType, AbstractSchemaNode schemaNode) {
        if (ATypeHierarchy.isCompatible(requestedType.getTypeTag(), schemaNode.getTypeTag())) {
            return;
        }
        if (warningCollector.shouldWarn() && functionCallInfoMap.containsKey(requestedType.getTypeName())) {
            Warning warning = functionCallInfoMap.get(requestedType.getTypeName())
                    .createWarning(requestedType.getTypeTag(), schemaNode.getTypeTag());
            if (warning != null) {
                warningCollector.warn(warning);
            }
        }
    }

    private boolean unionContainsMultipleNumeric(AbstractSchemaNode schemaNode) {
        if (schemaNode.getTypeTag() == ATypeTag.UNION) {
            UnionSchemaNode unionNode = (UnionSchemaNode) schemaNode;
            return unionNode.getNumberOfNumericChildren() > 1;
        }
        return false;
    }

    private static boolean isNumeric(ATypeTag typeTag) {
        return ATypeHierarchy.getTypeDomain(typeTag) == ATypeHierarchy.Domain.NUMERIC;
    }
}
