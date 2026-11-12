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

import static org.apache.asterix.om.dictionary.AbstractFieldNamesDictionary.MAP_FIELD_NAME_INDEX;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.asterix.column.metadata.schema.AbstractSchemaNode;
import org.apache.asterix.column.metadata.schema.ObjectSchemaNode;
import org.apache.asterix.column.metadata.schema.UnionSchemaNode;
import org.apache.asterix.column.metadata.schema.collection.AbstractCollectionSchemaNode;
import org.apache.asterix.column.metadata.schema.primitive.MissingFieldSchemaNode;
import org.apache.asterix.column.metadata.schema.primitive.PrimitiveSchemaNode;
import org.apache.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.dictionary.IFieldNamesDictionary;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.IATypeVisitor;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.projection.FunctionCallInformation;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.util.string.UTF8StringReader;
import org.apache.hyracks.util.string.UTF8StringWriter;

public class SchemaClipperVisitor implements IATypeVisitor<AbstractSchemaNode, AbstractSchemaNode> {
    private final IFieldNamesDictionary fieldNamesDictionary;
    private final IWarningCollector warningCollector;
    private final Map<String, FunctionCallInformation> functionCallInfoMap;
    private boolean ignoreFlatType;
    private final AMutableString mutableString = new AMutableString("");
    private final AStringSerializerDeserializer stringSerDer =
            new AStringSerializerDeserializer(new UTF8StringWriter(), new UTF8StringReader());

    public SchemaClipperVisitor(IFieldNamesDictionary fieldNamesDictionary,
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
        List<IValueReference> collapsedLookups = null;
        try {
            for (int i = 0; i < fieldNames.length; i++) {
                int fieldNameIndex = fieldNamesDictionary.getFieldNameIndex(fieldNames[i]);
                if (fieldNameIndex == -1) {
                    // Either genuinely absent, or collapsed into the map — map keys are stored as values in the key
                    // column and are deliberately never interned, so they can never resolve here.
                    if (objectNode.isMapified()) {
                        if (collapsedLookups == null) {
                            collapsedLookups = new ArrayList<>();
                        }
                        collapsedLookups.add(serializeName(fieldNames[i]));
                    }
                    continue;
                }
                if (objectNode.isMapified() && objectNode.getChild(fieldNameIndex) == MissingFieldSchemaNode.INSTANCE) {
                    /*
                     * Resolves, but only because some other node interned the name: the dictionary is shared by the
                     * whole schema, and this node routes every name it has no child for into its map. So the field
                     * can only live in the map. A missing child must not be added next to the map either -- the
                     * assembler has no column to read for it.
                     */
                    if (collapsedLookups == null) {
                        collapsedLookups = new ArrayList<>();
                    }
                    collapsedLookups.add(serializeName(fieldNames[i]));
                    continue;
                }
                AbstractSchemaNode child = objectNode.getChild(fieldNameIndex);
                clippedObjectNode.addChild(fieldNameIndex, fieldTypes[i].accept(this, child));
            }
            if (collapsedLookups != null) {
                /*
                 * A requested name that resolved to nothing may still live in the collapsed map, so the map has to be
                 * read to answer the query — returning without it would silently answer "missing" for a field that is
                 * present. The requested names travel with the clipped node so the assembler can drop entries the
                 * query never asked for, rather than rebuilding the entire object and leaving the caller to select.
                 */
                clippedObjectNode.addChild(MAP_FIELD_NAME_INDEX, objectNode.getMapNode().clip(collapsedLookups));
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

    /** Serializes a requested name to the untagged UTF-8 form the key column stores, for byte-level comparison. */
    private IValueReference serializeName(String fieldName) throws HyracksDataException {
        ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
        mutableString.setValue(fieldName);
        stringSerDer.serialize(mutableString, storage.getDataOutput());
        return storage;
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
            if (unionChildNode.getTypeTag() == ATypeTag.MISSING) {
                continue;
            }
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
