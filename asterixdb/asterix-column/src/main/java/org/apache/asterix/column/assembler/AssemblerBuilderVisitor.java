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
package org.apache.asterix.column.assembler;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.column.assembler.value.IValueGetter;
import org.apache.asterix.column.assembler.value.IValueGetterFactory;
import org.apache.asterix.column.metadata.FieldNamesDictionary;
import org.apache.asterix.column.metadata.schema.AbstractSchemaNode;
import org.apache.asterix.column.metadata.schema.ISchemaNodeVisitor;
import org.apache.asterix.column.metadata.schema.ObjectSchemaNode;
import org.apache.asterix.column.metadata.schema.UnionSchemaNode;
import org.apache.asterix.column.metadata.schema.collection.AbstractCollectionSchemaNode;
import org.apache.asterix.column.metadata.schema.primitive.PrimitiveSchemaNode;
import org.apache.asterix.column.operation.query.QueryColumnMetadata;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.asterix.column.values.IColumnValuesReaderFactory;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

public class AssemblerBuilderVisitor implements ISchemaNodeVisitor<AbstractValueAssembler, AssemblerInfo> {
    private static final BitSet NO_DECLARED_FIELDS = new BitSet(0);
    private final QueryColumnMetadata columnMetadata;
    private final IColumnValuesReaderFactory readerFactory;
    private final List<AbstractPrimitiveValueAssembler> valueAssemblers;
    private final IValueGetterFactory valueGetterFactory;
    private final Map<Integer, IColumnValuesReader> primaryKeyReaders;
    private AbstractValueAssembler rootAssembler;

    //Recursion info
    private final IntList delimiters;
    private RepeatedPrimitiveValueAssembler delegateAssembler;
    private int level;

    public AssemblerBuilderVisitor(QueryColumnMetadata columnMetadata, IColumnValuesReaderFactory readerFactory,
            IValueGetterFactory valueGetterFactory) {
        this.columnMetadata = columnMetadata;
        this.readerFactory = readerFactory;
        this.valueGetterFactory = valueGetterFactory;
        valueAssemblers = new ArrayList<>();
        delimiters = new IntArrayList();
        primaryKeyReaders = new HashMap<>();
        for (IColumnValuesReader reader : columnMetadata.getPrimaryKeyReaders()) {
            primaryKeyReaders.put(reader.getColumnIndex(), reader);
        }
    }

    public AbstractPrimitiveValueAssembler[] createValueAssemblers(AbstractSchemaNode requestedSchema,
            ARecordType declaredType) throws HyracksDataException {
        EmptyAssembler root = new EmptyAssembler();
        AssemblerInfo info = new AssemblerInfo(declaredType, root);
        level = 0;
        rootAssembler = requestedSchema.accept(this, info);
        return valueAssemblers.toArray(new AbstractPrimitiveValueAssembler[0]);
    }

    public AbstractValueAssembler getRootAssembler() {
        return rootAssembler;
    }

    @Override
    public AbstractValueAssembler visit(ObjectSchemaNode objectNode, AssemblerInfo info) throws HyracksDataException {
        ObjectValueAssembler objectAssembler = new ObjectValueAssembler(level, info);
        level++;

        BitSet declaredFields = handleDeclaredFields(objectNode, info, objectAssembler);
        IntList childrenFieldNameIndexes = objectNode.getChildrenFieldNameIndexes();
        int numberOfAddedChildren = declaredFields.cardinality();
        if (numberOfAddedChildren < childrenFieldNameIndexes.size()) {
            // Now handle any open fields
            for (int i = 0; i < childrenFieldNameIndexes.size(); i++) {
                int fieldNameIdx = childrenFieldNameIndexes.getInt(i);
                AbstractSchemaNode childNode = objectNode.getChild(fieldNameIdx);
                if (fieldNameIdx == FieldNamesDictionary.DUMMY_FIELD_NAME_INDEX || !declaredFields.get(fieldNameIdx)) {
                    numberOfAddedChildren++;
                    IAType childType = getChildType(childNode, BuiltinType.ANY);
                    IValueReference fieldName = columnMetadata.getFieldNamesDictionary().getFieldName(fieldNameIdx);
                    //The last child should be a delegate
                    boolean delegate = numberOfAddedChildren == childrenFieldNameIndexes.size();
                    AssemblerInfo childInfo = new AssemblerInfo(childType, objectAssembler, delegate, fieldName);
                    childNode.accept(this, childInfo);
                }
            }
        }

        level--;
        return objectAssembler;
    }

    private BitSet handleDeclaredFields(ObjectSchemaNode objectNode, AssemblerInfo info,
            ObjectValueAssembler objectAssembler) throws HyracksDataException {
        ARecordType declaredType = (ARecordType) info.getDeclaredType();
        if (declaredType == DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE) {
            return NO_DECLARED_FIELDS;
        }
        BitSet processedFields = new BitSet();
        String[] declaredFieldNames = declaredType.getFieldNames();
        IAType[] declaredFieldTypes = declaredType.getFieldTypes();

        int addedChildren = 0;
        int requestedChildren = objectNode.getChildren().size();
        for (int i = 0; i < declaredFieldTypes.length; i++) {
            String fieldName = declaredFieldNames[i];
            int fieldNameIndex = columnMetadata.getFieldNamesDictionary().getFieldNameIndex(fieldName);
            //Check if the declared field was requested
            AbstractSchemaNode childNode = objectNode.getChild(fieldNameIndex);
            if (childNode.getTypeTag() != ATypeTag.MISSING) {
                addedChildren++;
                IAType childType = getChildType(childNode, declaredFieldTypes[i]);
                processedFields.set(fieldNameIndex);
                boolean delegate = addedChildren == requestedChildren;
                AssemblerInfo childInfo = new AssemblerInfo(childType, objectAssembler, delegate, i);
                childNode.accept(this, childInfo);
            }
        }
        return processedFields;
    }

    @Override
    public AbstractValueAssembler visit(AbstractCollectionSchemaNode collectionNode, AssemblerInfo info)
            throws HyracksDataException {
        AbstractCollectionType declaredType = (AbstractCollectionType) info.getDeclaredType();
        AbstractSchemaNode itemNode = collectionNode.getItemNode();

        ArrayValueAssembler arrayAssembler = itemNode.getTypeTag() == ATypeTag.UNION
                ? new ArrayWithUnionValueAssembler(level, info, valueAssemblers.size(), itemNode)
                : new ArrayValueAssembler(level, info, valueAssemblers.size());
        delimiters.add(level - 1);
        level++;

        RepeatedPrimitiveValueAssembler previousDelegate = delegateAssembler;
        delegateAssembler = null;

        IAType itemDeclaredType = getChildType(itemNode, declaredType.getItemType());
        AssemblerInfo itemInfo = new AssemblerInfo(itemDeclaredType, arrayAssembler, false);
        itemNode.accept(this, itemInfo);

        // if delegateAssembler is null, that means no column will be accessed
        if (delegateAssembler != null) {
            // Set repeated assembler as a delegate (responsible for writing null values)
            delegateAssembler.setAsDelegate(level - 1);
            IColumnValuesReader reader = delegateAssembler.getReader();
            int numberOfDelimiters = reader.getNumberOfDelimiters();
            // End of group assembler is responsible to finalize array/multiset builders
            EndOfRepeatedGroupAssembler endOfGroupAssembler =
                    new EndOfRepeatedGroupAssembler(reader, arrayAssembler, numberOfDelimiters - delimiters.size());
            valueAssemblers.add(endOfGroupAssembler);
        }

        level--;
        delimiters.removeInt(delimiters.size() - 1);
        if (previousDelegate != null && !delimiters.isEmpty()) {
            // Return the delegate assembler to the previous one
            delegateAssembler = previousDelegate;
        }
        return arrayAssembler;
    }

    @Override
    public AbstractValueAssembler visit(UnionSchemaNode unionNode, AssemblerInfo info) throws HyracksDataException {
        /*
         * UnionSchemaNode does not actually exist. We know the parent of the union could have items of multiple types.
         * Thus, the union's parent is the actual parent for all the union types
         */
        Collection<AbstractSchemaNode> children = unionNode.getChildren().values();
        int index = 0;
        for (AbstractSchemaNode node : children) {
            IAType unionDeclaredType = getChildType(node, info.getDeclaredType());
            boolean delegate = info.isDelegate() && index++ == children.size() - 1;
            AssemblerInfo unionInfo = new AssemblerInfo(unionDeclaredType, info.getParent(), delegate,
                    info.getFieldName(), info.getFieldIndex(), true);
            node.accept(this, unionInfo);
        }
        return info.getParent();
    }

    @Override
    public AbstractValueAssembler visit(PrimitiveSchemaNode primitiveNode, AssemblerInfo info) {
        AbstractPrimitiveValueAssembler assembler;
        IValueGetter valueGetter = valueGetterFactory.createValueGetter(getTypeTag(info, primitiveNode));
        if (!delimiters.isEmpty()) {
            IColumnValuesReader reader = readerFactory.createValueReader(primitiveNode.getTypeTag(),
                    primitiveNode.getColumnIndex(), level, getDelimiters());

            assembler = new RepeatedPrimitiveValueAssembler(level, info, reader, valueGetter);
            setDelegate(reader, (RepeatedPrimitiveValueAssembler) assembler);

        } else {
            IColumnValuesReader reader;
            boolean primaryKey = primitiveNode.isPrimaryKey();
            if (primaryKey) {
                reader = primaryKeyReaders.get(primitiveNode.getColumnIndex());
            } else {
                reader = readerFactory.createValueReader(primitiveNode.getTypeTag(), primitiveNode.getColumnIndex(),
                        level, false);
            }
            assembler = new PrimitiveValueAssembler(level, info, reader, valueGetter, primaryKey);
        }
        valueAssemblers.add(assembler);
        return assembler;
    }

    private ATypeTag getTypeTag(AssemblerInfo info, PrimitiveSchemaNode primitiveNode) {
        IAType declaredType = info.getDeclaredType();

        if (declaredType.getTypeTag() == ATypeTag.ANY) {
            return primitiveNode.getTypeTag();
        }

        // Declared types are not (and cannot be) normalized
        return declaredType.getTypeTag();
    }

    private int[] getDelimiters() {
        int numOfDelimiters = delimiters.size();
        int[] reversed = new int[numOfDelimiters];
        for (int i = 0; i < numOfDelimiters; i++) {
            reversed[i] = delimiters.getInt(numOfDelimiters - i - 1);
        }
        return reversed;
    }

    private IAType getChildType(AbstractSchemaNode childNode, IAType childType) {
        if (childType.getTypeTag() != ATypeTag.ANY) {
            return childType;
        }
        ATypeTag childTypeTag = childNode.getTypeTag();
        if (childTypeTag == ATypeTag.UNION) {
            //Union type could return any type
            return BuiltinType.ANY;
        } else if (childTypeTag.isDerivedType()) {
            return DefaultOpenFieldType.getDefaultOpenFieldType(childTypeTag);
        } else {
            return BuiltinType.getBuiltinType(childTypeTag);
        }
    }

    private void setDelegate(IColumnValuesReader reader, RepeatedPrimitiveValueAssembler assembler) {
        int delegateIndex =
                delegateAssembler == null ? Integer.MAX_VALUE : delegateAssembler.getReader().getColumnIndex();
        if (delegateIndex > reader.getColumnIndex()) {
            delegateAssembler = assembler;
        }
    }
}
