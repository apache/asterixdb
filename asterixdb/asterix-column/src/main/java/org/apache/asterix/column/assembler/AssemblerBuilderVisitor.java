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

import static org.apache.asterix.om.dictionary.AbstractFieldNamesDictionary.DUMMY_FIELD_NAME_INDEX;
import static org.apache.asterix.om.dictionary.AbstractFieldNamesDictionary.MAP_FIELD_NAME_INDEX;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.column.assembler.value.IValueGetter;
import org.apache.asterix.column.assembler.value.IValueGetterFactory;
import org.apache.asterix.column.metadata.schema.AbstractSchemaNode;
import org.apache.asterix.column.metadata.schema.ISchemaNodeVisitor;
import org.apache.asterix.column.metadata.schema.ObjectSchemaNode;
import org.apache.asterix.column.metadata.schema.UnionSchemaNode;
import org.apache.asterix.column.metadata.schema.collection.AbstractCollectionSchemaNode;
import org.apache.asterix.column.metadata.schema.collection.MapSchemaNode;
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
    private List<IColumnValuesReader> endOfGroupPrimitiveReaders;

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
        this.endOfGroupPrimitiveReaders = new ArrayList<>();
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
        IntList childrenFieldNameIndexes = objectNode.getChildrenFieldNameIndexes();
        int effectiveNumberOfChildren = childrenFieldNameIndexes.size();
        ObjectValueAssembler objectAssembler = new ObjectValueAssembler(level, info);
        level++;
        BitSet declaredFields = handleDeclaredFields(objectNode, info, objectAssembler, objectNode.isCollapsed());
        int numberOfAddedChildren = declaredFields.cardinality();
        if (numberOfAddedChildren < childrenFieldNameIndexes.size()) {
            // Now handle any open fields
            for (int i = 0; i < childrenFieldNameIndexes.size(); i++) {
                int fieldNameIdx = childrenFieldNameIndexes.getInt(i);
                if (fieldNameIdx == MAP_FIELD_NAME_INDEX) {
                    // Visited after the loop, see below
                    continue;
                }
                AbstractSchemaNode childNode = objectNode.getChild(fieldNameIdx);
                if (fieldNameIdx == DUMMY_FIELD_NAME_INDEX || !declaredFields.get(fieldNameIdx)) {
                    numberOfAddedChildren++;
                    IAType childType = getChildType(childNode, BuiltinType.ANY);
                    IValueReference fieldName = columnMetadata.getFieldNamesDictionary().getFieldName(fieldNameIdx);
                    boolean delegate =
                            !objectNode.isCollapsed() && numberOfAddedChildren == childrenFieldNameIndexes.size();
                    AssemblerInfo childInfo = new AssemblerInfo(childType, objectAssembler, delegate, fieldName);
                    if (childNode instanceof UnionSchemaNode) {
                        //UnionSchemaNode does not actually exist. We know the parent of the union could have items of multiple types.
                        //Thus, the union's parent is the actual parent for all the union types
                        effectiveNumberOfChildren += ((UnionSchemaNode) childNode).getChildren().size() - 1;
                    }
                    childNode.accept(this, childInfo);
                }
            }
            /*
             * The collapsed child must be the delegate — the delegate is whichever child ends the parent record — and
             * so it must also be visited last: were it visited before another child, it would end the record, and
             * that child's start() would then reset the record builder and drop everything already assembled.
             */
            MapSchemaNode mapNode = objectNode.getMapNode();
            if (mapNode != null) {
                mapNode.accept(this, new AssemblerInfo(BuiltinType.ANY, objectAssembler, true));
            }
        }

        objectAssembler.setNumberOfEffectiveFields(effectiveNumberOfChildren);
        level--;
        return objectAssembler;
    }

    private BitSet handleDeclaredFields(ObjectSchemaNode objectNode, AssemblerInfo info,
            ObjectValueAssembler objectAssembler, boolean mapIsDelegate) throws HyracksDataException {
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
                boolean delegate = !mapIsDelegate && addedChildren == requestedChildren;
                AssemblerInfo childInfo = new AssemblerInfo(childType, objectAssembler, delegate, i);
                childNode.accept(this, childInfo);
            }
        }
        return processedFields;
    }

    @Override
    public AbstractValueAssembler visit(AbstractCollectionSchemaNode collectionNode, AssemblerInfo info)
            throws HyracksDataException {
        if (collectionNode instanceof MapSchemaNode) {
            return visitMap((MapSchemaNode) collectionNode, info);
        }
        AbstractCollectionType declaredType = (AbstractCollectionType) info.getDeclaredType();
        AbstractSchemaNode itemNode = collectionNode.getItemNode();

        ArrayValueAssembler arrayAssembler = itemNode.getTypeTag() == ATypeTag.UNION
                ? new ArrayWithUnionValueAssembler(level, info, valueAssemblers.size(), itemNode)
                : new ArrayValueAssembler(level, info, valueAssemblers.size());
        delimiters.add(level - 1);
        level++;

        RepeatedPrimitiveValueAssembler previousDelegate = delegateAssembler;
        delegateAssembler = null;

        List<IColumnValuesReader> previousEndOfGroupPrimitiveReaders = endOfGroupPrimitiveReaders;
        endOfGroupPrimitiveReaders = new ArrayList<>();

        IAType itemDeclaredType = getChildType(itemNode, declaredType.getItemType());
        AssemblerInfo itemInfo = new AssemblerInfo(itemDeclaredType, arrayAssembler, false);
        itemNode.accept(this, itemInfo);

        // if delegateAssembler is null, that means no column will be accessed
        if (delegateAssembler != null) {
            // Set repeated assembler as a delegate (responsible for writing null values)
            IColumnValuesReader reader = delegateAssembler.getReader();
            int numberOfDelimiters = reader.getNumberOfDelimiters();
            // End of group assembler is responsible to finalize array/multiset builders
            EndOfRepeatedGroupAssembler endOfGroupAssembler = new EndOfRepeatedGroupAssembler(
                    endOfGroupPrimitiveReaders, arrayAssembler, arrayAssembler.getFirstValueIndex(), delimiters.size());
            previousEndOfGroupPrimitiveReaders.addAll(endOfGroupPrimitiveReaders);
            valueAssemblers.add(endOfGroupAssembler);
        }

        level--;
        delimiters.removeInt(delimiters.size() - 1);
        if (previousDelegate != null && !delimiters.isEmpty()) {
            // Return the delegate assembler to the previous one
            delegateAssembler = previousDelegate;
        }

        endOfGroupPrimitiveReaders = previousEndOfGroupPrimitiveReaders;
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
            endOfGroupPrimitiveReaders.add(reader);

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

    /**
     * Mirrors the collection case, with one extra child: the key column. Key is visited first so that its assembler
     * precedes the value's in the ordered assembler array, matching the order the two columns were written in and so
     * the pairing in {@link MapValueAssembler} holds.
     */
    private AbstractValueAssembler visitMap(MapSchemaNode mapNode, AssemblerInfo info) throws HyracksDataException {
        AbstractSchemaNode itemNode = mapNode.getItemNode();
        MapValueAssembler mapAssembler = new MapValueAssembler(level, info, valueAssemblers.size());
        delimiters.add(level - 1);
        level++;

        RepeatedPrimitiveValueAssembler previousDelegate = delegateAssembler;
        delegateAssembler = null;
        List<IColumnValuesReader> previousEndOfGroupPrimitiveReaders = endOfGroupPrimitiveReaders;
        endOfGroupPrimitiveReaders = new ArrayList<>();

        AssemblerInfo keyInfo = new AssemblerInfo(BuiltinType.ASTRING, mapAssembler, false);
        mapAssembler.setKeyAssembler(mapNode.getKeyNode().accept(this, keyInfo));
        mapAssembler.setRequestedKeys(mapNode.getRequestedKeys());
        boolean unionValue = itemNode != null && itemNode.getTypeTag() == ATypeTag.UNION;
        mapAssembler.setUnionValue(unionValue);
        // The key, plus each value arm: a union's arms all report to the map directly, as the union node has no assembler
        int valueArms = itemNode == null ? 0 : unionValue ? ((UnionSchemaNode) itemNode).getChildren().size() : 1;
        mapAssembler.setReporters(1 + valueArms);

        if (itemNode != null) {
            IAType itemDeclaredType = getChildType(itemNode, BuiltinType.ANY);
            itemNode.accept(this, new AssemblerInfo(itemDeclaredType, mapAssembler, false));
        }

        if (delegateAssembler != null) {
            EndOfRepeatedGroupAssembler endOfGroupAssembler = new EndOfRepeatedGroupAssembler(
                    endOfGroupPrimitiveReaders, mapAssembler, mapAssembler.getFirstValueIndex(), delimiters.size());
            previousEndOfGroupPrimitiveReaders.addAll(endOfGroupPrimitiveReaders);
            valueAssemblers.add(endOfGroupAssembler);
        }

        level--;
        delimiters.removeInt(delimiters.size() - 1);
        if (previousDelegate != null && !delimiters.isEmpty()) {
            delegateAssembler = previousDelegate;
        }
        endOfGroupPrimitiveReaders = previousEndOfGroupPrimitiveReaders;
        return mapAssembler;
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
