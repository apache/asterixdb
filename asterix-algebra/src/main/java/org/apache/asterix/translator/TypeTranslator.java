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

package org.apache.asterix.translator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.annotations.IRecordFieldDataGen;
import org.apache.asterix.common.annotations.RecordDataGenAnnotation;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.expression.OrderedListTypeDefinition;
import org.apache.asterix.lang.common.expression.RecordTypeDefinition;
import org.apache.asterix.lang.common.expression.RecordTypeDefinition.RecordKind;
import org.apache.asterix.lang.common.expression.TypeExpression;
import org.apache.asterix.lang.common.expression.TypeReferenceExpression;
import org.apache.asterix.lang.common.expression.UnorderedListTypeDefinition;
import org.apache.asterix.metadata.MetadataException;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.entities.AsterixBuiltinTypeMap;
import org.apache.asterix.metadata.entities.Datatype;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.AbstractComplexType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeSignature;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;

public class TypeTranslator {

    public static Map<TypeSignature, IAType> computeTypes(MetadataTransactionContext mdTxnCtx, TypeExpression typeExpr,
            String typeName, String typeDataverse) throws AlgebricksException, MetadataException {
        Map<TypeSignature, IAType> typeMap = new HashMap<TypeSignature, IAType>();
        return computeTypes(mdTxnCtx, typeExpr, typeName, typeDataverse, typeMap);
    }

    public static Map<TypeSignature, IAType> computeTypes(MetadataTransactionContext mdTxnCtx, TypeExpression typeExpr,
            String typeName, String typeDataverse, Map<TypeSignature, IAType> typeMap)
                    throws AlgebricksException, MetadataException {
        Map<String, Map<ARecordType, List<Integer>>> incompleteFieldTypes = new HashMap<String, Map<ARecordType, List<Integer>>>();
        Map<TypeSignature, List<AbstractCollectionType>> incompleteItemTypes = new HashMap<TypeSignature, List<AbstractCollectionType>>();
        Map<TypeSignature, List<TypeSignature>> incompleteTopLevelTypeReferences = new HashMap<TypeSignature, List<TypeSignature>>();
        firstPass(typeExpr, typeName, typeMap, incompleteFieldTypes, incompleteItemTypes,
                incompleteTopLevelTypeReferences, typeDataverse);
        secondPass(mdTxnCtx, typeMap, incompleteFieldTypes, incompleteItemTypes, incompleteTopLevelTypeReferences,
                typeDataverse);

        for (IAType type : typeMap.values())
            if (type.getTypeTag().isDerivedType())
                ((AbstractComplexType) type).generateNestedDerivedTypeNames();
        return typeMap;
    }

    private static Map<String, BuiltinType> builtinTypeMap = AsterixBuiltinTypeMap.getBuiltinTypes();

    private static void firstPass(TypeExpression typeExpr, String typeName, Map<TypeSignature, IAType> typeMap,
            Map<String, Map<ARecordType, List<Integer>>> incompleteFieldTypes,
            Map<TypeSignature, List<AbstractCollectionType>> incompleteItemTypes,
            Map<TypeSignature, List<TypeSignature>> incompleteTopLevelTypeReferences, String typeDataverse)
                    throws AlgebricksException {

        if (builtinTypeMap.get(typeName) != null) {
            throw new AlgebricksException("Cannot redefine builtin type " + typeName + " .");
        }
        TypeSignature typeSignature = new TypeSignature(typeDataverse, typeName);
        try {
            switch (typeExpr.getTypeKind()) {
                case TYPEREFERENCE: {
                    TypeReferenceExpression tre = (TypeReferenceExpression) typeExpr;
                    IAType t = solveTypeReference(new TypeSignature(typeDataverse, tre.getIdent().getValue()), typeMap);
                    if (t != null) {
                        typeMap.put(typeSignature, t);
                    } else {
                        addIncompleteTopLevelTypeReference(typeName, tre, incompleteTopLevelTypeReferences,
                                typeDataverse);
                    }
                    break;
                }
                case RECORD: {
                    RecordTypeDefinition rtd = (RecordTypeDefinition) typeExpr;
                    ARecordType recType = computeRecordType(typeSignature, rtd, typeMap, incompleteFieldTypes,
                            incompleteItemTypes, typeDataverse);
                    typeMap.put(typeSignature, recType);
                    break;
                }
                case ORDEREDLIST: {
                    OrderedListTypeDefinition oltd = (OrderedListTypeDefinition) typeExpr;
                    AOrderedListType olType = computeOrderedListType(typeSignature, oltd, typeMap, incompleteItemTypes,
                            incompleteFieldTypes, typeDataverse);
                    typeMap.put(typeSignature, olType);
                    break;
                }
                case UNORDEREDLIST: {
                    UnorderedListTypeDefinition ultd = (UnorderedListTypeDefinition) typeExpr;
                    AUnorderedListType ulType = computeUnorderedListType(typeSignature, ultd, typeMap,
                            incompleteItemTypes, incompleteFieldTypes, typeDataverse);
                    typeMap.put(typeSignature, ulType);
                    break;
                }
                default: {
                    throw new IllegalStateException();
                }
            }
        } catch (AsterixException e) {
            throw new AlgebricksException(e);
        }
    }

    private static void secondPass(MetadataTransactionContext mdTxnCtx, Map<TypeSignature, IAType> typeMap,
            Map<String, Map<ARecordType, List<Integer>>> incompleteFieldTypes,
            Map<TypeSignature, List<AbstractCollectionType>> incompleteItemTypes,
            Map<TypeSignature, List<TypeSignature>> incompleteTopLevelTypeReferences, String typeDataverse)
                    throws AlgebricksException, MetadataException {
        // solve remaining top level references

        for (TypeSignature typeSignature : incompleteTopLevelTypeReferences.keySet()) {
            IAType t;// = typeMap.get(trefName);
            Datatype dt = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, typeSignature.getNamespace(),
                    typeSignature.getName());
            if (dt == null) {
                throw new AlgebricksException("Could not resolve type " + typeSignature);
            } else
                t = dt.getDatatype();
            for (TypeSignature sign : incompleteTopLevelTypeReferences.get(typeSignature)) {
                typeMap.put(sign, t);
            }
        }
        // solve remaining field type references
        for (String trefName : incompleteFieldTypes.keySet()) {
            IAType t;// = typeMap.get(trefName);
            Datatype dt = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, typeDataverse, trefName);
            if (dt == null) {
                throw new AlgebricksException("Could not resolve type " + trefName);
            } else
                t = dt.getDatatype();
            Map<ARecordType, List<Integer>> fieldsToFix = incompleteFieldTypes.get(trefName);
            for (ARecordType recType : fieldsToFix.keySet()) {
                List<Integer> positions = fieldsToFix.get(recType);
                IAType[] fldTypes = recType.getFieldTypes();
                for (Integer pos : positions) {
                    if (fldTypes[pos] == null) {
                        fldTypes[pos] = t;
                    } else { // nullable
                        AUnionType nullableUnion = (AUnionType) fldTypes[pos];
                        nullableUnion.setTypeAtIndex(t, 1);
                    }
                }
            }
        }

        // solve remaining item type references
        for (TypeSignature typeSignature : incompleteItemTypes.keySet()) {
            IAType t;// = typeMap.get(trefName);
            Datatype dt = null;
            if (MetadataManager.INSTANCE != null) {
                dt = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, typeSignature.getNamespace(),
                        typeSignature.getName());
                if (dt == null) {
                    throw new AlgebricksException("Could not resolve type " + typeSignature);
                }
                t = dt.getDatatype();
            } else {
                t = typeMap.get(typeSignature);
            }
            for (AbstractCollectionType act : incompleteItemTypes.get(typeSignature)) {
                act.setItemType(t);
            }
        }
    }

    private static AOrderedListType computeOrderedListType(TypeSignature typeSignature, OrderedListTypeDefinition oltd,
            Map<TypeSignature, IAType> typeMap, Map<TypeSignature, List<AbstractCollectionType>> incompleteItemTypes,
            Map<String, Map<ARecordType, List<Integer>>> incompleteFieldTypes, String defaultDataverse)
                    throws AsterixException {
        TypeExpression tExpr = oltd.getItemTypeExpression();
        String typeName = typeSignature != null ? typeSignature.getName() : null;
        AOrderedListType aolt = new AOrderedListType(null, typeName);
        setCollectionItemType(tExpr, typeMap, incompleteItemTypes, incompleteFieldTypes, aolt, defaultDataverse);
        return aolt;
    }

    private static AUnorderedListType computeUnorderedListType(TypeSignature typeSignature,
            UnorderedListTypeDefinition ultd, Map<TypeSignature, IAType> typeMap,
            Map<TypeSignature, List<AbstractCollectionType>> incompleteItemTypes,
            Map<String, Map<ARecordType, List<Integer>>> incompleteFieldTypes, String defaulDataverse)
                    throws AsterixException {
        TypeExpression tExpr = ultd.getItemTypeExpression();
        String typeName = typeSignature != null ? typeSignature.getName() : null;
        AUnorderedListType ault = new AUnorderedListType(null, typeName);
        setCollectionItemType(tExpr, typeMap, incompleteItemTypes, incompleteFieldTypes, ault, defaulDataverse);
        return ault;
    }

    private static void setCollectionItemType(TypeExpression tExpr, Map<TypeSignature, IAType> typeMap,
            Map<TypeSignature, List<AbstractCollectionType>> incompleteItemTypes,
            Map<String, Map<ARecordType, List<Integer>>> incompleteFieldTypes, AbstractCollectionType act,
            String defaultDataverse) throws AsterixException {
        switch (tExpr.getTypeKind()) {
            case ORDEREDLIST: {
                OrderedListTypeDefinition oltd = (OrderedListTypeDefinition) tExpr;
                IAType t = computeOrderedListType(null, oltd, typeMap, incompleteItemTypes, incompleteFieldTypes,
                        defaultDataverse);
                act.setItemType(t);
                break;
            }
            case UNORDEREDLIST: {
                UnorderedListTypeDefinition ultd = (UnorderedListTypeDefinition) tExpr;
                IAType t = computeUnorderedListType(null, ultd, typeMap, incompleteItemTypes, incompleteFieldTypes,
                        defaultDataverse);
                act.setItemType(t);
                break;
            }
            case RECORD: {
                RecordTypeDefinition rtd = (RecordTypeDefinition) tExpr;
                IAType t = computeRecordType(null, rtd, typeMap, incompleteFieldTypes, incompleteItemTypes,
                        defaultDataverse);
                act.setItemType(t);
                break;
            }
            case TYPEREFERENCE: {
                TypeReferenceExpression tre = (TypeReferenceExpression) tExpr;
                TypeSignature signature = new TypeSignature(defaultDataverse, tre.getIdent().getValue());
                IAType tref = solveTypeReference(signature, typeMap);
                if (tref != null) {
                    act.setItemType(tref);
                } else {
                    addIncompleteCollectionTypeReference(act, tre, incompleteItemTypes, defaultDataverse);
                }
                break;
            }
            default: {
                throw new IllegalStateException();
            }
        }
    }

    private static void addIncompleteCollectionTypeReference(AbstractCollectionType collType,
            TypeReferenceExpression tre, Map<TypeSignature, List<AbstractCollectionType>> incompleteItemTypes,
            String defaultDataverse) {
        String typeName = tre.getIdent().getValue();
        TypeSignature typeSignature = new TypeSignature(defaultDataverse, typeName);
        List<AbstractCollectionType> typeList = incompleteItemTypes.get(typeSignature);
        if (typeList == null) {
            typeList = new LinkedList<AbstractCollectionType>();
            incompleteItemTypes.put(typeSignature, typeList);
        }
        typeList.add(collType);
    }

    private static void addIncompleteFieldTypeReference(ARecordType recType, int fldPosition,
            TypeReferenceExpression tre, Map<String, Map<ARecordType, List<Integer>>> incompleteFieldTypes) {
        String typeName = tre.getIdent().getValue();
        Map<ARecordType, List<Integer>> refMap = incompleteFieldTypes.get(typeName);
        if (refMap == null) {
            refMap = new HashMap<ARecordType, List<Integer>>();
            incompleteFieldTypes.put(typeName, refMap);
        }
        List<Integer> typeList = refMap.get(recType);
        if (typeList == null) {
            typeList = new ArrayList<Integer>();
            refMap.put(recType, typeList);
        }
        typeList.add(fldPosition);
    }

    private static void addIncompleteTopLevelTypeReference(String tdeclName, TypeReferenceExpression tre,
            Map<TypeSignature, List<TypeSignature>> incompleteTopLevelTypeReferences, String defaultDataverse) {
        String name = tre.getIdent().getValue();
        TypeSignature typeSignature = new TypeSignature(defaultDataverse, name);
        List<TypeSignature> refList = incompleteTopLevelTypeReferences.get(name);
        if (refList == null) {
            refList = new LinkedList<TypeSignature>();
            incompleteTopLevelTypeReferences.put(new TypeSignature(defaultDataverse, tre.getIdent().getValue()),
                    refList);
        }
        refList.add(typeSignature);
    }

    private static IAType solveTypeReference(TypeSignature typeSignature, Map<TypeSignature, IAType> typeMap) {
        IAType builtin = builtinTypeMap.get(typeSignature.getName());
        if (builtin != null) {
            return builtin;
        } else {
            return typeMap.get(typeSignature);
        }
    }

    private static ARecordType computeRecordType(TypeSignature typeSignature, RecordTypeDefinition rtd,
            Map<TypeSignature, IAType> typeMap, Map<String, Map<ARecordType, List<Integer>>> incompleteFieldTypes,
            Map<TypeSignature, List<AbstractCollectionType>> incompleteItemTypes, String defaultDataverse)
                    throws AsterixException {
        List<String> names = rtd.getFieldNames();
        int n = names.size();
        String[] fldNames = new String[n];
        IAType[] fldTypes = new IAType[n];
        int i = 0;
        for (String s : names) {
            fldNames[i++] = s;
        }
        boolean isOpen = rtd.getRecordKind() == RecordKind.OPEN;
        ARecordType recType = new ARecordType(typeSignature == null ? null : typeSignature.getName(), fldNames,
                fldTypes, isOpen);
        List<IRecordFieldDataGen> fieldDataGen = rtd.getFieldDataGen();
        if (fieldDataGen.size() == n) {
            IRecordFieldDataGen[] rfdg = new IRecordFieldDataGen[n];
            rfdg = fieldDataGen.toArray(rfdg);
            recType.getAnnotations().add(new RecordDataGenAnnotation(rfdg, rtd.getUndeclaredFieldsDataGen()));
        }

        for (int j = 0; j < n; j++) {
            TypeExpression texpr = rtd.getFieldTypes().get(j);
            switch (texpr.getTypeKind()) {
                case TYPEREFERENCE: {
                    TypeReferenceExpression tre = (TypeReferenceExpression) texpr;
                    TypeSignature signature = new TypeSignature(defaultDataverse, tre.getIdent().getValue());
                    IAType tref = solveTypeReference(signature, typeMap);
                    if (tref != null) {
                        if (!rtd.getNullableFields().get(j)) { // not nullable
                            fldTypes[j] = tref;
                        } else { // nullable
                            fldTypes[j] = AUnionType.createNullableType(tref);
                        }
                    } else {
                        addIncompleteFieldTypeReference(recType, j, tre, incompleteFieldTypes);
                        if (rtd.getNullableFields().get(j)) {
                            fldTypes[j] = AUnionType.createNullableType(null);
                        }
                    }
                    break;
                }
                case RECORD: {
                    RecordTypeDefinition recTypeDef2 = (RecordTypeDefinition) texpr;
                    IAType t2 = computeRecordType(null, recTypeDef2, typeMap, incompleteFieldTypes, incompleteItemTypes,
                            defaultDataverse);
                    if (!rtd.getNullableFields().get(j)) { // not nullable
                        fldTypes[j] = t2;
                    } else { // nullable
                        fldTypes[j] = AUnionType.createNullableType(t2);
                    }
                    break;
                }
                case ORDEREDLIST: {
                    OrderedListTypeDefinition oltd = (OrderedListTypeDefinition) texpr;
                    IAType t2 = computeOrderedListType(null, oltd, typeMap, incompleteItemTypes, incompleteFieldTypes,
                            defaultDataverse);
                    fldTypes[j] = (rtd.getNullableFields().get(j)) ? AUnionType.createNullableType(t2) : t2;
                    break;
                }
                case UNORDEREDLIST: {
                    UnorderedListTypeDefinition ultd = (UnorderedListTypeDefinition) texpr;
                    IAType t2 = computeUnorderedListType(null, ultd, typeMap, incompleteItemTypes, incompleteFieldTypes,
                            defaultDataverse);
                    fldTypes[j] = (rtd.getNullableFields().get(j)) ? AUnionType.createNullableType(t2) : t2;
                    break;
                }
                default: {
                    throw new IllegalStateException();
                }
            }

        }

        return recType;
    }
}
