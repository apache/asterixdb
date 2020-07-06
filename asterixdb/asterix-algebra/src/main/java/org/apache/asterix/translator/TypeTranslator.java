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

import static org.apache.asterix.om.types.BuiltinType.ANY;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.annotations.IRecordFieldDataGen;
import org.apache.asterix.common.annotations.RecordDataGenAnnotation;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.lang.common.expression.OrderedListTypeDefinition;
import org.apache.asterix.lang.common.expression.RecordTypeDefinition;
import org.apache.asterix.lang.common.expression.RecordTypeDefinition.RecordKind;
import org.apache.asterix.lang.common.expression.TypeExpression;
import org.apache.asterix.lang.common.expression.TypeReferenceExpression;
import org.apache.asterix.lang.common.expression.UnorderedListTypeDefinition;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.entities.BuiltinTypeMap;
import org.apache.asterix.metadata.entities.Datatype;
import org.apache.asterix.metadata.utils.MetadataConstants;
import org.apache.asterix.metadata.utils.TypeUtil;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.AbstractComplexType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeSignature;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class TypeTranslator {

    private TypeTranslator() {
    }

    public static Map<TypeSignature, IAType> computeTypes(DataverseName typeDataverse, String typeName,
            TypeExpression typeExpr, DataverseName defaultDataverse, MetadataTransactionContext mdTxnCtx)
            throws AlgebricksException {
        Map<TypeSignature, IAType> typeMap = new HashMap<>();
        computeTypes(typeDataverse, typeName, typeExpr, defaultDataverse, mdTxnCtx, typeMap);
        return typeMap;
    }

    public static void computeTypes(DataverseName typeDataverse, String typeName, TypeExpression typeExpr,
            DataverseName defaultDataverse, MetadataTransactionContext mdTxnCtx, Map<TypeSignature, IAType> outTypeMap)
            throws AlgebricksException {
        Map<String, Map<ARecordType, List<Integer>>> incompleteFieldTypes = new HashMap<>();
        Map<TypeSignature, List<AbstractCollectionType>> incompleteItemTypes = new HashMap<>();
        Map<TypeSignature, List<TypeSignature>> incompleteTopLevelTypeReferences = new HashMap<>();
        firstPass(typeDataverse, typeName, typeExpr, outTypeMap, incompleteFieldTypes, incompleteItemTypes,
                incompleteTopLevelTypeReferences, typeDataverse);
        secondPass(mdTxnCtx, outTypeMap, incompleteFieldTypes, incompleteItemTypes, incompleteTopLevelTypeReferences,
                typeDataverse, typeExpr.getSourceLocation());

        for (IAType type : outTypeMap.values()) {
            if (type.getTypeTag().isDerivedType()) {
                ((AbstractComplexType) type).generateNestedDerivedTypeNames();
            }
        }
    }

    private static void firstPass(DataverseName typeDataverse, String typeName, TypeExpression typeExpr,
            Map<TypeSignature, IAType> outTypeMap, Map<String, Map<ARecordType, List<Integer>>> incompleteFieldTypes,
            Map<TypeSignature, List<AbstractCollectionType>> incompleteItemTypes,
            Map<TypeSignature, List<TypeSignature>> incompleteTopLevelTypeReferences, DataverseName defaultDataverse)
            throws AlgebricksException {

        if (BuiltinTypeMap.getBuiltinType(typeName) != null) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, typeExpr.getSourceLocation(),
                    "Cannot redefine builtin type " + typeName);
        }
        TypeSignature typeSignature = new TypeSignature(typeDataverse, typeName);
        switch (typeExpr.getTypeKind()) {
            case TYPEREFERENCE: {
                TypeReferenceExpression tre = (TypeReferenceExpression) typeExpr;
                TypeSignature treSignature = createTypeSignature(tre, defaultDataverse);
                IAType t = solveTypeReference(treSignature, outTypeMap);
                if (t != null) {
                    outTypeMap.put(typeSignature, t);
                } else {
                    addIncompleteTopLevelTypeReference(typeSignature, treSignature, incompleteTopLevelTypeReferences);
                }
                break;
            }
            case RECORD: {
                RecordTypeDefinition rtd = (RecordTypeDefinition) typeExpr;
                ARecordType recType = computeRecordType(typeSignature, rtd, outTypeMap, incompleteFieldTypes,
                        incompleteItemTypes, typeDataverse);
                outTypeMap.put(typeSignature, recType);
                break;
            }
            case ORDEREDLIST: {
                OrderedListTypeDefinition oltd = (OrderedListTypeDefinition) typeExpr;
                AOrderedListType olType = computeOrderedListType(typeSignature, oltd, outTypeMap, incompleteItemTypes,
                        incompleteFieldTypes, typeDataverse);
                outTypeMap.put(typeSignature, olType);
                break;
            }
            case UNORDEREDLIST: {
                UnorderedListTypeDefinition ultd = (UnorderedListTypeDefinition) typeExpr;
                AUnorderedListType ulType = computeUnorderedListType(typeSignature, ultd, outTypeMap,
                        incompleteItemTypes, incompleteFieldTypes, typeDataverse);
                outTypeMap.put(typeSignature, ulType);
                break;
            }
            default: {
                throw new IllegalStateException();
            }
        }
    }

    private static void secondPass(MetadataTransactionContext mdTxnCtx, Map<TypeSignature, IAType> typeMap,
            Map<String, Map<ARecordType, List<Integer>>> incompleteFieldTypes,
            Map<TypeSignature, List<AbstractCollectionType>> incompleteItemTypes,
            Map<TypeSignature, List<TypeSignature>> incompleteTopLevelTypeReferences, DataverseName typeDataverse,
            SourceLocation sourceLoc) throws AlgebricksException {
        // solve remaining top level references
        for (TypeSignature typeSignature : incompleteTopLevelTypeReferences.keySet()) {
            IAType t;
            Datatype dt = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, typeSignature.getDataverseName(),
                    typeSignature.getName());
            if (dt == null) {
                throw new CompilationException(ErrorCode.UNKNOWN_TYPE, sourceLoc, typeSignature.getName());
            } else {
                t = dt.getDatatype();
            }
            for (TypeSignature sign : incompleteTopLevelTypeReferences.get(typeSignature)) {
                typeMap.put(sign, t);
            }
        }
        // solve remaining field type references
        for (String trefName : incompleteFieldTypes.keySet()) {
            IAType t;
            Datatype dt = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, typeDataverse, trefName);
            if (dt == null) {
                dt = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, MetadataConstants.METADATA_DATAVERSE_NAME,
                        trefName);
            }
            if (dt == null) {
                throw new CompilationException(ErrorCode.UNKNOWN_TYPE, sourceLoc, trefName);
            } else {
                t = dt.getDatatype();
            }
            Map<ARecordType, List<Integer>> fieldsToFix = incompleteFieldTypes.get(trefName);
            for (ARecordType recType : fieldsToFix.keySet()) {
                List<Integer> positions = fieldsToFix.get(recType);
                IAType[] fldTypes = recType.getFieldTypes();
                for (Integer pos : positions) {
                    if (fldTypes[pos] == null) {
                        fldTypes[pos] = t;
                    } else { // nullable
                        AUnionType nullableUnion = (AUnionType) fldTypes[pos];
                        nullableUnion.setActualType(t);
                    }
                }
            }
        }

        // solve remaining item type references
        for (TypeSignature typeSignature : incompleteItemTypes.keySet()) {
            IAType t;
            Datatype dt;
            if (MetadataManager.INSTANCE != null) {
                dt = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, typeSignature.getDataverseName(),
                        typeSignature.getName());
                if (dt == null) {
                    throw new CompilationException(ErrorCode.UNKNOWN_TYPE, sourceLoc, typeSignature.getName());
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
            Map<String, Map<ARecordType, List<Integer>>> incompleteFieldTypes, DataverseName defaultDataverse)
            throws AlgebricksException {
        TypeExpression tExpr = oltd.getItemTypeExpression();
        String typeName = typeSignature != null ? typeSignature.getName() : null;
        AOrderedListType aolt = new AOrderedListType(ANY, typeName);
        setCollectionItemType(tExpr, typeMap, incompleteItemTypes, incompleteFieldTypes, aolt, defaultDataverse);
        return aolt;
    }

    private static AUnorderedListType computeUnorderedListType(TypeSignature typeSignature,
            UnorderedListTypeDefinition ultd, Map<TypeSignature, IAType> typeMap,
            Map<TypeSignature, List<AbstractCollectionType>> incompleteItemTypes,
            Map<String, Map<ARecordType, List<Integer>>> incompleteFieldTypes, DataverseName defaulDataverse)
            throws AlgebricksException {
        TypeExpression tExpr = ultd.getItemTypeExpression();
        String typeName = typeSignature != null ? typeSignature.getName() : null;
        AUnorderedListType ault = new AUnorderedListType(ANY, typeName);
        setCollectionItemType(tExpr, typeMap, incompleteItemTypes, incompleteFieldTypes, ault, defaulDataverse);
        return ault;
    }

    private static void setCollectionItemType(TypeExpression tExpr, Map<TypeSignature, IAType> typeMap,
            Map<TypeSignature, List<AbstractCollectionType>> incompleteItemTypes,
            Map<String, Map<ARecordType, List<Integer>>> incompleteFieldTypes, AbstractCollectionType act,
            DataverseName defaultDataverse) throws AlgebricksException {
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
                TypeSignature treSignature = createTypeSignature(tre, defaultDataverse);
                IAType tref = solveTypeReference(treSignature, typeMap);
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
            DataverseName defaultDataverse) {
        TypeSignature typeSignature = createTypeSignature(tre, defaultDataverse);
        List<AbstractCollectionType> typeList =
                incompleteItemTypes.computeIfAbsent(typeSignature, k -> new ArrayList<>());
        typeList.add(collType);
    }

    private static void addIncompleteFieldTypeReference(ARecordType recType, int fldPosition,
            TypeReferenceExpression tre, Map<String, Map<ARecordType, List<Integer>>> incompleteFieldTypes) {
        String typeName = tre.getIdent().second.getValue();
        Map<ARecordType, List<Integer>> refMap = incompleteFieldTypes.computeIfAbsent(typeName, k -> new HashMap<>());
        List<Integer> typeList = refMap.computeIfAbsent(recType, k -> new ArrayList<>());
        typeList.add(fldPosition);
    }

    private static void addIncompleteTopLevelTypeReference(TypeSignature typeSignature, TypeSignature treSignature,
            Map<TypeSignature, List<TypeSignature>> incompleteTopLevelTypeReferences) {
        List<TypeSignature> refList =
                incompleteTopLevelTypeReferences.computeIfAbsent(treSignature, k -> new ArrayList<>());
        refList.add(typeSignature);
    }

    private static IAType solveTypeReference(TypeSignature typeSignature, Map<TypeSignature, IAType> typeMap) {
        IAType builtin = BuiltinTypeMap.getBuiltinType(typeSignature.getName());
        if (builtin != null) {
            return builtin;
        } else {
            return typeMap.get(typeSignature);
        }
    }

    private static ARecordType computeRecordType(TypeSignature typeSignature, RecordTypeDefinition rtd,
            Map<TypeSignature, IAType> typeMap, Map<String, Map<ARecordType, List<Integer>>> incompleteFieldTypes,
            Map<TypeSignature, List<AbstractCollectionType>> incompleteItemTypes, DataverseName defaultDataverse)
            throws AlgebricksException {
        List<String> names = rtd.getFieldNames();
        int n = names.size();
        String[] fldNames = new String[n];
        IAType[] fldTypes = new IAType[n];
        int i = 0;
        for (String s : names) {
            fldNames[i++] = s;
        }
        boolean isOpen = rtd.getRecordKind() == RecordKind.OPEN;
        ARecordType recType =
                new ARecordType(typeSignature == null ? null : typeSignature.getName(), fldNames, fldTypes, isOpen);
        List<IRecordFieldDataGen> fieldDataGen = rtd.getFieldDataGen();
        if (fieldDataGen.size() == n) {
            IRecordFieldDataGen[] rfdg = new IRecordFieldDataGen[n];
            rfdg = fieldDataGen.toArray(rfdg);
            recType.getAnnotations().add(new RecordDataGenAnnotation(rfdg, rtd.getUndeclaredFieldsDataGen()));
        }

        for (int j = 0; j < n; j++) {
            TypeExpression texpr = rtd.getFieldTypes().get(j);
            IAType type;
            switch (texpr.getTypeKind()) {
                case TYPEREFERENCE: {
                    TypeReferenceExpression tre = (TypeReferenceExpression) texpr;
                    TypeSignature signature = createTypeSignature(tre, defaultDataverse);
                    type = solveTypeReference(signature, typeMap);
                    if (type == null) {
                        addIncompleteFieldTypeReference(recType, j, tre, incompleteFieldTypes);
                    }
                    break;
                }
                case RECORD: {
                    RecordTypeDefinition recTypeDef2 = (RecordTypeDefinition) texpr;
                    type = computeRecordType(null, recTypeDef2, typeMap, incompleteFieldTypes, incompleteItemTypes,
                            defaultDataverse);
                    break;
                }
                case ORDEREDLIST: {
                    OrderedListTypeDefinition oltd = (OrderedListTypeDefinition) texpr;
                    type = computeOrderedListType(null, oltd, typeMap, incompleteItemTypes, incompleteFieldTypes,
                            defaultDataverse);
                    break;
                }
                case UNORDEREDLIST: {
                    UnorderedListTypeDefinition ultd = (UnorderedListTypeDefinition) texpr;
                    type = computeUnorderedListType(null, ultd, typeMap, incompleteItemTypes, incompleteFieldTypes,
                            defaultDataverse);
                    break;
                }
                default: {
                    throw new IllegalStateException();
                }
            }

            Boolean nullable = rtd.getNullableFields().get(j);
            Boolean missable = rtd.getMissableFields().get(j);
            fldTypes[j] = TypeUtil.createQuantifiedType(type, nullable, missable);
        }

        return recType;
    }

    private static TypeSignature createTypeSignature(TypeReferenceExpression tre, DataverseName defaultDataverse) {
        Pair<DataverseName, Identifier> treTypeName = tre.getIdent();
        DataverseName activeDataverse = treTypeName.first == null ? defaultDataverse : treTypeName.first;
        return new TypeSignature(activeDataverse, treTypeName.second.getValue());
    }
}
