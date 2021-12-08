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
package org.apache.asterix.metadata.utils;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.typecomputer.impl.TypeComputeUtils;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeSignature;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * Provider utility methods for data types
 */
public class TypeUtil {

    private static final char TYPE_NAME_DELIMITER = '$';

    private static final String DATASET_INLINE_TYPE_PREFIX = "$d$t$";

    private static final String FUNCTION_INLINE_TYPE_PREFIX = "$f$t$";

    public static final String DATETIME_PARAMETER_NAME = BuiltinType.ADATETIME.getTypeName();

    public static final String DATE_PARAMETER_NAME = BuiltinType.ADATE.getTypeName();

    public static final String TIME_PARAMETER_NAME = BuiltinType.ATIME.getTypeName();

    private TypeUtil() {
    }

    /**
     * @param type type
     * @param withFormat true when needing a constructor with format
     *
     * @return a type constructor that produces NULL for MISSING input
     */
    public static FunctionIdentifier getTypeConstructorDefaultNull(IAType type, boolean withFormat) {
        switch (type.getTypeTag()) {
            case TINYINT:
                return BuiltinFunctions.INT8_DEFAULT_NULL_CONSTRUCTOR;
            case SMALLINT:
                return BuiltinFunctions.INT16_DEFAULT_NULL_CONSTRUCTOR;
            case INTEGER:
                return BuiltinFunctions.INT32_DEFAULT_NULL_CONSTRUCTOR;
            case BIGINT:
                return BuiltinFunctions.INT64_DEFAULT_NULL_CONSTRUCTOR;
            case FLOAT:
                return BuiltinFunctions.FLOAT_DEFAULT_NULL_CONSTRUCTOR;
            case DOUBLE:
                return BuiltinFunctions.DOUBLE_DEFAULT_NULL_CONSTRUCTOR;
            case BOOLEAN:
                return BuiltinFunctions.BOOLEAN_DEFAULT_NULL_CONSTRUCTOR;
            case STRING:
                return BuiltinFunctions.STRING_DEFAULT_NULL_CONSTRUCTOR;
            case DATE:
                return withFormat ? BuiltinFunctions.DATE_DEFAULT_NULL_CONSTRUCTOR_WITH_FORMAT
                        : BuiltinFunctions.DATE_DEFAULT_NULL_CONSTRUCTOR;
            case TIME:
                return withFormat ? BuiltinFunctions.TIME_DEFAULT_NULL_CONSTRUCTOR_WITH_FORMAT
                        : BuiltinFunctions.TIME_DEFAULT_NULL_CONSTRUCTOR;
            case DATETIME:
                return withFormat ? BuiltinFunctions.DATETIME_DEFAULT_NULL_CONSTRUCTOR_WITH_FORMAT
                        : BuiltinFunctions.DATETIME_DEFAULT_NULL_CONSTRUCTOR;
            case YEARMONTHDURATION:
                return BuiltinFunctions.YEAR_MONTH_DURATION_DEFAULT_NULL_CONSTRUCTOR;
            case DAYTIMEDURATION:
                return BuiltinFunctions.DAY_TIME_DURATION_DEFAULT_NULL_CONSTRUCTOR;
            case DURATION:
                return BuiltinFunctions.DURATION_DEFAULT_NULL_CONSTRUCTOR;
            case UUID:
                return BuiltinFunctions.UUID_DEFAULT_NULL_CONSTRUCTOR;
            case BINARY:
                return BuiltinFunctions.BINARY_BASE64_DEFAULT_NULL_CONSTRUCTOR;
            default:
                return null;
        }
    }

    public static Map<String, String> validateConfiguration(Map<String, String> config, SourceLocation sourceLoc)
            throws CompilationException {
        if (config == null) {
            return Collections.emptyMap();
        }
        for (Map.Entry<String, String> me : config.entrySet()) {
            String name = me.getKey();
            String value = me.getValue();
            if (DATETIME_PARAMETER_NAME.equals(name) || DATE_PARAMETER_NAME.equals(name)
                    || TIME_PARAMETER_NAME.equals(name)) {
                if (value == null) {
                    throw new CompilationException(ErrorCode.INVALID_REQ_PARAM_VAL, sourceLoc, name, "null");
                }
            } else {
                throw new CompilationException(ErrorCode.ILLEGAL_SET_PARAMETER, sourceLoc, name);
            }
        }
        return config;
    }

    public static String getDatetimeFormat(Map<String, String> config) {
        return config.get(DATETIME_PARAMETER_NAME);
    }

    public static String getDateFormat(Map<String, String> config) {
        return config.get(DATE_PARAMETER_NAME);
    }

    public static String getTimeFormat(Map<String, String> config) {
        return config.get(TIME_PARAMETER_NAME);
    }

    public static String getTemporalFormat(IAType targetType, Triple<String, String, String> temporalFormatByType) {
        switch (targetType.getTypeTag()) {
            case DATETIME:
                return temporalFormatByType.first;
            case DATE:
                return temporalFormatByType.second;
            case TIME:
                return temporalFormatByType.third;
            default:
                return null;
        }
    }

    public static IAObject getTemporalFormatArg(AbstractFunctionCallExpression funExpr) {
        FunctionIdentifier funId = funExpr.getFunctionIdentifier();
        if (BuiltinFunctions.DATE_DEFAULT_NULL_CONSTRUCTOR_WITH_FORMAT.equals(funId)
                || BuiltinFunctions.TIME_DEFAULT_NULL_CONSTRUCTOR_WITH_FORMAT.equals(funId)
                || BuiltinFunctions.DATETIME_DEFAULT_NULL_CONSTRUCTOR_WITH_FORMAT.equals(funId)) {
            List<Mutable<ILogicalExpression>> arguments = funExpr.getArguments();
            if (arguments.size() > 1) {
                return ConstantExpressionUtil.getConstantIaObject(arguments.get(1).getValue(), null);
            }
        }
        return null;
    }

    private static class EnforcedTypeBuilder {
        private final Deque<Triple<IAType, String, Boolean>> typeStack = new ArrayDeque<>();
        private List<Boolean> keyUnnestFlags;
        private List<String> keyFieldNames;
        private ARecordType baseRecordType;
        private IAType keyFieldType;

        // Output from nested-type-stack construction.
        private String bridgeNameFoundFromOpenTypeBuild;
        private IAType endOfOpenTypeBuild;
        private int indexOfOpenPart;
        private boolean castDefaultNull;

        public void reset(ARecordType baseRecordType, List<String> keyFieldNames, List<Boolean> keyUnnestFlags,
                IAType keyFieldType, boolean castDefaultNull) {
            this.baseRecordType = baseRecordType;
            this.keyFieldNames = keyFieldNames;
            this.keyUnnestFlags = keyUnnestFlags;
            this.keyFieldType = keyFieldType;
            this.castDefaultNull = castDefaultNull;
        }

        public ARecordType build() throws AlgebricksException {
            boolean isOpen = constructNestedTypeStack();
            IAType newTypeToAdd = (isOpen) ? buildNewForOpenType() : buildNewForFullyClosedType();
            return buildRestOfRecord(newTypeToAdd);
        }

        private boolean constructNestedTypeStack() throws AlgebricksException {
            IAType typeIntermediate = baseRecordType;
            List<String> subFieldName = new ArrayList<>();
            for (int i = 0; i < keyFieldNames.size() - 1; i++) {
                typeStack.push(
                        new Triple<>(typeIntermediate, keyFieldNames.get(i), i != 0 && keyUnnestFlags.get(i - 1)));
                bridgeNameFoundFromOpenTypeBuild = typeIntermediate.getTypeName();

                if (i == 0 || !keyUnnestFlags.get(i - 1)) {
                    subFieldName.add(keyFieldNames.get(i));
                } else {
                    // We have a multi-valued intermediate. Perform our UNNEST then add our field name.
                    typeIntermediate = TypeComputeUtils.extractListItemType(typeIntermediate);
                    if (typeIntermediate == null) {
                        String fName = String.join(".", subFieldName);
                        throw new AsterixException(ErrorCode.COMPILATION_ERROR,
                                "No list item type found. Wrong type given from field " + fName);
                    }
                    subFieldName.add(keyFieldNames.get(i));
                }

                // Attempt to resolve the type of our working subfield.
                typeIntermediate = TypeComputeUtils.getActualType(typeIntermediate);
                typeIntermediate =
                        ((ARecordType) typeIntermediate).getSubFieldType(subFieldName.subList(i, subFieldName.size()));

                if (typeIntermediate == null) {
                    endOfOpenTypeBuild = null;
                    indexOfOpenPart = i;
                    return true;
                }
                ATypeTag tt = TypeComputeUtils.getActualType(typeIntermediate).getTypeTag();
                if (tt != ATypeTag.OBJECT && tt != ATypeTag.ARRAY && tt != ATypeTag.MULTISET) {
                    String fName = String.join(".", subFieldName);
                    throw new AsterixException(ErrorCode.COMPILATION_ERROR,
                            "Field accessor is not defined for \"" + fName + "\" of type " + tt);
                }
            }

            endOfOpenTypeBuild = typeIntermediate;
            indexOfOpenPart = keyFieldNames.size() - 1;
            return false;
        }

        private IAType buildNewForOpenType() {
            // Walk backwards through our flags and construct the desired type.
            List<Boolean> unnestFlagsForOpenType = keyUnnestFlags.subList(indexOfOpenPart, keyUnnestFlags.size());
            List<String> fieldNamesForOpenType = keyFieldNames.subList(indexOfOpenPart, keyFieldNames.size());
            IAType resultant = keyFieldType;
            for (int i = unnestFlagsForOpenType.size() - 1; i >= 0; i--) {
                // Construct the type name.
                StringBuilder recordTypeNameBuilder = new StringBuilder();
                recordTypeNameBuilder.append(baseRecordType.getTypeName());
                for (int j = 0; j < i + indexOfOpenPart; j++) {
                    recordTypeNameBuilder.append("_").append(keyFieldNames.get(j));
                    if (keyUnnestFlags.get(j)) {
                        recordTypeNameBuilder.append("_Item");
                    }
                }

                // Construct the type itself and account for any array steps.
                resultant = nestArrayType(resultant, unnestFlagsForOpenType.get(i));
                resultant =
                        new ARecordType(recordTypeNameBuilder.toString(), new String[] { fieldNamesForOpenType.get(i) },
                                new IAType[] { AUnionType.createUnknownableType(resultant) }, true);
            }

            // Now update the parent to include this optional field, accounting for intermediate arrays.
            Triple<IAType, String, Boolean> gapTriple = this.typeStack.pop();
            ARecordType parentRecord =
                    (ARecordType) unnestArrayType(TypeComputeUtils.getActualType(gapTriple.first), gapTriple.third);
            IAType[] parentFieldTypes = ArrayUtils.addAll(parentRecord.getFieldTypes().clone(),
                    ((ARecordType) resultant).getFieldTypes().clone());
            resultant = new ARecordType(bridgeNameFoundFromOpenTypeBuild,
                    ArrayUtils.addAll(parentRecord.getFieldNames(), gapTriple.second), parentFieldTypes, true);
            resultant = keepUnknown(gapTriple.first, nestArrayType(resultant, gapTriple.third));

            return resultant;
        }

        private IAType buildNewForFullyClosedType() throws AsterixException {
            // The schema is closed all the way to the field itself.
            IAType typeIntermediate = TypeComputeUtils.getActualType(endOfOpenTypeBuild);
            boolean isOpenTypeWithUnnest = indexOfOpenPart != 0 && keyUnnestFlags.get(indexOfOpenPart - 1);
            boolean isKeyTypeWithUnnest = keyUnnestFlags.get(indexOfOpenPart);
            ARecordType lastNestedRecord = (ARecordType) unnestArrayType(typeIntermediate, isOpenTypeWithUnnest);
            Map<String, IAType> recordNameTypesMap = createRecordNameTypeMap(lastNestedRecord);

            // If an enforced field already exists, verify that the type is correct.
            IAType enforcedFieldType = recordNameTypesMap.get(keyFieldNames.get(keyFieldNames.size() - 1));
            if (enforcedFieldType != null && enforcedFieldType.getTypeTag() == ATypeTag.UNION
                    && ((AUnionType) enforcedFieldType).isUnknownableType()) {
                enforcedFieldType = ((AUnionType) enforcedFieldType).getActualType();
            }
            if (enforcedFieldType != null) {
                // choose the type specified in the DDL over the type in the dataset schema if CAST is used
                if (castDefaultNull) {
                    recordNameTypesMap.put(keyFieldNames.get(keyFieldNames.size() - 1),
                            AUnionType.createNullableType(nestArrayType(keyFieldType, isKeyTypeWithUnnest)));
                } else if (!ATypeHierarchy.canPromote(enforcedFieldType.getTypeTag(), this.keyFieldType.getTypeTag())) {
                    throw new AsterixException(ErrorCode.COMPILATION_ERROR, "Cannot enforce field \""
                            + String.join(".", this.keyFieldNames) + "\" to have type " + this.keyFieldType);
                }
            } else {
                recordNameTypesMap.put(keyFieldNames.get(keyFieldNames.size() - 1),
                        AUnionType.createUnknownableType(nestArrayType(keyFieldType, isKeyTypeWithUnnest)));
            }

            // Build the nested record, and account for the wrapping array.
            IAType resultant = nestArrayType(
                    new ARecordType(lastNestedRecord.getTypeName(), recordNameTypesMap.keySet().toArray(new String[0]),
                            recordNameTypesMap.values().toArray(new IAType[0]), lastNestedRecord.isOpen()),
                    isOpenTypeWithUnnest);
            return keepUnknown(endOfOpenTypeBuild, resultant);
        }

        private ARecordType buildRestOfRecord(IAType newTypeToAdd) {
            IAType resultant = TypeComputeUtils.getActualType(newTypeToAdd);
            while (!typeStack.isEmpty()) {
                Triple<IAType, String, Boolean> typeFromStack = typeStack.pop();
                IAType typeIntermediate = unnestArrayType(typeFromStack.first, typeFromStack.third);
                typeIntermediate = TypeComputeUtils.getActualType(typeIntermediate);
                ARecordType recordType = (ARecordType) typeIntermediate;
                IAType[] fieldTypes = recordType.getFieldTypes().clone();
                fieldTypes[recordType.getFieldIndex(typeFromStack.second)] = resultant;

                typeIntermediate = nestArrayType(new ARecordType(recordType.getTypeName() + "_enforced",
                        recordType.getFieldNames(), fieldTypes, recordType.isOpen()), typeFromStack.third);
                resultant = keepUnknown(typeFromStack.first, typeIntermediate);
            }
            return (ARecordType) resultant;
        }

        private static Map<String, IAType> createRecordNameTypeMap(ARecordType recordType) {
            LinkedHashMap<String, IAType> recordNameTypesMap = new LinkedHashMap<>();
            for (int j = 0; j < recordType.getFieldNames().length; j++) {
                recordNameTypesMap.put(recordType.getFieldNames()[j], recordType.getFieldTypes()[j]);
            }
            return recordNameTypesMap;
        }

        private static IAType keepUnknown(IAType originalRecordType, IAType updatedRecordType) {
            if (originalRecordType.getTypeTag() == ATypeTag.UNION) {
                return AUnionType.createUnknownableType(updatedRecordType, updatedRecordType.getTypeName());
            }
            return updatedRecordType;
        }

        private static IAType nestArrayType(IAType originalType, boolean isWithinArray) {
            if (isWithinArray) {
                String newTypeName = originalType.getTypeName().endsWith("_Item")
                        ? originalType.getTypeName().substring(0, originalType.getTypeName().length() - 5)
                        : originalType.getTypeName();
                return new AOrderedListType(originalType, newTypeName + "_Array");

            } else {
                return originalType;
            }
        }

        private static IAType unnestArrayType(IAType originalType, boolean isWithinArray) {
            IAType resultant = originalType;
            if (isWithinArray) {
                resultant = TypeComputeUtils.extractListItemType(resultant);
                if (resultant != null) {
                    resultant = TypeComputeUtils.getActualType(resultant);
                }
            }
            return resultant;
        }
    }

    /**
     * Merges typed index fields with specified recordType, allowing indexed fields to be optional.
     * I.e. the type { "personId":int32, "name": string, "address" : { "street": string } } with typed indexes
     * on age:int32, address.state:string will be merged into type { "personId":int32, "name": string,
     * "age": int32? "address" : { "street": string, "state": string? } } Used by open indexes to enforce
     * the type of an indexed record
     */
    public static Pair<ARecordType, ARecordType> createEnforcedType(ARecordType recordType, ARecordType metaType,
            List<Index> indexes) throws AlgebricksException {
        EnforcedTypeBuilder enforcedTypeBuilder = new EnforcedTypeBuilder();
        ARecordType enforcedRecordType = recordType;
        for (Index index : indexes) {
            if (!index.isSecondaryIndex() || !index.getIndexDetails().isOverridingKeyFieldTypes()) {
                continue;
            }
            switch (Index.IndexCategory.of(index.getIndexType())) {
                case VALUE:
                    enforcedRecordType = appendValueIndexType(enforcedRecordType,
                            (Index.ValueIndexDetails) index.getIndexDetails(), enforcedTypeBuilder);
                    break;
                case TEXT:
                    enforcedRecordType = appendTextIndexType(enforcedRecordType,
                            (Index.TextIndexDetails) index.getIndexDetails(), enforcedTypeBuilder);
                    break;
                case ARRAY:
                    enforcedRecordType = appendArrayIndexTypes(enforcedRecordType,
                            (Index.ArrayIndexDetails) index.getIndexDetails(), enforcedTypeBuilder);
                    break;
                default:
                    throw new CompilationException(ErrorCode.COMPILATION_UNKNOWN_INDEX_TYPE,
                            String.valueOf(index.getIndexType()));
            }
        }

        validateRecord(enforcedRecordType);
        return new Pair<>(enforcedRecordType, metaType);
    }

    private static ARecordType appendValueIndexType(ARecordType enforcedRecordType,
            Index.ValueIndexDetails valueIndexDetails, EnforcedTypeBuilder enforcedTypeBuilder)
            throws AlgebricksException {
        List<List<String>> keyFieldNames = valueIndexDetails.getKeyFieldNames();
        List<IAType> keyFieldTypes = valueIndexDetails.getKeyFieldTypes();
        List<Integer> keySources = valueIndexDetails.getKeyFieldSourceIndicators();
        for (int i = 0; i < keyFieldNames.size(); i++) {
            if (keySources.get(i) != Index.RECORD_INDICATOR) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR,
                        "Indexing an open field is only supported on the record part");
            }
            enforcedTypeBuilder.reset(enforcedRecordType, keyFieldNames.get(i),
                    Collections.nCopies(keyFieldNames.get(i).size(), false), keyFieldTypes.get(i),
                    valueIndexDetails.getCastDefaultNull().getOrElse(false));
            validateRecord(enforcedRecordType);
            enforcedRecordType = enforcedTypeBuilder.build();
        }

        return enforcedRecordType;
    }

    private static ARecordType appendTextIndexType(ARecordType enforcedRecordType,
            Index.TextIndexDetails textIndexDetails, EnforcedTypeBuilder enforcedTypeBuilder)
            throws AlgebricksException {
        List<List<String>> keyFieldNames = textIndexDetails.getKeyFieldNames();
        List<IAType> keyFieldTypes = textIndexDetails.getKeyFieldTypes();
        List<Integer> keySources = textIndexDetails.getKeyFieldSourceIndicators();
        for (int i = 0; i < keyFieldNames.size(); i++) {
            if (keySources.get(i) != Index.RECORD_INDICATOR) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR,
                        "Indexing an open field is only supported on the record part");
            }
            enforcedTypeBuilder.reset(enforcedRecordType, keyFieldNames.get(i),
                    Collections.nCopies(keyFieldNames.get(i).size(), false), keyFieldTypes.get(i), false);
            validateRecord(enforcedRecordType);
            enforcedRecordType = enforcedTypeBuilder.build();
        }

        return enforcedRecordType;
    }

    private static ARecordType appendArrayIndexTypes(ARecordType enforcedRecordType,
            Index.ArrayIndexDetails arrayIndexDetails, EnforcedTypeBuilder enforcedTypeBuilder)
            throws AlgebricksException {
        for (Index.ArrayIndexElement e : arrayIndexDetails.getElementList()) {
            if (e.getSourceIndicator() != Index.RECORD_INDICATOR) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR,
                        "Indexing an open field is only supported on the record part");
            }
            List<List<String>> unnestList = e.getUnnestList();
            List<List<String>> projectList = e.getProjectList();
            List<IAType> typeList = e.getTypeList();
            for (int i = 0; i < projectList.size(); i++) {
                List<String> project = projectList.get(i);
                enforcedTypeBuilder.reset(enforcedRecordType,
                        ArrayIndexUtil.getFlattenedKeyFieldNames(unnestList, project),
                        ArrayIndexUtil.getUnnestFlags(unnestList, project), typeList.get(i), false);
                validateRecord(enforcedRecordType);
                enforcedRecordType = enforcedTypeBuilder.build();
            }
        }

        return enforcedRecordType;
    }

    /**
     * Creates a map from name to type for fields in the passed type
     *
     * @param recordType the type to be mapped
     * @return a map mapping all fields to their types
     */
    private static Map<String, IAType> createRecordNameTypeMap(ARecordType recordType) {
        LinkedHashMap<String, IAType> recordNameTypesMap = new LinkedHashMap<>();
        for (int j = 0; j < recordType.getFieldNames().length; j++) {
            recordNameTypesMap.put(recordType.getFieldNames()[j], recordType.getFieldTypes()[j]);
        }
        return recordNameTypesMap;
    }

    /**
     * Maintains the {@code updatedRecordType} as nullable/missable (?) in case it was originally nullable/missable
     * @param originalRecordType the original record type
     * @param updatedRecordType the original record type being enforced/modified with new non-declared fields included
     * @return {@code updatedRecordType}
     */
    private static IAType keepUnknown(IAType originalRecordType, ARecordType updatedRecordType) {
        if (originalRecordType.getTypeTag() == ATypeTag.UNION) {
            return AUnionType.createUnknownableType(updatedRecordType, updatedRecordType.getTypeName());
        }
        return updatedRecordType;
    }

    /**
     * Makes sure the dataset record type being enforced/modified stays as a pure record type
     * @param enforcedDatasetRecordType the dataset record type enforced and modified by adding the extra fields indexed
     */
    private static void validateRecord(IAType enforcedDatasetRecordType) {
        if (enforcedDatasetRecordType.getTypeTag() != ATypeTag.OBJECT) {
            throw new IllegalStateException("The dataset type must be a record type to be able to build an index");
        }
    }

    /**
     * Makes sure the chain of fields accessed and leading to the indexed field are all valid record types.
     * E.g. for CREATE INDEX idx on ds(a.b.c.d: int) validate that a, b and c are all valid record types (?).
     * @param nestedRecordType the nested record field being accessed
     * @param fieldName the name of the nested record field
     * @throws AsterixException when supplying bad fields, e.g. CREATE INDEX i on ds(a.b: int, a.b.c: int) (mostly
     * for non-declared fields)
     */
    private static void validateNestedRecord(IAType nestedRecordType, List<String> fieldName) throws AsterixException {
        IAType actualType = TypeComputeUtils.getActualType(nestedRecordType);
        if (actualType.getTypeTag() != ATypeTag.OBJECT) {
            String fName = String.join(".", fieldName);
            throw new AsterixException(ErrorCode.COMPILATION_ERROR,
                    "Field accessor is not defined for \"" + fName + "\" of type " + actualType.getTypeTag());
        }
    }

    /**
     * Warning: this is not a general-purpose method.
     * Use only when processing types stored in metadata.
     * Doesn't properly handle ANY, UNION, NULL and MISSING types.
     * Allows {@code null} type reference which will be filled later during type translation.
     */
    public static IAType createQuantifiedType(IAType primeType, boolean nullable, boolean missable) {
        if (primeType != null) {
            switch (primeType.getTypeTag()) {
                case ANY:
                case UNION:
                case NULL:
                case MISSING:
                    throw new IllegalArgumentException(primeType.getDisplayName());
            }
        }

        IAType resType = primeType;
        if (nullable && missable) {
            resType = AUnionType.createUnknownableType(resType);
        } else if (nullable) {
            resType = AUnionType.createNullableType(resType);
        } else if (missable) {
            resType = AUnionType.createMissableType(resType);
        }
        return resType;
    }

    public static boolean isReservedInlineTypeName(String typeName) {
        return typeName.length() > 0 && typeName.charAt(0) == TYPE_NAME_DELIMITER;
    }

    public static String createDatasetInlineTypeName(String datasetName, boolean forMetaItemType) {
        char typeChar = forMetaItemType ? 'm' : 'i';
        return DATASET_INLINE_TYPE_PREFIX + typeChar + TYPE_NAME_DELIMITER + datasetName;
    }

    public static boolean isDatasetInlineTypeName(Dataset dataset, DataverseName typeDataverseName, String typeName) {
        return isInlineTypeName(dataset.getDataverseName(), typeDataverseName, typeName, DATASET_INLINE_TYPE_PREFIX);
    }

    private static boolean isInlineTypeName(DataverseName entityDataverseName, DataverseName typeDataverseName,
            String typeName, String inlineTypePrefix) {
        return entityDataverseName.equals(typeDataverseName) && typeName.startsWith(inlineTypePrefix);
    }

    public static String createFunctionParameterTypeName(String functionName, int arity, int parameterIndex) {
        StringBuilder sb = new StringBuilder(FUNCTION_INLINE_TYPE_PREFIX.length() + functionName.length() + 8);
        sb.append(FUNCTION_INLINE_TYPE_PREFIX).append(functionName).append(TYPE_NAME_DELIMITER).append(arity);
        if (parameterIndex >= 0) {
            sb.append(TYPE_NAME_DELIMITER).append(parameterIndex);
        } else if (parameterIndex != -1) {
            throw new IllegalArgumentException(String.valueOf(parameterIndex));
        } // otherwise it's a return type

        return sb.toString();
    }

    public static boolean isFunctionInlineTypeName(Function function, DataverseName typeDataverseName,
            String typeName) {
        return isInlineTypeName(function.getDataverseName(), typeDataverseName, typeName, FUNCTION_INLINE_TYPE_PREFIX);
    }

    public static String getFullyQualifiedDisplayName(DataverseName dataverseName, String typeName) {
        return MetadataUtil.getFullyQualifiedDisplayName(dataverseName, typeName);
    }

    /**
     * Inline type names are unique within a function, so we don't need to perform duplicate elimination
     */
    public static List<TypeSignature> getFunctionInlineTypes(Function function) {
        List<TypeSignature> inlineTypes = Collections.emptyList();
        TypeSignature returnType = function.getReturnType();
        if (returnType != null
                && isFunctionInlineTypeName(function, returnType.getDataverseName(), returnType.getName())) {
            inlineTypes = new ArrayList<>();
            inlineTypes.add(returnType);
        }
        List<TypeSignature> parameterTypes = function.getParameterTypes();
        if (parameterTypes != null) {
            for (TypeSignature parameterType : parameterTypes) {
                if (parameterType != null && isFunctionInlineTypeName(function, parameterType.getDataverseName(),
                        parameterType.getName())) {
                    if (inlineTypes.isEmpty()) {
                        inlineTypes = new ArrayList<>();
                    }
                    inlineTypes.add(parameterType);
                }
            }
        }
        return inlineTypes;
    }
}
