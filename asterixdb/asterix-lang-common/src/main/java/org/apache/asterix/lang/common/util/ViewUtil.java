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

package org.apache.asterix.lang.common.util;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DatasetFullyQualifiedName;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.IParser;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.apache.asterix.lang.common.base.IQueryRewriter;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.NullLiteral;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.statement.ViewDecl;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.ViewDetails;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.SourceLocation;

public final class ViewUtil {

    public static final String DATETIME_PARAMETER_NAME = BuiltinType.ADATETIME.getTypeName();
    public static final String DATE_PARAMETER_NAME = BuiltinType.ADATE.getTypeName();
    public static final String TIME_PARAMETER_NAME = BuiltinType.ATIME.getTypeName();

    private ViewUtil() {
    }

    public static ViewDecl parseStoredView(DatasetFullyQualifiedName viewName, ViewDetails view,
            IParserFactory parserFactory, IWarningCollector warningCollector, SourceLocation sourceLoc)
            throws CompilationException {
        IParser parser = parserFactory.createParser(new StringReader(view.getViewBody()));
        try {
            ViewDecl viewDecl = parser.parseViewBody(viewName);
            viewDecl.setSourceLocation(sourceLoc);
            if (warningCollector != null) {
                parser.getWarnings(warningCollector);
            }
            return viewDecl;
        } catch (CompilationException e) {
            throw new CompilationException(ErrorCode.COMPILATION_BAD_VIEW_DEFINITION, e, sourceLoc, viewName,
                    e.getMessage());
        }
    }

    public static List<List<Triple<DataverseName, String, String>>> getViewDependencies(ViewDecl viewDecl,
            IQueryRewriter rewriter) throws CompilationException {
        Expression normBody = viewDecl.getNormalizedViewBody();
        if (normBody == null) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, viewDecl.getSourceLocation(),
                    viewDecl.getViewName().toString());
        }

        // Get the list of used functions and used datasets
        List<Triple<DataverseName, String, String>> datasetDependencies = new ArrayList<>();
        List<Triple<DataverseName, String, String>> synonymDependencies = new ArrayList<>();
        List<Triple<DataverseName, String, String>> functionDependencies = new ArrayList<>();
        ExpressionUtils.collectDependencies(normBody, rewriter, datasetDependencies, synonymDependencies,
                functionDependencies);

        List<Triple<DataverseName, String, String>> typeDependencies = Collections.emptyList();
        return ViewDetails.createDependencies(datasetDependencies, functionDependencies, typeDependencies,
                synonymDependencies);
    }

    public static void validateViewItemType(ARecordType recordType, SourceLocation sourceLoc)
            throws CompilationException {
        if (recordType.isOpen()) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc, "view type cannot have open fields");
        }
        String[] fieldNames = recordType.getFieldNames();
        IAType[] fieldTypes = recordType.getFieldTypes();
        for (int i = 0, n = fieldNames.length; i < n; i++) {
            IAType fieldType = fieldTypes[i];
            IAType primeType;
            if (fieldType.getTypeTag() == ATypeTag.UNION) {
                AUnionType unionType = (AUnionType) fieldType;
                if (!unionType.isNullableType()) {
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                            String.format("Invalid type for field %s. Optional type must allow NULL", fieldNames[i]));
                }
                primeType = unionType.getActualType();
            } else {
                primeType = fieldType;
            }
            if (getTypeConstructor(primeType) == null) {
                throw new CompilationException(ErrorCode.COMPILATION_TYPE_UNSUPPORTED, sourceLoc, "view",
                        primeType.getTypeName());
            }
        }
    }

    public static Map<String, String> validateViewConfiguration(Map<String, String> viewConfig, boolean hasItemType)
            throws CompilationException {
        if (viewConfig == null) {
            viewConfig = Collections.emptyMap();
        }
        if (hasItemType) {
            for (Map.Entry<String, String> me : viewConfig.entrySet()) {
                String name = me.getKey();
                String value = me.getValue();
                if (DATETIME_PARAMETER_NAME.equals(name) || DATE_PARAMETER_NAME.equals(name)
                        || TIME_PARAMETER_NAME.equals(name)) {
                    if (value == null) {
                        throw new CompilationException(ErrorCode.INVALID_REQ_PARAM_VAL, name, value);
                    }
                } else {
                    throw new CompilationException(ErrorCode.ILLEGAL_SET_PARAMETER, name);
                }
            }
        } else if (!viewConfig.isEmpty()) {
            throw new CompilationException(ErrorCode.ILLEGAL_SET_PARAMETER, viewConfig.keySet().iterator().next());
        }
        return viewConfig;
    }

    public static List<String> validateViewPrimaryKey(Pair<List<Integer>, List<List<String>>> primaryKeyFieldsPair,
            boolean hasItemType) throws CompilationException {
        if (primaryKeyFieldsPair == null || primaryKeyFieldsPair.second.isEmpty()) {
            return null;
        }
        if (!hasItemType) {
            throw new CompilationException(ErrorCode.INVALID_PRIMARY_KEY_DEFINITION);
        }
        List<Integer> sourceIndicators = primaryKeyFieldsPair.first;
        List<List<String>> primaryKeyFields = primaryKeyFieldsPair.second;
        int n = primaryKeyFields.size();
        List<String> resultFields = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            if (sourceIndicators.get(i) != Index.RECORD_INDICATOR) {
                throw new CompilationException(ErrorCode.INVALID_PRIMARY_KEY_DEFINITION);
            }
            List<String> nestedField = primaryKeyFields.get(i);
            if (nestedField.size() != 1) {
                throw new CompilationException(ErrorCode.INVALID_PRIMARY_KEY_DEFINITION);
            }
            resultFields.add(nestedField.get(0));
        }
        return resultFields;
    }

    public static Expression createTypeConvertExpression(Expression inExpr, IAType targetType,
            Triple<String, String, String> temporalDataFormat, DatasetFullyQualifiedName viewName,
            SourceLocation sourceLoc) throws CompilationException {
        String format = temporalDataFormat != null ? getTemporalFormat(targetType, temporalDataFormat) : null;
        boolean withFormat = format != null;
        FunctionIdentifier constrFid =
                withFormat ? getTypeConstructorWithFormat(targetType) : getTypeConstructor(targetType);
        if (constrFid == null) {
            throw new CompilationException(ErrorCode.COMPILATION_TYPE_UNSUPPORTED, sourceLoc, viewName.toString(),
                    targetType.getTypeName());
        }
        List<Expression> convertArgList = new ArrayList<>(2);
        convertArgList.add(inExpr);
        if (format != null) {
            LiteralExpr formatExpr = new LiteralExpr(new StringLiteral(format));
            formatExpr.setSourceLocation(sourceLoc);
            convertArgList.add(formatExpr);
        }
        CallExpr convertExpr = new CallExpr(new FunctionSignature(constrFid), convertArgList);
        convertExpr.setSourceLocation(inExpr.getSourceLocation());
        return convertExpr;
    }

    public static Expression createMissingToNullExpression(Expression inExpr, SourceLocation sourceLoc) {
        List<Expression> missing2NullArgs = new ArrayList<>(2);
        missing2NullArgs.add(inExpr);
        missing2NullArgs.add(new LiteralExpr(NullLiteral.INSTANCE));
        CallExpr missing2NullExpr = new CallExpr(new FunctionSignature(BuiltinFunctions.IF_MISSING), missing2NullArgs);
        missing2NullExpr.setSourceLocation(sourceLoc);
        return missing2NullExpr;
    }

    public static Expression createNotIsNullExpression(Expression inExpr, SourceLocation sourceLoc) {
        List<Expression> isNullArgs = new ArrayList<>(1);
        isNullArgs.add(inExpr);
        CallExpr isNullExpr = new CallExpr(new FunctionSignature(BuiltinFunctions.IS_NULL), isNullArgs);
        isNullExpr.setSourceLocation(sourceLoc);
        List<Expression> notExprArgs = new ArrayList<>(1);
        notExprArgs.add(isNullExpr);
        CallExpr notExpr = new CallExpr(new FunctionSignature(BuiltinFunctions.NOT), notExprArgs);
        notExpr.setSourceLocation(sourceLoc);
        return notExpr;
    }

    public static Expression createFieldAccessExpression(VarIdentifier inVar, String fieldName,
            SourceLocation sourceLoc) {
        VariableExpr inVarRef = new VariableExpr(inVar);
        inVarRef.setSourceLocation(sourceLoc);
        FieldAccessor fa = new FieldAccessor(inVarRef, new Identifier(fieldName));
        fa.setSourceLocation(sourceLoc);
        return fa;
    }

    public static FunctionIdentifier getTypeConstructor(IAType type) {
        switch (type.getTypeTag()) {
            case TINYINT:
                return BuiltinFunctions.INT8_CONSTRUCTOR;
            case SMALLINT:
                return BuiltinFunctions.INT16_CONSTRUCTOR;
            case INTEGER:
                return BuiltinFunctions.INT32_CONSTRUCTOR;
            case BIGINT:
                return BuiltinFunctions.INT64_CONSTRUCTOR;
            case FLOAT:
                return BuiltinFunctions.FLOAT_CONSTRUCTOR;
            case DOUBLE:
                return BuiltinFunctions.DOUBLE_CONSTRUCTOR;
            case BOOLEAN:
                return BuiltinFunctions.BOOLEAN_CONSTRUCTOR;
            case STRING:
                return BuiltinFunctions.STRING_CONSTRUCTOR;
            case DATE:
                return BuiltinFunctions.DATE_CONSTRUCTOR;
            case TIME:
                return BuiltinFunctions.TIME_CONSTRUCTOR;
            case DATETIME:
                return BuiltinFunctions.DATETIME_CONSTRUCTOR;
            case YEARMONTHDURATION:
                return BuiltinFunctions.YEAR_MONTH_DURATION_CONSTRUCTOR;
            case DAYTIMEDURATION:
                return BuiltinFunctions.DAY_TIME_DURATION_CONSTRUCTOR;
            case DURATION:
                return BuiltinFunctions.DURATION_CONSTRUCTOR;
            case UUID:
                return BuiltinFunctions.UUID_CONSTRUCTOR;
            case BINARY:
                return BuiltinFunctions.BINARY_BASE64_CONSTRUCTOR;
            default:
                return null;
        }
    }

    public static FunctionIdentifier getTypeConstructorWithFormat(IAType type) {
        switch (type.getTypeTag()) {
            case DATE:
                return BuiltinFunctions.DATE_CONSTRUCTOR_WITH_FORMAT;
            case TIME:
                return BuiltinFunctions.TIME_CONSTRUCTOR_WITH_FORMAT;
            case DATETIME:
                return BuiltinFunctions.DATETIME_CONSTRUCTOR_WITH_FORMAT;
            default:
                return null;
        }
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
}
