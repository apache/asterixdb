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

import static org.apache.asterix.metadata.utils.TypeUtil.getTemporalFormat;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DatasetFullyQualifiedName;
import org.apache.asterix.common.metadata.DependencyFullyQualifiedName;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.IParser;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.apache.asterix.lang.common.base.IQueryRewriter;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.statement.ViewDecl;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.metadata.entities.ViewDetails;
import org.apache.asterix.metadata.utils.TypeUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.util.LogRedactionUtil;

public final class ViewUtil {

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

    public static List<List<DependencyFullyQualifiedName>> getViewDependencies(ViewDecl viewDecl,
            List<ViewDetails.ForeignKey> foreignKeys, IQueryRewriter rewriter) throws CompilationException {
        Expression normBody = viewDecl.getNormalizedViewBody();
        if (normBody == null) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, viewDecl.getSourceLocation(),
                    viewDecl.getViewName().toString());
        }

        // Get the list of used functions and used datasets
        List<DependencyFullyQualifiedName> datasetDependencies = new ArrayList<>();
        List<DependencyFullyQualifiedName> synonymDependencies = new ArrayList<>();
        List<DependencyFullyQualifiedName> functionDependencies = new ArrayList<>();
        ExpressionUtils.collectDependencies(normBody, rewriter, datasetDependencies, synonymDependencies,
                functionDependencies);

        if (foreignKeys != null) {
            DatasetFullyQualifiedName viewName = viewDecl.getViewName();
            for (ViewDetails.ForeignKey foreignKey : foreignKeys) {
                DatasetFullyQualifiedName refName = foreignKey.getReferencedDatasetName();
                boolean isSelfReference = refName.equals(viewName);
                if (isSelfReference || containsDependency(datasetDependencies, refName)) {
                    continue;
                }
                datasetDependencies.add(new DependencyFullyQualifiedName(refName.getDatabaseName(),
                        refName.getDataverseName(), refName.getDatasetName(), null));
            }
        }

        List<DependencyFullyQualifiedName> typeDependencies = Collections.emptyList();
        return ViewDetails.createDependencies(datasetDependencies, functionDependencies, typeDependencies,
                synonymDependencies);
    }

    private static boolean containsDependency(List<DependencyFullyQualifiedName> inList,
            DatasetFullyQualifiedName searchName) {
        for (DependencyFullyQualifiedName d : inList) {
            if (d.getDatabaseName().equals(searchName.getDatabaseName())
                    && d.getDataverseName().equals(searchName.getDataverseName())
                    && d.getSubName1().equals(searchName.getDatasetName())) {
                return true;
            }
        }
        return false;
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
                            String.format("Invalid type for field %s. Optional type must allow NULL",
                                    LogRedactionUtil.userData(fieldNames[i])));
                }
                primeType = unionType.getActualType();
            } else {
                primeType = fieldType;
            }
            if (TypeUtil.getTypeConstructorDefaultNull(primeType, false) == null) {
                throw new CompilationException(ErrorCode.COMPILATION_TYPE_UNSUPPORTED, sourceLoc, "view",
                        primeType.getTypeName());
            }
        }
    }

    public static Expression createTypeConvertExpression(Expression inExpr, IAType targetType,
            Triple<String, String, String> temporalDataFormat, DatasetFullyQualifiedName viewName,
            SourceLocation sourceLoc) throws CompilationException {
        String format = temporalDataFormat != null ? getTemporalFormat(targetType, temporalDataFormat) : null;
        boolean withFormat = format != null;
        FunctionIdentifier constrFid = TypeUtil.getTypeConstructorDefaultNull(targetType, withFormat);
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

}
