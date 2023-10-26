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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DatasetFullyQualifiedName;
import org.apache.asterix.common.metadata.DependencyFullyQualifiedName;
import org.apache.asterix.lang.common.base.AbstractStatement;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.IQueryRewriter;
import org.apache.asterix.lang.common.base.Literal;
import org.apache.asterix.lang.common.expression.AbstractCallExpression;
import org.apache.asterix.lang.common.expression.FieldBinding;
import org.apache.asterix.lang.common.expression.ListConstructor;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.expression.UnaryExpr;
import org.apache.asterix.lang.common.literal.DoubleLiteral;
import org.apache.asterix.lang.common.literal.FloatLiteral;
import org.apache.asterix.lang.common.literal.IntegerLiteral;
import org.apache.asterix.lang.common.literal.LongIntegerLiteral;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.statement.ViewDecl;
import org.apache.asterix.lang.common.struct.UnaryExprType;
import org.apache.asterix.lang.common.visitor.GatherFunctionCallsVisitor;
import org.apache.asterix.object.base.AdmArrayNode;
import org.apache.asterix.object.base.AdmBigIntNode;
import org.apache.asterix.object.base.AdmBooleanNode;
import org.apache.asterix.object.base.AdmDoubleNode;
import org.apache.asterix.object.base.AdmNullNode;
import org.apache.asterix.object.base.AdmObjectNode;
import org.apache.asterix.object.base.AdmStringNode;
import org.apache.asterix.object.base.IAdmNode;
import org.apache.asterix.om.exceptions.TypeMismatchException;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.api.exceptions.SourceLocation;

import com.google.common.graph.GraphBuilder;
import com.google.common.graph.Graphs;
import com.google.common.graph.MutableGraph;

public class ExpressionUtils {
    private ExpressionUtils() {
    }

    public static IAdmNode toNode(Expression expr) throws CompilationException {
        switch (expr.getKind()) {
            case LIST_CONSTRUCTOR_EXPRESSION:
                return toNode((ListConstructor) expr);
            case LITERAL_EXPRESSION:
                return toNode((LiteralExpr) expr);
            case RECORD_CONSTRUCTOR_EXPRESSION:
                return toNode((RecordConstructor) expr);
            case UNARY_EXPRESSION:
                UnaryExpr unaryExpr = (UnaryExpr) expr;
                UnaryExprType unaryExprType = unaryExpr.getExprType();
                if (unaryExprType == UnaryExprType.POSITIVE || unaryExprType == UnaryExprType.NEGATIVE) {
                    Expression uexpr = unaryExpr.getExpr();
                    if (uexpr.getKind() == Expression.Kind.LITERAL_EXPRESSION) {
                        if (unaryExprType == UnaryExprType.POSITIVE) {
                            return toNode(uexpr);
                        } else {
                            Literal lit = ((LiteralExpr) uexpr).getValue();
                            return toNode(new LiteralExpr(reverseSign(lit)));
                        }
                    } else {
                        throw new CompilationException(ErrorCode.LITERAL_TYPE_NOT_SUPPORTED_IN_CONSTANT_RECORD,
                                uexpr.getKind());
                    }
                } else {
                    throw new CompilationException(ErrorCode.EXPRESSION_NOT_SUPPORTED_IN_CONSTANT_RECORD,
                            unaryExprType);
                }
            default:
                throw new CompilationException(ErrorCode.EXPRESSION_NOT_SUPPORTED_IN_CONSTANT_RECORD, expr.getKind());
        }
    }

    public static AdmObjectNode toNode(RecordConstructor recordConstructor) throws CompilationException {
        AdmObjectNode node = new AdmObjectNode();
        final List<FieldBinding> fbList = recordConstructor.getFbList();
        for (int i = 0; i < fbList.size(); i++) {
            FieldBinding binding = fbList.get(i);
            String key = LangRecordParseUtil.exprToStringLiteral(binding.getLeftExpr()).getStringValue();
            IAdmNode value = ExpressionUtils.toNode(binding.getRightExpr());
            node.set(key, value);
        }
        return node;
    }

    private static IAdmNode toNode(ListConstructor listConstructor) throws CompilationException {
        final List<Expression> exprList = listConstructor.getExprList();
        AdmArrayNode array = new AdmArrayNode(exprList.size());
        for (int i = 0; i < exprList.size(); i++) {
            array.add(ExpressionUtils.toNode(exprList.get(i)));
        }
        return array;
    }

    private static IAdmNode toNode(LiteralExpr literalExpr) throws CompilationException {
        final Literal value = literalExpr.getValue();
        final Literal.Type literalType = value.getLiteralType();
        switch (literalType) {
            case DOUBLE:
                return new AdmDoubleNode(((DoubleLiteral) value).getDoubleValue());
            case FALSE:
            case TRUE:
                return AdmBooleanNode.get((Boolean) value.getValue());
            case LONG:
                return new AdmBigIntNode(((LongIntegerLiteral) value).getLongValue());
            case NULL:
                return AdmNullNode.INSTANCE;
            case STRING:
                return new AdmStringNode(((StringLiteral) value).getValue());
            default:
                throw new CompilationException(ErrorCode.LITERAL_TYPE_NOT_SUPPORTED_IN_CONSTANT_RECORD, literalType);
        }
    }

    public static <T> Collection<T> emptyIfNull(Collection<T> coll) {
        return coll == null ? Collections.emptyList() : coll;
    }

    public static String getStringLiteral(Expression arg) {
        if (arg.getKind() == Expression.Kind.LITERAL_EXPRESSION) {
            Literal item = ((LiteralExpr) arg).getValue();
            if (item.getLiteralType() == Literal.Type.STRING) {
                return item.getStringValue();
            }
        }
        return null;
    }

    public static Boolean getBooleanLiteral(Expression arg) {
        if (arg.getKind() == Expression.Kind.LITERAL_EXPRESSION) {
            Literal item = ((LiteralExpr) arg).getValue();
            switch (item.getLiteralType()) {
                case TRUE:
                    return true;
                case FALSE:
                    return false;
            }
        }
        return null;
    }

    public static Literal reverseSign(Literal lit) throws TypeMismatchException {
        switch (lit.getLiteralType()) {
            case DOUBLE:
                DoubleLiteral dLit = (DoubleLiteral) lit;
                DoubleLiteral reversedDLit = new DoubleLiteral(-dLit.getValue());
                return reversedDLit;
            case FLOAT:
                FloatLiteral fLit = (FloatLiteral) lit;
                FloatLiteral reversedFLit = new FloatLiteral(-fLit.getValue());
                return reversedFLit;
            case LONG:
                LongIntegerLiteral lLit = (LongIntegerLiteral) lit;
                LongIntegerLiteral reversedLLit = new LongIntegerLiteral(-lLit.getValue());
                return reversedLLit;
            case INTEGER:
                IntegerLiteral iLit = (IntegerLiteral) lit;
                IntegerLiteral reversedILit = new IntegerLiteral(-iLit.getValue());
                return reversedILit;
            case NULL:
            case MISSING:
                return lit;
            default:
                throw new TypeMismatchException(null, convertLiteralTypeTagToATypeTag(lit), ATypeTag.DOUBLE,
                        ATypeTag.FLOAT, ATypeTag.BIGINT, ATypeTag.INTEGER);
        }
    }

    public static double getDoubleValue(Literal item) throws TypeMismatchException {
        if ((item.getLiteralType() == Literal.Type.DOUBLE) || (item.getLiteralType() == Literal.Type.FLOAT)
                || (item.getLiteralType() == Literal.Type.LONG) || (item.getLiteralType() == Literal.Type.INTEGER)) {
            return ((Number) item.getValue()).doubleValue();
        } else {
            throw new TypeMismatchException(null, convertLiteralTypeTagToATypeTag(item), ATypeTag.DOUBLE,
                    ATypeTag.FLOAT, ATypeTag.BIGINT, ATypeTag.INTEGER);
        }
    }

    public static long getLongValue(Literal item) throws TypeMismatchException {
        if ((item.getLiteralType() == Literal.Type.LONG) || (item.getLiteralType() == Literal.Type.INTEGER)) {
            return ((Number) item.getValue()).longValue();
        } else {
            throw new TypeMismatchException(null, convertLiteralTypeTagToATypeTag(item), ATypeTag.BIGINT,
                    ATypeTag.INTEGER);
        }
    }

    private static ATypeTag convertLiteralTypeTagToATypeTag(Literal lit) {
        switch (lit.getLiteralType()) {
            case DOUBLE:
                return ATypeTag.DOUBLE;
            case FLOAT:
                return ATypeTag.FLOAT;
            case LONG:
                return ATypeTag.BIGINT;
            case INTEGER:
                return ATypeTag.INTEGER;
            case TRUE:
            case FALSE:
                return ATypeTag.BOOLEAN;
            case NULL:
                return ATypeTag.NULL;
            case MISSING:
                return ATypeTag.MISSING;
            default:
                return ATypeTag.STRING;
        }
    }

    public static void collectDependencies(Expression expression, IQueryRewriter rewriter,
            List<DependencyFullyQualifiedName> outDatasetDependencies,
            List<DependencyFullyQualifiedName> outSynonymDependencies,
            List<DependencyFullyQualifiedName> outFunctionDependencies) throws CompilationException {
        // Duplicate elimination
        Set<DatasetFullyQualifiedName> seenDatasets = new HashSet<>();
        Set<DatasetFullyQualifiedName> seenSynonyms = new HashSet<>();
        Set<FunctionSignature> seenFunctions = new HashSet<>();
        List<AbstractCallExpression> functionCalls = new ArrayList<>();
        rewriter.getFunctionCalls(expression, functionCalls);

        for (AbstractCallExpression functionCall : functionCalls) {
            switch (functionCall.getKind()) {
                case CALL_EXPRESSION:
                    FunctionSignature signature = functionCall.getFunctionSignature();
                    if (FunctionUtil.isBuiltinFunctionSignature(signature)) {
                        if (FunctionUtil.isBuiltinDatasetFunction(signature)) {
                            Triple<DatasetFullyQualifiedName, Boolean, DatasetFullyQualifiedName> dsArgs =
                                    FunctionUtil.parseDatasetFunctionArguments(functionCall);
                            DatasetFullyQualifiedName synonymReference = dsArgs.third;
                            if (synonymReference != null) {
                                // resolved via synonym -> store synonym name as a dependency
                                if (seenSynonyms.add(synonymReference)) {
                                    outSynonymDependencies.add(new DependencyFullyQualifiedName(
                                            synonymReference.getDatabaseName(), synonymReference.getDataverseName(),
                                            synonymReference.getDatasetName(), null));
                                }
                            } else {
                                // resolved directly -> store dataset (or view) name as a dependency
                                DatasetFullyQualifiedName datasetReference = dsArgs.first;
                                if (seenDatasets.add(datasetReference)) {
                                    outDatasetDependencies.add(new DependencyFullyQualifiedName(
                                            datasetReference.getDatabaseName(), datasetReference.getDataverseName(),
                                            datasetReference.getDatasetName(), null));
                                }
                            }
                        }
                    } else {
                        if (seenFunctions.add(signature)) {
                            outFunctionDependencies.add(new DependencyFullyQualifiedName(signature.getDatabaseName(),
                                    signature.getDataverseName(), signature.getName(),
                                    Integer.toString(signature.getArity())));
                        }
                    }
                    break;
                case WINDOW_EXPRESSION:
                    // there cannot be used-defined window functions
                    break;
                default:
                    throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                            functionCall.getSourceLocation(), functionCall.getFunctionSignature().toString(false));
            }
        }
    }

    public static boolean hasFunctionOrViewRecursion(Map<FunctionSignature, FunctionDecl> functionDeclMap,
            Map<DatasetFullyQualifiedName, ViewDecl> viewDeclMap,
            java.util.function.Function<Collection<AbstractCallExpression>, GatherFunctionCallsVisitor> callVisitorFactory)
            throws CompilationException {
        List<AbstractCallExpression> callList = new ArrayList<>();
        GatherFunctionCallsVisitor callVisitor = callVisitorFactory.apply(callList);
        MutableGraph<AbstractStatement> callGraph = GraphBuilder.directed().allowsSelfLoops(true).build();
        for (FunctionDecl from : functionDeclMap.values()) {
            callList.clear();
            from.getNormalizedFuncBody().accept(callVisitor, null);
            for (AbstractCallExpression callExpr : callList) {
                addToCallGraph(callGraph, from, callExpr, functionDeclMap, viewDeclMap);
            }
        }
        for (ViewDecl from : viewDeclMap.values()) {
            callList.clear();
            from.getNormalizedViewBody().accept(callVisitor, null);
            for (AbstractCallExpression callExpr : callList) {
                addToCallGraph(callGraph, from, callExpr, functionDeclMap, viewDeclMap);
            }
        }
        return Graphs.hasCycle(callGraph);
    }

    private static void addToCallGraph(MutableGraph<AbstractStatement> callGraph, AbstractStatement from,
            AbstractCallExpression callExpr, Map<FunctionSignature, FunctionDecl> functionDeclMap,
            Map<DatasetFullyQualifiedName, ViewDecl> viewDeclMap) throws CompilationException {
        if (callExpr.getKind() == Expression.Kind.CALL_EXPRESSION) {
            FunctionSignature callSignature = callExpr.getFunctionSignature();
            if (FunctionUtil.isBuiltinFunctionSignature(callSignature)) {
                if (FunctionUtil.isBuiltinDatasetFunction(callSignature)) {
                    Triple<DatasetFullyQualifiedName, Boolean, DatasetFullyQualifiedName> dsArgs =
                            FunctionUtil.parseDatasetFunctionArguments(callExpr);
                    if (Boolean.TRUE.equals(dsArgs.second)) {
                        DatasetFullyQualifiedName viewName = dsArgs.first;
                        ViewDecl vdTo = viewDeclMap.get(viewName);
                        if (vdTo != null) {
                            callGraph.putEdge(from, vdTo);
                        }
                    }
                }
            } else {
                FunctionDecl fdTo = functionDeclMap.get(callSignature);
                if (fdTo != null) {
                    callGraph.putEdge(from, fdTo);
                }
            }
        }
    }

    public static Query createWrappedQuery(Expression expr, SourceLocation sourceLoc) {
        Query wrappedQuery = new Query(false);
        wrappedQuery.setSourceLocation(sourceLoc);
        wrappedQuery.setBody(expr);
        wrappedQuery.setTopLevel(false);
        return wrappedQuery;
    }
}
