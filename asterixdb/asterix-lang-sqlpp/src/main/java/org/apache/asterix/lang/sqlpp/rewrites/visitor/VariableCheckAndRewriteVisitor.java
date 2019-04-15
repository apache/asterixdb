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
package org.apache.asterix.lang.sqlpp.rewrites.visitor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Expression.Kind;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.context.Scope;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.expression.WindowExpression;
import org.apache.asterix.lang.sqlpp.util.FunctionMapUtil;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.lang.sqlpp.visitor.CheckDatasetOnlyResolutionVisitor;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppExpressionScopingVisitor;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.StringUtils;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class VariableCheckAndRewriteVisitor extends AbstractSqlppExpressionScopingVisitor {

    private static final FunctionSignature FN_DATASET = new FunctionSignature(BuiltinFunctions.DATASET);

    protected final MetadataProvider metadataProvider;

    /**
     * @param context,
     *            manages ids of variables and guarantees uniqueness of variables.
     */
    public VariableCheckAndRewriteVisitor(LangRewritingContext context, MetadataProvider metadataProvider,
            Collection<VarIdentifier> externalVars) {
        super(context, externalVars);
        this.metadataProvider = metadataProvider;
    }

    @Override
    public Expression visit(FieldAccessor fa, ILangExpression parent) throws CompilationException {
        Expression leadingExpr = fa.getExpr();
        if (leadingExpr.getKind() != Kind.VARIABLE_EXPRESSION) {
            fa.setExpr(leadingExpr.accept(this, parent));
            return fa;
        } else {
            VariableExpr varExpr = (VariableExpr) leadingExpr;
            String lastIdentifier = fa.getIdent().getValue();
            Expression resolvedExpr = resolve(varExpr,
                    /* Resolves within the dataverse that has the same name as the variable name. */
                    SqlppVariableUtil.toUserDefinedVariableName(varExpr.getVar().getValue()).getValue(), lastIdentifier,
                    parent);
            if (resolvedExpr.getKind() == Kind.CALL_EXPRESSION) {
                CallExpr callExpr = (CallExpr) resolvedExpr;
                if (callExpr.getFunctionSignature().equals(FN_DATASET)) {
                    // The field access is resolved to be a dataset access in the form of "dataverse.dataset".
                    return resolvedExpr;
                }
            }
            fa.setExpr(resolvedExpr);
            return fa;
        }
    }

    @Override
    public Expression visit(VariableExpr varExpr, ILangExpression parent) throws CompilationException {
        return resolve(varExpr, metadataProvider.getDefaultDataverseName(),
                SqlppVariableUtil.toUserDefinedVariableName(varExpr.getVar().getValue()).getValue(), parent);
    }

    // Resolve a variable expression with dataverse name and dataset name.
    private Expression resolve(VariableExpr varExpr, String dataverseName, String datasetName, ILangExpression parent)
            throws CompilationException {

        VarIdentifier varId = varExpr.getVar();
        String varName = varId.getValue();
        SourceLocation sourceLoc = varExpr.getSourceLocation();
        VarIdentifier var = lookupVariable(varName, sourceLoc);
        if (var != null) {
            // Exists such an identifier
            varExpr.setIsNewVar(false);
            varExpr.setVar(var);
            return varExpr;
        }

        if (SqlppVariableUtil.isExternalVariableIdentifier(varId)) {
            throw new CompilationException(ErrorCode.PARAMETER_NO_VALUE, sourceLoc,
                    SqlppVariableUtil.variableNameToDisplayedFieldName(varId.getValue()));
        }

        boolean resolveToDataset = parent.accept(CheckDatasetOnlyResolutionVisitor.INSTANCE, varExpr);
        if (resolveToDataset) {
            // resolve the undefined identifier reference as a dataset access.
            // for a From/Join/UNNEST/Quantifiers binding expression
            return resolveAsDataset(dataverseName, datasetName, sourceLoc);
        } else {
            // resolve the undefined identifier reference as a field access on a context variable
            Map<VariableExpr, Set<? extends Scope.SymbolAnnotation>> localVars =
                    scopeChecker.getCurrentScope().getLiveVariables(scopeChecker.getPrecedingScope());
            Set<VariableExpr> contextVars = Scope.findVariablesAnnotatedBy(localVars.keySet(),
                    SqlppVariableAnnotation.CONTEXT_VARIABLE, localVars, sourceLoc);
            VariableExpr contextVar = pickContextVar(contextVars, varExpr);
            return resolveAsFieldAccess(contextVar, varId, sourceLoc);
        }
    }

    private VarIdentifier lookupVariable(String varName, SourceLocation sourceLoc) throws CompilationException {
        if (scopeChecker.isInForbiddenScopes(varName)) {
            throw new CompilationException(ErrorCode.FORBIDDEN_SCOPE, sourceLoc);
        }
        Identifier ident = scopeChecker.lookupSymbol(varName);
        return ident != null ? (VarIdentifier) ident : null;
    }

    private Expression resolveAsDataset(String dataverseName, String datasetName, SourceLocation sourceLoc)
            throws CompilationException {
        Dataset dataset = findDataset(dataverseName, datasetName, sourceLoc);
        if (dataset == null) {
            throw createUnresolvableError(dataverseName, datasetName, sourceLoc);
        }
        metadataProvider.addAccessedDataset(dataset);
        List<Expression> argList = new ArrayList<>(1);
        argList.add(new LiteralExpr(new StringLiteral(dataset.getFullyQualifiedName())));
        CallExpr callExpr = new CallExpr(new FunctionSignature(BuiltinFunctions.DATASET), argList);
        callExpr.setSourceLocation(sourceLoc);
        return callExpr;
    }

    // Rewrites for an field access by name
    static Expression resolveAsFieldAccess(Expression sourceExpr, VarIdentifier fieldVar, SourceLocation sourceLoc) {
        VarIdentifier fieldName = SqlppVariableUtil.toUserDefinedVariableName(fieldVar.getValue());
        FieldAccessor fa = new FieldAccessor(sourceExpr, fieldName);
        fa.setSourceLocation(sourceLoc);
        return fa;
    }

    private CompilationException createUnresolvableError(String dataverseName, String datasetName,
            SourceLocation sourceLoc) {
        String defaultDataverseName = metadataProvider.getDefaultDataverseName();
        if (dataverseName == null && defaultDataverseName == null) {
            return new CompilationException(ErrorCode.NAME_RESOLVE_UNKNOWN_DATASET, sourceLoc, datasetName);
        }
        //If no available dataset nor in-scope variable to resolve to, we throw an error.
        return new CompilationException(ErrorCode.NAME_RESOLVE_UNKNOWN_DATASET_IN_DATAVERSE, sourceLoc, datasetName,
                dataverseName == null ? defaultDataverseName : dataverseName);
    }

    private Dataset findDataset(String dataverseName, String datasetName, SourceLocation sourceLoc)
            throws CompilationException {
        try {
            Dataset dataset = metadataProvider.findDataset(dataverseName, datasetName);
            if (dataset != null) {
                return dataset;
            }
            return findDatasetByFullyQualifiedName(datasetName);
        } catch (AlgebricksException e) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, e, sourceLoc, e.getMessage());
        }
    }

    private Dataset findDatasetByFullyQualifiedName(String name) throws AlgebricksException {
        if (name.indexOf('.') < 0) {
            return null;
        }
        String[] path = StringUtils.split(name, '.');
        if (path.length != 2) {
            return null;
        }
        return metadataProvider.findDataset(path[0], path[1]);
    }

    @Override
    public Expression visit(CallExpr callExpr, ILangExpression arg) throws CompilationException {
        // skip variables inside SQL-92 aggregates (they will be resolved by SqlppGroupByAggregationSugarVisitor)
        if (FunctionMapUtil.isSql92AggregateFunction(callExpr.getFunctionSignature())) {
            return callExpr;
        }
        return super.visit(callExpr, arg);
    }

    @Override
    public Expression visit(WindowExpression winExpr, ILangExpression arg) throws CompilationException {
        // skip variables inside list arguments of window functions (will be resolved by SqlppWindowExpressionVisitor)
        FunctionSignature fs = winExpr.getFunctionSignature();
        FunctionIdentifier winfi = FunctionMapUtil.getInternalWindowFunction(fs);
        if (winfi != null) {
            if (BuiltinFunctions.builtinFunctionHasProperty(winfi,
                    BuiltinFunctions.WindowFunctionProperty.HAS_LIST_ARG)) {
                visitWindowExpressionExcludingExprList(winExpr, arg);
                List<Expression> exprList = winExpr.getExprList();
                List<Expression> newExprList = new ArrayList<>(exprList.size());
                Iterator<Expression> i = exprList.iterator();
                newExprList.add(i.next()); // don't visit the list arg
                while (i.hasNext()) {
                    newExprList.add(visit(i.next(), arg));
                }
                winExpr.setExprList(newExprList);
                return winExpr;
            }
        } else if (FunctionMapUtil.isSql92AggregateFunction(fs)) {
            visitWindowExpressionExcludingExprList(winExpr, arg);
            return winExpr;
        }
        return super.visit(winExpr, arg);
    }

    static VariableExpr pickContextVar(Collection<VariableExpr> contextVars, VariableExpr usedVar)
            throws CompilationException {
        switch (contextVars.size()) {
            case 0:
                throw new CompilationException(ErrorCode.UNDEFINED_IDENTIFIER, usedVar.getSourceLocation(),
                        SqlppVariableUtil.toUserDefinedVariableName(usedVar.getVar().getValue()).getValue());
            case 1:
                return contextVars.iterator().next();
            default:
                throw new CompilationException(ErrorCode.AMBIGUOUS_IDENTIFIER, usedVar.getSourceLocation(),
                        SqlppVariableUtil.toUserDefinedVariableName(usedVar.getVar().getValue()).getValue());
        }
    }
}
