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
import java.util.List;
import java.util.Set;

import org.apache.asterix.common.config.MetadataConstants;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.base.Expression.Kind;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppExpressionScopingVisitor;
import org.apache.asterix.metadata.declared.AqlMetadataProvider;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;

public class VariableCheckAndRewriteVisitor extends AbstractSqlppExpressionScopingVisitor {

    protected final FunctionSignature datasetFunction =
            new FunctionSignature(MetadataConstants.METADATA_DATAVERSE_NAME, "dataset", 1);
    protected final boolean overwrite;
    protected final AqlMetadataProvider metadataProvider;

    /**
     * @param context,
     *            manages ids of variables and guarantees uniqueness of variables.
     * @param overwrite,
     *            whether rewrite unbounded variables to dataset function calls.
     *            This flag can only be true for rewriting a top-level query.
     *            It should be false for rewriting the body expression of a user-defined function.
     */
    public VariableCheckAndRewriteVisitor(LangRewritingContext context, boolean overwrite,
            AqlMetadataProvider metadataProvider) {
        super(context);
        this.overwrite = overwrite;
        this.metadataProvider = metadataProvider;
    }

    @Override
    public Expression visit(FieldAccessor fa, ILangExpression arg) throws AsterixException {
        Expression leadingExpr = fa.getExpr();
        if (leadingExpr.getKind() != Kind.VARIABLE_EXPRESSION) {
            fa.setExpr(leadingExpr.accept(this, fa));
            return fa;
        } else {
            VariableExpr varExpr = (VariableExpr) leadingExpr;
            String lastIdentifier = fa.getIdent().getValue();
            Expression resolvedExpr = resolve(varExpr,
                    /** Resolves within the dataverse that has the same name as the variable name. */
                    SqlppVariableUtil.toUserDefinedVariableName(varExpr.getVar().getValue()).getValue(), lastIdentifier,
                    arg);
            if (resolvedExpr.getKind() == Kind.CALL_EXPRESSION) {
                CallExpr callExpr = (CallExpr) resolvedExpr;
                if (callExpr.getFunctionSignature().equals(datasetFunction)) {
                    // The field access is resolved to be a dataset access in the form of "dataverse.dataset".
                    return resolvedExpr;
                }
            }
            fa.setExpr(resolvedExpr);
            return fa;
        }
    }

    @Override
    public Expression visit(VariableExpr varExpr, ILangExpression arg) throws AsterixException {
        return resolve(varExpr, null /** Resolves within the default dataverse. */
                , SqlppVariableUtil.toUserDefinedVariableName(varExpr.getVar().getValue()).getValue(), arg);
    }

    // Resolve a variable expression with dataverse name and dataset name.
    private Expression resolve(VariableExpr varExpr, String dataverseName, String datasetName, ILangExpression arg)
            throws AsterixException {
        String varName = varExpr.getVar().getValue();
        checkError(varName);
        if (!rewriteNeeded(varExpr)) {
            return varExpr;
        }
        // Note: WITH variables are not used for path resolution. The reason is that
        // the accurate typing for ordered list with an UNION item type is not implemented.
        // We currently type it as [ANY]. If we include WITH variables for path resolution,
        // it will lead to ambiguities and the plan is going to be very complex.  An example query is:
        // asterixdb/asterix-app/src/test/resources/runtimets/queries_sqlpp/subquery/exists
        Set<VariableExpr> liveVars = SqlppVariableUtil.getLiveVariables(scopeChecker.getCurrentScope(), false);
        boolean resolveAsDataset = resolveDatasetFirst(arg) && datasetExists(dataverseName, datasetName);
        if (resolveAsDataset) {
            return wrapWithDatasetFunction(dataverseName, datasetName);
        } else if (liveVars.isEmpty()) {
            String defaultDataverseName = metadataProvider.getDefaultDataverseName();
            if (dataverseName == null && defaultDataverseName == null) {
                throw new AsterixException("Cannot find dataset " + datasetName
                        + " because there is no dataverse declared, nor an alias with name " + datasetName + "!");
            }
            //If no available dataset nor in-scope variable to resolve to, we throw an error.
            throw new AsterixException("Cannot find dataset " + datasetName + " in dataverse "
                    + (dataverseName == null ? defaultDataverseName : dataverseName) + " nor an alias with name "
                    + datasetName + "!");
        }
        return wrapWithResolveFunction(varExpr, liveVars);
    }

    // Checks whether we need to error the variable reference, e.g., the variable is referred
    // in a LIMIT clause.
    private void checkError(String varName) throws AsterixException {
        if (scopeChecker.isInForbiddenScopes(varName)) {
            throw new AsterixException(
                    "Inside limit clauses, it is disallowed to reference a variable having the same name"
                            + " as any variable bound in the same scope as the limit clause.");
        }
    }

    // For From/Join/UNNEST/NEST, we resolve the undefined identifier reference as dataset reference first.
    private boolean resolveDatasetFirst(ILangExpression arg) {
        return arg != null;
    }

    // Whether a rewrite is needed for a variable reference expression.
    private boolean rewriteNeeded(VariableExpr varExpr) throws AsterixException {
        String varName = varExpr.getVar().getValue();
        Identifier ident = scopeChecker.lookupSymbol(varName);
        if (ident != null) {
            // Exists such an identifier
            varExpr.setIsNewVar(false);
            varExpr.setVar((VarIdentifier) ident);
            return false;
        } else {
            // Meets a undefined variable
            return overwrite;
        }
    }

    private Expression wrapWithDatasetFunction(String dataverseName, String datasetName) throws AsterixException {
        String fullyQualifiedName = dataverseName == null ? datasetName : dataverseName + "." + datasetName;
        List<Expression> argList = new ArrayList<>();
        argList.add(new LiteralExpr(new StringLiteral(fullyQualifiedName)));
        return new CallExpr(datasetFunction, argList);
    }

    private boolean datasetExists(String dataverseName, String datasetName) throws AsterixException {
        try {
            if (metadataProvider.findDataset(dataverseName, datasetName) != null) {
                return true;
            }
            return fullyQualifiedDatasetNameExists(datasetName);
        } catch (AlgebricksException e) {
            throw new AsterixException(e);
        }
    }

    private boolean fullyQualifiedDatasetNameExists(String name) throws AlgebricksException {
        if (!name.contains(".")) {
            return false;
        }
        String[] path = name.split("\\.");
        if (path.length != 2) {
            return false;
        }
        if (metadataProvider.findDataset(path[0], path[1]) != null) {
            return true;
        }
        return false;
    }

}
