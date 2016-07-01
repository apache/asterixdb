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
import org.apache.asterix.lang.common.expression.CallExpr;
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

    protected final FunctionSignature datasetFunction = new FunctionSignature(MetadataConstants.METADATA_DATAVERSE_NAME,
            "dataset", 1);
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
    public Expression visit(VariableExpr varExpr, ILangExpression arg) throws AsterixException {
        String varName = varExpr.getVar().getValue();
        if (scopeChecker.isInForbiddenScopes(varName)) {
            throw new AsterixException(
                    "Inside limit clauses, it is disallowed to reference a variable having the same name"
                            + " as any variable bound in the same scope as the limit clause.");
        }
        if (!rewriteNeeded(varExpr)) {
            return varExpr;
        }
        boolean resolveAsDataset = resolveDatasetFirst(arg)
                && datasetExists(SqlppVariableUtil.toUserDefinedVariableName(varName).getValue());
        if (resolveAsDataset) {
            return wrapWithDatasetFunction(varExpr);
        }
        Set<VariableExpr> liveVars = SqlppVariableUtil.getLiveUserDefinedVariables(scopeChecker.getCurrentScope());
        return wrapWithResolveFunction(varExpr, liveVars);
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

    private Expression wrapWithDatasetFunction(VariableExpr expr) throws AsterixException {
        List<Expression> argList = new ArrayList<>();
        //Ignore the parser-generated prefix "$" for a dataset.
        String varName = SqlppVariableUtil.toUserDefinedVariableName(expr.getVar()).getValue();
        argList.add(new LiteralExpr(new StringLiteral(varName)));
        return new CallExpr(datasetFunction, argList);
    }

    private boolean datasetExists(String name) throws AsterixException {
        try {
            if (metadataProvider.findDataset(null, name) != null) {
                return true;
            }
            return pathDatasetExists(name);
        } catch (AlgebricksException e) {
            throw new AsterixException(e);
        }
    }

    private boolean pathDatasetExists(String name) throws AlgebricksException {
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
