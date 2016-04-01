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

import org.apache.asterix.common.config.MetadataConstants;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.Expression;
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

public class VariableCheckAndRewriteVisitor extends AbstractSqlppExpressionScopingVisitor {

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
    public Expression visit(VariableExpr varExpr, Expression arg) throws AsterixException {
        String varName = varExpr.getVar().getValue();
        if (scopeChecker.isInForbiddenScopes(varName)) {
            throw new AsterixException(
                    "Inside limit clauses, it is disallowed to reference a variable having the same name as any variable bound in the same scope as the limit clause.");
        }
        if (rewriteNeeded(varExpr)) {
            return datasetRewrite(varExpr);
        } else {
            return varExpr;
        }
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
            return true;
        }
    }

    // Rewrites for global variable (e.g., dataset) references.
    private Expression datasetRewrite(VariableExpr expr) throws AsterixException {
        if (!overwrite) {
            return expr;
        }
        String funcName = "dataset";
        String dataverse = MetadataConstants.METADATA_DATAVERSE_NAME;
        FunctionSignature signature = new FunctionSignature(dataverse, funcName, 1);
        List<Expression> argList = new ArrayList<Expression>();
        //Ignore the parser-generated prefix "$" for a dataset.
        String dataset = SqlppVariableUtil.toUserDefinedVariableName(expr.getVar()).getValue();
        argList.add(new LiteralExpr(new StringLiteral(dataset)));
        return new CallExpr(signature, argList);
    }
}
