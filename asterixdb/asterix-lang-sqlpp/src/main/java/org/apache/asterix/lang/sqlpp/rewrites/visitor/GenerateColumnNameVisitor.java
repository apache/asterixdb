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

import java.util.HashSet;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.GroupbyClause;
import org.apache.asterix.lang.common.expression.GbyVariableExpressionPair;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.clause.Projection;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.parser.ParseException;
import org.apache.asterix.lang.sqlpp.util.ExpressionToVariableUtil;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppExpressionScopingVisitor;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * <pre>
 * 1. Generates implicit column names for projections in a SELECT clause,
 * 2. Generates group by key variables if they were not specified in the query
 * </pre>
 */
public class GenerateColumnNameVisitor extends AbstractSqlppExpressionScopingVisitor {

    private final Set<VarIdentifier> gbyKeyVars = new HashSet<>();

    public GenerateColumnNameVisitor(LangRewritingContext context) {
        super(context);
    }

    @Override
    public Expression visit(SelectBlock selectBlock, ILangExpression arg) throws CompilationException {
        // Visit selectBlock first so that column names starts from $1.
        selectBlock.getSelectClause().accept(this, arg);
        return super.visit(selectBlock, arg);
    }

    @Override
    public Expression visit(Projection projection, ILangExpression arg) throws CompilationException {
        if (!projection.star() && !projection.varStar() && projection.getName() == null) {
            projection.setName(SqlppVariableUtil.variableNameToDisplayedFieldName(context.newVariable().getValue()));
        }
        return super.visit(projection, arg);
    }

    @Override
    public Expression visit(GroupbyClause groupbyClause, ILangExpression arg) throws CompilationException {
        gbyKeyVars.clear();
        for (GbyVariableExpressionPair gbyKeyPair : groupbyClause.getGbyPairList()) {
            if (gbyKeyPair.getVar() == null) {
                Expression gbyKeyExpr = gbyKeyPair.getExpr();
                SourceLocation sourceLoc = gbyKeyExpr.getSourceLocation();
                VariableExpr varExpr;
                try {
                    varExpr = ExpressionToVariableUtil.getGeneratedVariable(gbyKeyExpr, false);
                } catch (ParseException e) {
                    throw new CompilationException(ErrorCode.PARSE_ERROR, e, sourceLoc);
                }
                if (varExpr == null || gbyKeyVars.contains(varExpr.getVar())) {
                    varExpr = new VariableExpr(context.newVariable());
                }
                varExpr.setSourceLocation(sourceLoc);
                gbyKeyPair.setVar(varExpr);
            }
            gbyKeyVars.add(gbyKeyPair.getVar().getVar());
        }
        return super.visit(groupbyClause, arg);
    }
}
