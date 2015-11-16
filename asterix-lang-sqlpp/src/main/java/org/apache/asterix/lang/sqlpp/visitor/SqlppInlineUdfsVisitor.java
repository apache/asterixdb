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
package org.apache.asterix.lang.sqlpp.visitor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.visitor.AbstractInlineUdfsVisitor;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.HavingClause;
import org.apache.asterix.lang.sqlpp.clause.JoinClause;
import org.apache.asterix.lang.sqlpp.clause.NestClause;
import org.apache.asterix.lang.sqlpp.clause.Projection;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectClause;
import org.apache.asterix.lang.sqlpp.clause.SelectElement;
import org.apache.asterix.lang.sqlpp.clause.SelectRegular;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.clause.UnnestClause;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.struct.SetOperationRight;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableSubstitutionUtil;
import org.apache.asterix.lang.sqlpp.visitor.base.ISqlppVisitor;
import org.apache.hyracks.algebricks.common.utils.Pair;

public class SqlppInlineUdfsVisitor extends AbstractInlineUdfsVisitor
        implements ISqlppVisitor<Boolean, List<FunctionDecl>> {

    public SqlppInlineUdfsVisitor(LangRewritingContext context) {
        super(context, new SqlppCloneAndSubstituteVariablesVisitor(context));
    }

    @Override
    protected Expression generateQueryExpression(List<LetClause> letClauses, Expression returnExpr)
            throws AsterixException {
        Map<VariableExpr, Expression> varExprMap = extractLetBindingVariableExpressionMappings(letClauses);
        return (Expression) SqlppVariableSubstitutionUtil.substituteVariable(returnExpr, varExprMap);
    }

    @Override
    public Boolean visit(FromClause fromClause, List<FunctionDecl> func) throws AsterixException {
        boolean changed = false;
        for (FromTerm fromTerm : fromClause.getFromTerms()) {
            changed |= fromTerm.accept(this, func);
        }
        return changed;
    }

    @Override
    public Boolean visit(FromTerm fromTerm, List<FunctionDecl> func) throws AsterixException {
        boolean changed = false;
        changed |= fromTerm.getLeftExpression().accept(this, func);
        for (AbstractBinaryCorrelateClause correlateClause : fromTerm.getCorrelateClauses()) {
            changed |= correlateClause.accept(this, func);
        }
        return changed;
    }

    @Override
    public Boolean visit(JoinClause joinClause, List<FunctionDecl> funcs) throws AsterixException {
        Pair<Boolean, Expression> p1 = inlineUdfsInExpr(joinClause.getRightExpression(), funcs);
        joinClause.setRightExpression(p1.second);
        Pair<Boolean, Expression> p2 = inlineUdfsInExpr(joinClause.getConditionExpression(), funcs);
        joinClause.setConditionExpression(p2.second);
        return p1.first && p2.first;
    }

    @Override
    public Boolean visit(NestClause nestClause, List<FunctionDecl> funcs) throws AsterixException {
        Pair<Boolean, Expression> p1 = inlineUdfsInExpr(nestClause.getRightExpression(), funcs);
        nestClause.setRightExpression(p1.second);
        Pair<Boolean, Expression> p2 = inlineUdfsInExpr(nestClause.getConditionExpression(), funcs);
        nestClause.setConditionExpression(p2.second);
        return p1.first && p2.first;
    }

    @Override
    public Boolean visit(Projection projection, List<FunctionDecl> funcs) throws AsterixException {
        Pair<Boolean, Expression> p = inlineUdfsInExpr(projection.getExpression(), funcs);
        projection.setExpression(p.second);
        return p.first;
    }

    @Override
    public Boolean visit(SelectBlock selectBlock, List<FunctionDecl> funcs) throws AsterixException {
        boolean changed = false;
        if (selectBlock.hasFromClause()) {
            changed |= selectBlock.getFromClause().accept(this, funcs);
        }
        if (selectBlock.hasLetClauses()) {
            for (LetClause letClause : selectBlock.getLetList()) {
                changed |= letClause.accept(this, funcs);
            }
        }
        if (selectBlock.hasWhereClause()) {
            changed |= selectBlock.getWhereClause().accept(this, funcs);
        }
        if (selectBlock.hasGroupbyClause()) {
            changed |= selectBlock.getGroupbyClause().accept(this, funcs);
        }
        if (selectBlock.hasLetClausesAfterGroupby()) {
            for (LetClause letClause : selectBlock.getLetListAfterGroupby()) {
                changed |= letClause.accept(this, funcs);
            }
        }
        if (selectBlock.hasHavingClause()) {
            changed |= selectBlock.getHavingClause().accept(this, funcs);
        }
        changed |= selectBlock.getSelectClause().accept(this, funcs);
        return changed;
    }

    @Override
    public Boolean visit(SelectClause selectClause, List<FunctionDecl> funcs) throws AsterixException {
        boolean changed = false;
        if (selectClause.selectElement()) {
            changed |= selectClause.getSelectElement().accept(this, funcs);
        } else {
            changed |= selectClause.getSelectRegular().accept(this, funcs);
        }
        return changed;
    }

    @Override
    public Boolean visit(SelectElement selectElement, List<FunctionDecl> funcs) throws AsterixException {
        Pair<Boolean, Expression> p = inlineUdfsInExpr(selectElement.getExpression(), funcs);
        selectElement.setExpression(p.second);
        return p.first;
    }

    @Override
    public Boolean visit(SelectRegular selectRegular, List<FunctionDecl> funcs) throws AsterixException {
        boolean changed = false;
        for (Projection projection : selectRegular.getProjections()) {
            changed |= projection.accept(this, funcs);
        }
        return changed;
    }

    @Override
    public Boolean visit(SelectSetOperation selectSetOperation, List<FunctionDecl> funcs) throws AsterixException {
        boolean changed = false;
        changed |= selectSetOperation.getLeftInput().accept(this, funcs);
        for (SetOperationRight right : selectSetOperation.getRightInputs()) {
            changed |= right.getSetOperationRightInput().accept(this, funcs);
        }
        return changed;
    }

    @Override
    public Boolean visit(SelectExpression selectExpression, List<FunctionDecl> funcs) throws AsterixException {
        boolean changed = false;
        if (selectExpression.hasLetClauses()) {
            for (LetClause letClause : selectExpression.getLetList()) {
                changed |= letClause.accept(this, funcs);
            }
        }
        changed |= selectExpression.getSelectSetOperation().accept(this, funcs);
        if (selectExpression.hasOrderby()) {
            changed |= selectExpression.getOrderbyClause().accept(this, funcs);
        }
        if (selectExpression.hasLimit()) {
            changed |= selectExpression.getLimitClause().accept(this, funcs);
        }
        return changed;
    }

    @Override
    public Boolean visit(UnnestClause unnestClause, List<FunctionDecl> funcs) throws AsterixException {
        Pair<Boolean, Expression> p = inlineUdfsInExpr(unnestClause.getRightExpression(), funcs);
        unnestClause.setRightExpression(p.second);
        return p.first;
    }

    @Override
    public Boolean visit(HavingClause havingClause, List<FunctionDecl> funcs) throws AsterixException {
        Pair<Boolean, Expression> p = inlineUdfsInExpr(havingClause.getFilterExpression(), funcs);
        havingClause.setFilterExpression(p.second);
        return p.first;
    }

    private Map<VariableExpr, Expression> extractLetBindingVariableExpressionMappings(List<LetClause> letClauses)
            throws AsterixException {
        Map<VariableExpr, Expression> varExprMap = new HashMap<VariableExpr, Expression>();
        for (LetClause lc : letClauses) {
            // inline let variables one by one iteratively.
            lc.setBindingExpr(
                    (Expression) SqlppVariableSubstitutionUtil.substituteVariable(lc.getBindingExpr(), varExprMap));
            varExprMap.put(lc.getVarExpr(), lc.getBindingExpr());
        }
        return varExprMap;
    }
}
