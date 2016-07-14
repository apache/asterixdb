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

import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.clause.GroupbyClause;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.GbyVariableExpressionPair;
import org.apache.asterix.lang.common.rewrites.ExpressionSubstitutionEnvironment;
import org.apache.asterix.lang.common.visitor.SubstituteExpressionVisitor;
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
import org.apache.asterix.lang.sqlpp.expression.IndependentSubquery;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.struct.SetOperationRight;
import org.apache.asterix.lang.sqlpp.util.SqlppRewriteUtil;
import org.apache.asterix.lang.sqlpp.visitor.base.ISqlppVisitor;

public class SqlppSubstituteExpressionsVisitor extends SubstituteExpressionVisitor
        implements ISqlppVisitor<Expression, ExpressionSubstitutionEnvironment> {

    public SqlppSubstituteExpressionsVisitor() {
        super(SqlppRewriteUtil::deepCopy);
    }

    @Override
    public Expression visit(GroupbyClause gc, ExpressionSubstitutionEnvironment env) throws AsterixException {
        for (GbyVariableExpressionPair pair : gc.getGbyPairList()) {
            pair.setExpr(pair.getExpr().accept(this, env));
        }
        // Forces from binding variables to exit their scopes, i.e.,
        // one can still replace from binding variables.
        env.pop();
        for (GbyVariableExpressionPair pair : gc.getGbyPairList()) {
            env.disableVariable(pair.getVar());
        }
        return null;
    }

    @Override
    public Expression visit(FromClause fromClause, ExpressionSubstitutionEnvironment env) throws AsterixException {
        // Marks the states before the from clause, and a consequent
        // group-by clause will reset env to the state.
        env.mark();
        for (FromTerm fromTerm : fromClause.getFromTerms()) {
            fromTerm.accept(this, env);
            // From terms are correlated and thus we mask binding variables after
            // visiting each individual from term.
            env.disableVariable(fromTerm.getLeftVariable());
            if (fromTerm.hasPositionalVariable()) {
                env.disableVariable(fromTerm.getLeftVariable());
            }
        }
        return null;
    }

    @Override
    public Expression visit(FromTerm fromTerm, ExpressionSubstitutionEnvironment env) throws AsterixException {
        fromTerm.setLeftExpression(fromTerm.getLeftExpression().accept(this, env));
        if (fromTerm.hasCorrelateClauses()) {
            for (AbstractBinaryCorrelateClause correlateClause : fromTerm.getCorrelateClauses()) {
                correlateClause.accept(this, env);
            }
            // correlate clauses are independent and thus we mask their binding variables
            // after visiting them.
            for (AbstractBinaryCorrelateClause correlateClause : fromTerm.getCorrelateClauses()) {
                env.disableVariable(correlateClause.getRightVariable());
                if (correlateClause.hasPositionalVariable()) {
                    env.disableVariable(correlateClause.getPositionalVariable());
                }
            }
        }
        return null;
    }

    @Override
    public Expression visit(JoinClause joinClause, ExpressionSubstitutionEnvironment env) throws AsterixException {
        joinClause.setRightExpression(joinClause.getRightExpression().accept(this, env));
        // Condition expressions can see the join binding variables, thus we have to mask them for replacement.
        env.disableVariable(joinClause.getRightVariable());
        if (joinClause.hasPositionalVariable()) {
            env.disableVariable(joinClause.getPositionalVariable());
        }
        joinClause.setConditionExpression(joinClause.getConditionExpression().accept(this, env));
        // Re-enable them.
        env.enableVariable(joinClause.getRightVariable());
        if (joinClause.hasPositionalVariable()) {
            env.enableVariable(joinClause.getPositionalVariable());
        }
        return null;
    }

    @Override
    public Expression visit(NestClause nestClause, ExpressionSubstitutionEnvironment env) throws AsterixException {
        nestClause.setRightExpression(nestClause.getRightExpression().accept(this, env));
        // Condition expressions can see the join binding variables, thus we have to mask them for replacement.
        env.disableVariable(nestClause.getRightVariable());
        if (nestClause.hasPositionalVariable()) {
            env.disableVariable(nestClause.getPositionalVariable());
        }
        nestClause.setConditionExpression(nestClause.getConditionExpression().accept(this, env));
        // Re-enable them.
        env.enableVariable(nestClause.getRightVariable());
        if (nestClause.hasPositionalVariable()) {
            env.enableVariable(nestClause.getPositionalVariable());
        }
        return null;
    }

    @Override
    public Expression visit(Projection projection, ExpressionSubstitutionEnvironment env) throws AsterixException {
        if (!projection.star()) {
            projection.setExpression(projection.getExpression().accept(this, env));
        }
        return null;
    }

    @Override
    public Expression visit(SelectBlock selectBlock, ExpressionSubstitutionEnvironment env) throws AsterixException {
        // Traverses the select block in the order of "from", "let"s, "where",
        // "group by", "let"s, "having" and "select".
        if (selectBlock.hasFromClause()) {
            selectBlock.getFromClause().accept(this, env);
        }
        if (selectBlock.hasLetClauses()) {
            List<LetClause> letList = selectBlock.getLetList();
            for (LetClause letClause : letList) {
                letClause.accept(this, env);
            }
        }
        if (selectBlock.hasWhereClause()) {
            selectBlock.getWhereClause().accept(this, env);
        }
        if (selectBlock.hasGroupbyClause()) {
            selectBlock.getGroupbyClause().accept(this, env);
        }
        if (selectBlock.hasLetClausesAfterGroupby()) {
            List<LetClause> letListAfterGby = selectBlock.getLetListAfterGroupby();
            for (LetClause letClauseAfterGby : letListAfterGby) {
                letClauseAfterGby.accept(this, env);
            }
        }
        if (selectBlock.hasHavingClause()) {
            selectBlock.getHavingClause().accept(this, env);
        }
        selectBlock.getSelectClause().accept(this, env);
        return null;
    }

    @Override
    public Expression visit(SelectClause selectClause, ExpressionSubstitutionEnvironment env) throws AsterixException {
        if (selectClause.selectElement()) {
            selectClause.getSelectElement().accept(this, env);
        } else {
            selectClause.getSelectRegular().accept(this, env);
        }
        return null;
    }

    @Override
    public Expression visit(SelectElement selectElement, ExpressionSubstitutionEnvironment env)
            throws AsterixException {
        selectElement.setExpression(selectElement.getExpression().accept(this, env));
        return null;
    }

    @Override
    public Expression visit(SelectRegular selectRegular, ExpressionSubstitutionEnvironment env)
            throws AsterixException {
        for (Projection projection : selectRegular.getProjections()) {
            projection.accept(this, env);
        }
        return null;
    }

    @Override
    public Expression visit(SelectSetOperation selectSetOperation, ExpressionSubstitutionEnvironment env)
            throws AsterixException {
        selectSetOperation.getLeftInput().accept(this, env);
        for (SetOperationRight right : selectSetOperation.getRightInputs()) {
            right.getSetOperationRightInput().accept(this, env);
        }
        return null;
    }

    @Override
    public Expression visit(SelectExpression selectExpression, ExpressionSubstitutionEnvironment env)
            throws AsterixException {
        // Backups the current states of env and the end of this method will reset to the returned depth.
        int depth = env.mark();
        // visit let list
        if (selectExpression.hasLetClauses()) {
            for (LetClause letClause : selectExpression.getLetList()) {
                letClause.accept(this, env);
            }
        }

        // visit the main select.
        selectExpression.getSelectSetOperation().accept(this, env);

        // visit order by.
        if (selectExpression.hasOrderby()) {
            selectExpression.getOrderbyClause().accept(this, env);
        }

        // visit limit
        if (selectExpression.hasLimit()) {
            selectExpression.getLimitClause().accept(this, env);
        }

        // re-enable the replacements all of all binding variables in the current scope.
        if (selectExpression.hasLetClauses()) {
            for (LetClause letClause : selectExpression.getLetList()) {
                env.enableVariable(letClause.getVarExpr());
            }
        }
        // Restores the states of env to be the same as the beginning .
        env.reset(depth);
        return selectExpression;
    }

    @Override
    public Expression visit(UnnestClause unnestClause, ExpressionSubstitutionEnvironment env) throws AsterixException {
        unnestClause.setRightExpression(unnestClause.getRightExpression().accept(this, env));
        return null;
    }

    @Override
    public Expression visit(HavingClause havingClause, ExpressionSubstitutionEnvironment env) throws AsterixException {
        havingClause.setFilterExpression(havingClause.getFilterExpression().accept(this, env));
        return null;
    }

    @Override
    public Expression visit(IndependentSubquery independentSubquery, ExpressionSubstitutionEnvironment env)
            throws AsterixException {
        independentSubquery.setExpr(independentSubquery.getExpr().accept(this, env));
        return null;
    }

}
