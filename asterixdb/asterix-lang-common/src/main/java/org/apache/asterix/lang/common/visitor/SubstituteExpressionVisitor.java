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
package org.apache.asterix.lang.common.visitor;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.clause.LimitClause;
import org.apache.asterix.lang.common.clause.OrderbyClause;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.FieldBinding;
import org.apache.asterix.lang.common.expression.IfExpr;
import org.apache.asterix.lang.common.expression.IndexAccessor;
import org.apache.asterix.lang.common.expression.ListConstructor;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.OperatorExpr;
import org.apache.asterix.lang.common.expression.QuantifiedExpression;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.expression.UnaryExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.rewrites.ExpressionSubstitutionEnvironment;
import org.apache.asterix.lang.common.rewrites.ExpressionSubstitutionEnvironment.DeepCopier;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.struct.QuantifiedPair;
import org.apache.asterix.lang.common.visitor.base.AbstractQueryExpressionVisitor;

public abstract class SubstituteExpressionVisitor
        extends AbstractQueryExpressionVisitor<Expression, ExpressionSubstitutionEnvironment> {
    private final DeepCopier deepCopier;

    public SubstituteExpressionVisitor(DeepCopier deepCopier) {
        this.deepCopier = deepCopier;
    }

    @Override
    public Expression visit(LetClause lc, ExpressionSubstitutionEnvironment env) throws AsterixException {
        // Marks the binding variable for future visiting until it exists its scope.
        env.disableVariable(lc.getVarExpr());
        lc.setBindingExpr(lc.getBindingExpr().accept(this, env));
        return null;
    }

    @Override
    public Expression visit(Query q, ExpressionSubstitutionEnvironment env) throws AsterixException {
        q.setBody(q.getBody().accept(this, env));
        return null;
    }

    @Override
    public Expression visit(FunctionDecl fd, ExpressionSubstitutionEnvironment env) throws AsterixException {
        // Do nothing for a function declaration.
        return null;
    }

    @Override
    public Expression visit(LiteralExpr l, ExpressionSubstitutionEnvironment env) throws AsterixException {
        return env.findSubstitution(l, deepCopier);
    }

    @Override
    public Expression visit(VariableExpr v, ExpressionSubstitutionEnvironment env) throws AsterixException {
        return env.findSubstitution(v, deepCopier);
    }

    @Override
    public Expression visit(ListConstructor lc, ExpressionSubstitutionEnvironment env) throws AsterixException {
        Expression newLc = env.findSubstitution(lc, deepCopier);
        if (newLc == lc) {
            lc.setExprList(rewriteExpressionList(lc.getExprList(), env));
            return lc;
        } else {
            return newLc.accept(this, env);
        }
    }

    @Override
    public Expression visit(RecordConstructor rc, ExpressionSubstitutionEnvironment env) throws AsterixException {
        Expression newRc = env.findSubstitution(rc, deepCopier);
        if (newRc == rc) {
            for (FieldBinding fb : rc.getFbList()) {
                fb.setLeftExpr(fb.getLeftExpr().accept(this, env));
                fb.setRightExpr(fb.getRightExpr().accept(this, env));
            }
            return rc;
        } else {
            return newRc.accept(this, env);
        }
    }

    @Override
    public Expression visit(OperatorExpr operatorExpr, ExpressionSubstitutionEnvironment env) throws AsterixException {
        Expression newOpertorExpr = env.findSubstitution(operatorExpr, deepCopier);
        if (newOpertorExpr == operatorExpr) {
            operatorExpr.setExprList(rewriteExpressionList(operatorExpr.getExprList(), env));
            return operatorExpr;
        } else {
            return newOpertorExpr.accept(this, env);
        }
    }

    @Override
    public Expression visit(FieldAccessor fa, ExpressionSubstitutionEnvironment env) throws AsterixException {
        Expression newFa = env.findSubstitution(fa, deepCopier);
        if (newFa == fa) {
            fa.setExpr(fa.getExpr().accept(this, env));
            return fa;
        } else {
            return newFa.accept(this, env);
        }
    }

    @Override
    public Expression visit(IndexAccessor ia, ExpressionSubstitutionEnvironment env) throws AsterixException {
        Expression newIa = env.findSubstitution(ia, deepCopier);
        if (newIa == ia) {
            ia.setExpr(ia.getExpr().accept(this, env));
            ia.setIndexExpr(ia.getIndexExpr().accept(this, env));
            return ia;
        } else {
            return newIa.accept(this, env);
        }
    }

    @Override
    public Expression visit(IfExpr ifexpr, ExpressionSubstitutionEnvironment env) throws AsterixException {
        Expression newIfExpr = env.findSubstitution(ifexpr, deepCopier);
        if (newIfExpr == ifexpr) {
            ifexpr.setCondExpr(ifexpr.getCondExpr().accept(this, env));
            ifexpr.setThenExpr(ifexpr.getThenExpr().accept(this, env));
            ifexpr.setElseExpr(ifexpr.getElseExpr().accept(this, env));
            return ifexpr;
        } else {
            return newIfExpr.accept(this, env);
        }
    }

    @Override
    public Expression visit(QuantifiedExpression qe, ExpressionSubstitutionEnvironment env) throws AsterixException {
        Expression newQe = env.findSubstitution(qe, deepCopier);
        if (newQe == qe) {
            // Rewrites the quantifier list.
            for (QuantifiedPair pair : qe.getQuantifiedList()) {
                pair.setExpr(pair.getExpr().accept(this, env));
            }

            // Rewrites the condition.
            for (QuantifiedPair pair : qe.getQuantifiedList()) {
                // Marks each binding var.
                env.disableVariable(pair.getVarExpr());
            }
            qe.setSatisfiesExpr(qe.getSatisfiesExpr().accept(this, env));
            for (QuantifiedPair pair : qe.getQuantifiedList()) {
                // Let each binding var exit its scope.
                env.enableVariable(pair.getVarExpr());
            }
            return qe;
        } else {
            return newQe.accept(this, env);
        }
    }

    @Override
    public Expression visit(WhereClause wc, ExpressionSubstitutionEnvironment env) throws AsterixException {
        wc.setWhereExpr(wc.getWhereExpr().accept(this, env));
        return null;
    }

    @Override
    public Expression visit(OrderbyClause oc, ExpressionSubstitutionEnvironment env) throws AsterixException {
        oc.setOrderbyList(rewriteExpressionList(oc.getOrderbyList(), env));
        return null;
    }

    @Override
    public Expression visit(LimitClause lc, ExpressionSubstitutionEnvironment env) throws AsterixException {
        lc.setLimitExpr(lc.getLimitExpr().accept(this, env));
        if (lc.hasOffset()) {
            lc.setOffset(lc.getOffset().accept(this, env));
        }
        return null;
    }

    @Override
    public Expression visit(UnaryExpr u, ExpressionSubstitutionEnvironment env) throws AsterixException {
        Expression newU = env.findSubstitution(u, deepCopier);
        if (newU == u) {
            u.setExpr(u.getExpr().accept(this, env));
            return u;
        } else {
            return newU.accept(this, env);
        }
    }

    @Override
    public Expression visit(CallExpr callExpr, ExpressionSubstitutionEnvironment env) throws AsterixException {
        Expression newCallExpr = env.findSubstitution(callExpr, deepCopier);
        if (newCallExpr == callExpr) {
            callExpr.setExprList(rewriteExpressionList(callExpr.getExprList(), env));
            return callExpr;
        } else {
            return newCallExpr.accept(this, env);
        }
    }

    /**
     * Rewrites the expression list.
     *
     * @param exprs,
     *            list of expressions.
     * @param env,
     *            ExpressionSubstitutionEnvironment.
     * @return a list of rewritten expressions.
     * @throws AsterixException
     */
    protected List<Expression> rewriteExpressionList(List<Expression> exprs, ExpressionSubstitutionEnvironment env)
            throws AsterixException {
        List<Expression> newExprs = new ArrayList<>();
        for (Expression expr : exprs) {
            newExprs.add(env.findSubstitution(expr, deepCopier));
        }
        return newExprs;
    }
}
