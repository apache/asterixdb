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
package org.apache.asterix.lang.aql.visitor;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.aql.clause.DistinctClause;
import org.apache.asterix.lang.aql.clause.ForClause;
import org.apache.asterix.lang.aql.expression.FLWOGRExpression;
import org.apache.asterix.lang.aql.expression.UnionExpr;
import org.apache.asterix.lang.aql.visitor.base.IAQLVisitor;
import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.rewrites.VariableSubstitutionEnvironment;
import org.apache.asterix.lang.common.util.VariableCloneAndSubstitutionUtil;
import org.apache.asterix.lang.common.visitor.CloneAndSubstituteVariablesVisitor;
import org.apache.hyracks.algebricks.common.utils.Pair;

public class AQLCloneAndSubstituteVariablesVisitor extends CloneAndSubstituteVariablesVisitor implements
        IAQLVisitor<Pair<ILangExpression, VariableSubstitutionEnvironment>, VariableSubstitutionEnvironment> {

    private LangRewritingContext context;

    public AQLCloneAndSubstituteVariablesVisitor(LangRewritingContext context) {
        super(context);
        this.context = context;
    }

    @Override
    public Pair<ILangExpression, VariableSubstitutionEnvironment> visit(ForClause fc,
            VariableSubstitutionEnvironment env) throws CompilationException {
        Pair<ILangExpression, VariableSubstitutionEnvironment> p1 = fc.getInExpr().accept(this, env);
        VariableExpr varExpr = fc.getVarExpr();
        VariableExpr newVe = generateNewVariable(context, varExpr);
        VariableSubstitutionEnvironment resultEnv = new VariableSubstitutionEnvironment(env);
        resultEnv.removeSubstitution(varExpr);

        VariableExpr newPosVarExpr = null;
        if (fc.hasPosVar()) {
            VariableExpr posVarExpr = fc.getPosVarExpr();
            newPosVarExpr = generateNewVariable(context, posVarExpr);
            resultEnv.removeSubstitution(posVarExpr);
        }
        ForClause newFor = new ForClause(newVe, (Expression) p1.first, newPosVarExpr);
        return new Pair<ILangExpression, VariableSubstitutionEnvironment>(newFor, resultEnv);
    }

    @Override
    public Pair<ILangExpression, VariableSubstitutionEnvironment> visit(FLWOGRExpression flwor,
            VariableSubstitutionEnvironment env) throws CompilationException {
        List<Clause> newClauses = new ArrayList<Clause>(flwor.getClauseList().size());
        VariableSubstitutionEnvironment currentEnv = env;
        for (Clause c : flwor.getClauseList()) {
            Pair<ILangExpression, VariableSubstitutionEnvironment> p1 = c.accept(this, currentEnv);
            currentEnv = p1.second;
            newClauses.add((Clause) p1.first);
        }
        Pair<ILangExpression, VariableSubstitutionEnvironment> p2 = flwor.getReturnExpr().accept(this, currentEnv);
        Expression newReturnExpr = (Expression) p2.first;
        FLWOGRExpression newFlwor = new FLWOGRExpression(newClauses, newReturnExpr);
        return new Pair<ILangExpression, VariableSubstitutionEnvironment>(newFlwor, p2.second);
    }

    @Override
    public Pair<ILangExpression, VariableSubstitutionEnvironment> visit(UnionExpr u,
            VariableSubstitutionEnvironment env) throws CompilationException {
        List<Expression> exprList = VariableCloneAndSubstitutionUtil.visitAndCloneExprList(u.getExprs(), env, this);
        UnionExpr newU = new UnionExpr(exprList);
        return new Pair<ILangExpression, VariableSubstitutionEnvironment>(newU, env);
    }

    @Override
    public Pair<ILangExpression, VariableSubstitutionEnvironment> visit(DistinctClause dc,
            VariableSubstitutionEnvironment env) throws CompilationException {
        List<Expression> exprList =
                VariableCloneAndSubstitutionUtil.visitAndCloneExprList(dc.getDistinctByExpr(), env, this);
        DistinctClause dc2 = new DistinctClause(exprList);
        return new Pair<ILangExpression, VariableSubstitutionEnvironment>(dc2, env);
    }
}
