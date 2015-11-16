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
package org.apache.asterix.translator;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.algebra.base.ILangExpressionToPlanTranslator;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.aql.clause.DistinctClause;
import org.apache.asterix.lang.aql.clause.ForClause;
import org.apache.asterix.lang.aql.expression.FLWOGRExpression;
import org.apache.asterix.lang.aql.expression.UnionExpr;
import org.apache.asterix.lang.aql.visitor.base.IAQLVisitor;
import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Expression.Kind;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.metadata.declared.AqlMetadataProvider;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;

/**
 * Each visit returns a pair of an operator and a variable. The variable
 * corresponds to the new column, if any, added to the tuple flow. E.g., for
 * Unnest, the column is the variable bound to the elements in the list, for
 * Subplan it is null. The first argument of a visit method is the expression
 * which is translated. The second argument of a visit method is the tuple
 * source for the current subtree.
 */

class AqlExpressionToPlanTranslator extends LangExpressionToPlanTranslator implements ILangExpressionToPlanTranslator,
        IAQLVisitor<Pair<ILogicalOperator, LogicalVariable>, Mutable<ILogicalOperator>> {

    public AqlExpressionToPlanTranslator(AqlMetadataProvider metadataProvider, int currentVarCounter)
            throws AlgebricksException {
        super(metadataProvider, currentVarCounter);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(ForClause fc, Mutable<ILogicalOperator> tupSource)
            throws AsterixException {
        LogicalVariable v = context.newVar(fc.getVarExpr());
        Expression inExpr = fc.getInExpr();
        Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo = aqlExprToAlgExpression(inExpr, tupSource);
        ILogicalOperator returnedOp;

        if (fc.getPosVarExpr() == null) {
            returnedOp = new UnnestOperator(v, new MutableObject<ILogicalExpression>(makeUnnestExpression(eo.first)));
        } else {
            LogicalVariable pVar = context.newVar(fc.getPosVarExpr());
            // We set the positional variable type as INT64 type.
            returnedOp = new UnnestOperator(v, new MutableObject<ILogicalExpression>(makeUnnestExpression(eo.first)),
                    pVar, BuiltinType.AINT64, new AqlPositionWriter());
        }
        returnedOp.getInputs().add(eo.second);
        return new Pair<ILogicalOperator, LogicalVariable>(returnedOp, v);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(FLWOGRExpression flwor, Mutable<ILogicalOperator> tupSource)
            throws AsterixException {
        Mutable<ILogicalOperator> flworPlan = tupSource;
        boolean isTop = context.isTopFlwor();
        if (!isTop) {
            context.enterSubplan();
        }
        if (isTop) {
            context.setTopFlwor(false);
        }
        for (Clause c : flwor.getClauseList()) {
            Pair<ILogicalOperator, LogicalVariable> pC = c.accept(this, flworPlan);
            flworPlan = new MutableObject<ILogicalOperator>(pC.first);
        }

        Expression r = flwor.getReturnExpr();
        boolean noFlworClause = flwor.noForClause();

        Pair<ILogicalOperator, LogicalVariable> result;
        if (r.getKind() == Kind.VARIABLE_EXPRESSION) {
            VariableExpr v = (VariableExpr) r;
            LogicalVariable var = context.getVar(v.getVar().getId());
            result = produceFlworPlan(noFlworClause, isTop, flworPlan, var);
        } else {
            Mutable<ILogicalOperator> baseOp = new MutableObject<ILogicalOperator>(flworPlan.getValue());
            Pair<ILogicalOperator, LogicalVariable> rRes = r.accept(this, baseOp);
            ILogicalOperator rOp = rRes.first;
            ILogicalOperator resOp;
            if (expressionNeedsNoNesting(r)) {
                baseOp.setValue(flworPlan.getValue());
                resOp = rOp;
            } else {
                SubplanOperator s = new SubplanOperator(rOp);
                s.getInputs().add(flworPlan);
                resOp = s;
                baseOp.setValue(new NestedTupleSourceOperator(new MutableObject<ILogicalOperator>(s)));
            }
            Mutable<ILogicalOperator> resOpRef = new MutableObject<ILogicalOperator>(resOp);
            result = produceFlworPlan(noFlworClause, isTop, resOpRef, rRes.second);
        }
        if (!isTop) {
            context.existSubplan();
        }

        return result;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(Query q, Mutable<ILogicalOperator> tupSource)
            throws AsterixException {
        return q.getBody().accept(this, tupSource);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(DistinctClause dc, Mutable<ILogicalOperator> tupSource)
            throws AsterixException {
        List<Mutable<ILogicalExpression>> exprList = new ArrayList<Mutable<ILogicalExpression>>();
        Mutable<ILogicalOperator> input = null;
        for (Expression expr : dc.getDistinctByExpr()) {
            Pair<ILogicalExpression, Mutable<ILogicalOperator>> p = aqlExprToAlgExpression(expr, tupSource);
            exprList.add(new MutableObject<ILogicalExpression>(p.first));
            input = p.second;
        }
        DistinctOperator opDistinct = new DistinctOperator(exprList);
        opDistinct.getInputs().add(input);
        return new Pair<ILogicalOperator, LogicalVariable>(opDistinct, null);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(UnionExpr unionExpr, Mutable<ILogicalOperator> tupSource)
            throws AsterixException {
        //Translate the AQL union into an assign [var] <- [function-call: asterix:union, Args:[..]]
        //The rule "IntroduceUnionRule" will translates this assign operator into the UnionAll operator.
        Mutable<ILogicalOperator> ts = tupSource;
        LogicalVariable assignedVar = context.newVar();
        List<Mutable<ILogicalExpression>> inputVars = new ArrayList<Mutable<ILogicalExpression>>();
        for (Expression e : unionExpr.getExprs()) {
            Pair<ILogicalOperator, LogicalVariable> op_var = e.accept(this, ts);
            ts = new MutableObject<ILogicalOperator>(op_var.first);
            VariableReferenceExpression var = new VariableReferenceExpression(op_var.second);
            inputVars.add(new MutableObject<ILogicalExpression>(var));
        }
        AbstractFunctionCallExpression union = new ScalarFunctionCallExpression(
                FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.UNION), inputVars);
        AssignOperator a = new AssignOperator(assignedVar, new MutableObject<ILogicalExpression>(union));
        a.getInputs().add(ts);
        return new Pair<ILogicalOperator, LogicalVariable>(a, assignedVar);
    }

    private Pair<ILogicalOperator, LogicalVariable> produceFlworPlan(boolean noForClause, boolean isTop,
            Mutable<ILogicalOperator> resOpRef, LogicalVariable resVar) {
        if (isTop) {
            ProjectOperator pr = new ProjectOperator(resVar);
            pr.getInputs().add(resOpRef);
            return new Pair<ILogicalOperator, LogicalVariable>(pr, resVar);
        } else if (noForClause) {
            return new Pair<ILogicalOperator, LogicalVariable>(resOpRef.getValue(), resVar);
        } else {
            return aggListifyForSubquery(resVar, resOpRef, false);
        }
    }

}
