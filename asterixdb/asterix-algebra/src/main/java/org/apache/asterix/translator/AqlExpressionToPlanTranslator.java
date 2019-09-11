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
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.aql.clause.DistinctClause;
import org.apache.asterix.lang.aql.clause.ForClause;
import org.apache.asterix.lang.aql.expression.FLWOGRExpression;
import org.apache.asterix.lang.aql.expression.UnionExpr;
import org.apache.asterix.lang.aql.visitor.base.IAQLVisitor;
import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Expression.Kind;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.Counter;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
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

    public AqlExpressionToPlanTranslator(MetadataProvider metadataProvider, int currentVarCounterValue)
            throws AlgebricksException {
        super(metadataProvider, currentVarCounterValue);
    }

    // Keeps the given Counter if one is provided instead of a value.
    public AqlExpressionToPlanTranslator(MetadataProvider metadataProvider, Counter currentVarCounter)
            throws AlgebricksException {
        super(metadataProvider, currentVarCounter);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(ForClause fc, Mutable<ILogicalOperator> tupSource)
            throws CompilationException {
        LogicalVariable v = context.newVarFromExpression(fc.getVarExpr());
        Expression inExpr = fc.getInExpr();
        Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo = langExprToAlgExpression(inExpr, tupSource);
        Pair<ILogicalExpression, Mutable<ILogicalOperator>> pUnnestExpr = makeUnnestExpression(eo.first, eo.second);
        ILogicalOperator returnedOp;
        if (fc.getPosVarExpr() == null) {
            returnedOp = new UnnestOperator(v, new MutableObject<>(pUnnestExpr.first));
        } else {
            LogicalVariable pVar = context.newVarFromExpression(fc.getPosVarExpr());
            // We set the positional variable type as INT64 type.
            returnedOp = new UnnestOperator(v, new MutableObject<>(pUnnestExpr.first), pVar, BuiltinType.AINT64,
                    new PositionWriter());
        }
        returnedOp.getInputs().add(pUnnestExpr.second);
        return new Pair<>(returnedOp, v);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(FLWOGRExpression flwor, Mutable<ILogicalOperator> tupSource)
            throws CompilationException {
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
            flworPlan = new MutableObject<>(pC.first);
        }

        Expression r = flwor.getReturnExpr();
        boolean noForClause = flwor.noForClause();

        Pair<ILogicalOperator, LogicalVariable> result;
        if (r.getKind() == Kind.VARIABLE_EXPRESSION) {
            VariableExpr v = (VariableExpr) r;
            LogicalVariable var = context.getVar(v.getVar().getId());
            result = produceFlworPlan(noForClause, isTop, flworPlan, var);
        } else {
            Mutable<ILogicalOperator> baseOp = new MutableObject<>(flworPlan.getValue());
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
            Mutable<ILogicalOperator> resOpRef = new MutableObject<>(resOp);
            result = produceFlworPlan(noForClause, isTop, resOpRef, rRes.second);
        }
        if (!isTop) {
            context.exitSubplan();
        }

        return result;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(Query q, Mutable<ILogicalOperator> tupSource)
            throws CompilationException {
        return q.getBody().accept(this, tupSource);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(DistinctClause dc, Mutable<ILogicalOperator> tupSource)
            throws CompilationException {
        List<Mutable<ILogicalExpression>> exprList = new ArrayList<>();
        Mutable<ILogicalOperator> input = null;
        for (Expression expr : dc.getDistinctByExpr()) {
            Pair<ILogicalExpression, Mutable<ILogicalOperator>> p = langExprToAlgExpression(expr, tupSource);
            exprList.add(new MutableObject<ILogicalExpression>(p.first));
            input = p.second;
        }
        DistinctOperator opDistinct = new DistinctOperator(exprList);
        opDistinct.getInputs().add(input);
        return new Pair<>(opDistinct, null);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(UnionExpr unionExpr, Mutable<ILogicalOperator> tupSource)
            throws CompilationException {
        List<ILangExpression> inputExprs = new ArrayList<>();
        inputExprs.addAll(unionExpr.getExprs());
        Pair<ILogicalOperator, LogicalVariable> result = translateUnionAllFromInputExprs(inputExprs, tupSource, null);
        return aggListifyForSubquery(result.second, new MutableObject<>(result.first), false);
    }

    @Override
    protected boolean expressionNeedsNoNesting(Expression expr) throws CompilationException {
        boolean isFLWOGR = expr.getKind() == Kind.FLWOGR_EXPRESSION;
        boolean letOnly = true;
        // No nesting is needed for a FLWOR expression that only has LETs and RETURN.
        if (isFLWOGR) {
            FLWOGRExpression flwor = (FLWOGRExpression) expr;
            for (Clause clause : flwor.getClauseList()) {
                letOnly &= clause.getClauseType() == Clause.ClauseType.LET_CLAUSE;
            }
        }
        return (isFLWOGR && letOnly) || super.expressionNeedsNoNesting(expr);
    }

    private Pair<ILogicalOperator, LogicalVariable> produceFlworPlan(boolean noForClause, boolean isTop,
            Mutable<ILogicalOperator> resOpRef, LogicalVariable resVar) {
        if (isTop) {
            ProjectOperator pr = new ProjectOperator(resVar);
            pr.getInputs().add(resOpRef);
            return new Pair<>(pr, resVar);
        } else if (noForClause) {
            ILogicalOperator resOp = resOpRef.getValue();
            if (resOp.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                return new Pair<>(resOp, resVar);
            }
            LogicalVariable newResVar = context.newVar();
            ILogicalOperator assign =
                    new AssignOperator(newResVar, new MutableObject<>(new VariableReferenceExpression(resVar)));
            assign.getInputs().add(resOpRef);
            return new Pair<>(assign, newResVar);
        } else {
            return aggListifyForSubquery(resVar, resOpRef, false);
        }
    }

}
