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
import java.util.Iterator;
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
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
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
        Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo = langExprToAlgExpression(inExpr, tupSource);
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
            context.exitSubplan();
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
            Pair<ILogicalExpression, Mutable<ILogicalOperator>> p = langExprToAlgExpression(expr, tupSource);
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
        List<Mutable<ILogicalOperator>> inputOpRefsToUnion = new ArrayList<>();
        List<LogicalVariable> vars = new ArrayList<>();
        for (Expression e : unionExpr.getExprs()) {
            // Visits the expression of one branch.
            Pair<ILogicalOperator, LogicalVariable> opAndVar = e.accept(this, tupSource);

            // Creates an unnest operator.
            LogicalVariable unnestVar = context.newVar();
            List<Mutable<ILogicalExpression>> args = new ArrayList<>();
            args.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(opAndVar.second)));
            UnnestOperator unnestOp = new UnnestOperator(unnestVar,
                    new MutableObject<ILogicalExpression>(new UnnestingFunctionCallExpression(
                            FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.SCAN_COLLECTION), args)));
            unnestOp.getInputs().add(new MutableObject<ILogicalOperator>(opAndVar.first));
            inputOpRefsToUnion.add(new MutableObject<ILogicalOperator>(unnestOp));
            vars.add(unnestVar);
        }

        // Creates a tree of binary union-all operators.
        UnionAllOperator topUnionAllOp = null;
        LogicalVariable topUnionVar = null;
        Iterator<Mutable<ILogicalOperator>> inputOpRefIterator = inputOpRefsToUnion.iterator();
        Mutable<ILogicalOperator> leftInputBranch = inputOpRefIterator.next();
        Iterator<LogicalVariable> inputVarIterator = vars.iterator();
        LogicalVariable leftInputVar = inputVarIterator.next();

        while (inputOpRefIterator.hasNext()) {
            // Generates the variable triple <leftVar, rightVar, outputVar> .
            topUnionVar = context.newVar();
            Triple<LogicalVariable, LogicalVariable, LogicalVariable> varTriple = new Triple<>(leftInputVar,
                    inputVarIterator.next(), topUnionVar);
            List<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> varTriples = new ArrayList<>();
            varTriples.add(varTriple);

            // Creates a binary union-all operator.
            topUnionAllOp = new UnionAllOperator(varTriples);
            topUnionAllOp.getInputs().add(leftInputBranch);
            topUnionAllOp.getInputs().add(inputOpRefIterator.next());

            // Re-assigns leftInputBranch and leftInputVar.
            leftInputBranch = new MutableObject<ILogicalOperator>(topUnionAllOp);
            leftInputVar = topUnionVar;
        }

        Pair<ILogicalOperator, LogicalVariable> result = aggListifyForSubquery(topUnionVar,
                new MutableObject<ILogicalOperator>(topUnionAllOp), false);
        return result;
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
