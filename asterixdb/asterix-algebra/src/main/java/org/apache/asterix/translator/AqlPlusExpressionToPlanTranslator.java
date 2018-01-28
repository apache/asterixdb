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
import java.util.HashMap;
import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.lang.aql.clause.JoinClause;
import org.apache.asterix.lang.aql.clause.MetaVariableClause;
import org.apache.asterix.lang.aql.expression.MetaVariableExpr;
import org.apache.asterix.lang.aql.visitor.base.IAQLPlusVisitor;
import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.Counter;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;

/**
 * This class is an extension of AQLExpressionToPlanTranslator. Specifically, it contains the visitor for
 * three extensions (MetaVariable, MetaClause, and JoinClause) to AQL in AQL+.
 * Meta-Variable ($$) refers the primary key or variable(s) in the logical plan.
 * Meta-Clause (##) refers the operator in the logical plan.
 * Join-Clause (join, loj) is required to build an explicit join in AQL level.
 * For more details of AQL+, refer to this thesis: www.ics.uci.edu/~rares/pub/phd-thesis-vernica.pdf
 */

public class AqlPlusExpressionToPlanTranslator extends AqlExpressionToPlanTranslator
        implements IAQLPlusVisitor<Pair<ILogicalOperator, LogicalVariable>, Mutable<ILogicalOperator>> {

    private MetaScopeLogicalVariable metaScopeExp = new MetaScopeLogicalVariable();
    private MetaScopeILogicalOperator metaScopeOp = new MetaScopeILogicalOperator();

    public AqlPlusExpressionToPlanTranslator(MetadataProvider metadataProvider, Counter currentVarCounter)
            throws AlgebricksException {
        super(metadataProvider, currentVarCounter);
        this.context.setTopFlwor(false);
    }

    public ILogicalPlan translate(List<Clause> clauses) throws AlgebricksException, CompilationException {
        if (clauses == null) {
            return null;
        }

        Mutable<ILogicalOperator> opRef = new MutableObject<ILogicalOperator>(new EmptyTupleSourceOperator());
        Pair<ILogicalOperator, LogicalVariable> p = null;
        for (Clause c : clauses) {
            p = c.accept(this, opRef);
            opRef = new MutableObject<ILogicalOperator>(p.first);
        }

        ArrayList<Mutable<ILogicalOperator>> globalPlanRoots = new ArrayList<Mutable<ILogicalOperator>>();

        ILogicalOperator topOp = p.first;

        globalPlanRoots.add(new MutableObject<ILogicalOperator>(topOp));
        ILogicalPlan plan = new ALogicalPlanImpl(globalPlanRoots);
        return plan;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitMetaVariableClause(MetaVariableClause mc,
            Mutable<ILogicalOperator> tupSource) throws CompilationException {
        return new Pair<ILogicalOperator, LogicalVariable>(metaScopeOp.get(mc.getVar()), null);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitJoinClause(JoinClause jc, Mutable<ILogicalOperator> tupSource)
            throws CompilationException {
        Mutable<ILogicalOperator> opRef = tupSource;
        Pair<ILogicalOperator, LogicalVariable> leftSide = null;
        for (Clause c : jc.getLeftClauses()) {
            leftSide = c.accept(this, opRef);
            opRef = new MutableObject<ILogicalOperator>(leftSide.first);
        }

        opRef = tupSource;
        Pair<ILogicalOperator, LogicalVariable> rightSide = null;
        for (Clause c : jc.getRightClauses()) {
            rightSide = c.accept(this, opRef);
            opRef = new MutableObject<ILogicalOperator>(rightSide.first);
        }

        Pair<ILogicalExpression, Mutable<ILogicalOperator>> whereCond =
                langExprToAlgExpression(jc.getWhereExpr(), tupSource);

        AbstractBinaryJoinOperator join;
        switch (jc.getKind()) {
            case INNER:
                join = new InnerJoinOperator(new MutableObject<ILogicalExpression>(whereCond.first));
                break;
            case LEFT_OUTER:
                join = new LeftOuterJoinOperator(new MutableObject<ILogicalExpression>(whereCond.first));
                break;
            default:
                throw new CompilationException(ErrorCode.COMPILATION_AQLPLUS_NO_SUCH_JOIN_TYPE);
        }
        join.getInputs().add(new MutableObject<ILogicalOperator>(leftSide.first));
        join.getInputs().add(new MutableObject<ILogicalOperator>(rightSide.first));
        return new Pair<ILogicalOperator, LogicalVariable>(join, null);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visitMetaVariableExpr(MetaVariableExpr me,
            Mutable<ILogicalOperator> tupSource) throws CompilationException {
        LogicalVariable var = context.newVar();
        AssignOperator a = new AssignOperator(var,
                new MutableObject<ILogicalExpression>(metaScopeExp.getVariableReferenceExpression(me.getVar())));
        a.getInputs().add(tupSource);
        return new Pair<ILogicalOperator, LogicalVariable>(a, var);
    }

    public void addVariableToMetaScope(Identifier id, LogicalVariable var) {
        metaScopeExp.put(id, var);
    }

    public void addOperatorToMetaScope(Identifier id, ILogicalOperator op) {
        metaScopeOp.put(id, op);
    }

    // This method was overridden because of METAVARIABLE_EXPRESSION case.
    @Override
    protected Pair<ILogicalExpression, Mutable<ILogicalOperator>> langExprToAlgExpression(Expression expr,
            Mutable<ILogicalOperator> topOpRef) throws CompilationException {
        switch (expr.getKind()) {
            case METAVARIABLE_EXPRESSION:
                ILogicalExpression le = metaScopeExp.getVariableReferenceExpression(((VariableExpr) expr).getVar());
                return new Pair<ILogicalExpression, Mutable<ILogicalOperator>>(le, topOpRef);
            default:
                return super.langExprToAlgExpression(expr, topOpRef);
        }
    }

    /**
     * This class refers to the primary-key or other variables in the given plan in AQL+ statements level.
     */
    private class MetaScopeLogicalVariable {
        private HashMap<Identifier, LogicalVariable> map = new HashMap<Identifier, LogicalVariable>();

        public VariableReferenceExpression getVariableReferenceExpression(Identifier id) throws CompilationException {
            LogicalVariable var = map.get(id);
            if (var == null) {
                throw new CompilationException(ErrorCode.COMPILATION_AQLPLUS_IDENTIFIER_NOT_FOUND, id.toString());
            }
            return new VariableReferenceExpression(var);
        }

        public void put(Identifier id, LogicalVariable var) {
            map.put(id, var);
        }
    }

    /**
     * This class refers to the operators in the given plan in AQL+ statements level.
     */
    private class MetaScopeILogicalOperator {
        private HashMap<Identifier, ILogicalOperator> map = new HashMap<Identifier, ILogicalOperator>();

        public ILogicalOperator get(Identifier id) throws CompilationException {
            ILogicalOperator op = map.get(id);
            if (op == null) {
                throw new CompilationException(ErrorCode.COMPILATION_AQLPLUS_IDENTIFIER_NOT_FOUND, id.toString());
            }
            return op;
        }

        public void put(Identifier id, ILogicalOperator op) {
            map.put(id, op);
        }
    }

}
