/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.common.utils.Triple;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DistributeResultOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ExtensionOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IndexInsertDeleteOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.PartitioningSplitOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.RunningAggregateOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ScriptOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SinkOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.WriteOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.WriteResultOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.typing.ITypingContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public class SubstituteVariableVisitor implements ILogicalOperatorVisitor<Void, Pair<LogicalVariable, LogicalVariable>> {

    private final boolean goThroughNts;
    private final ITypingContext ctx;

    public SubstituteVariableVisitor(boolean goThroughNts, ITypingContext ctx) {
        this.goThroughNts = goThroughNts;
        this.ctx = ctx;
    }

    @Override
    public Void visitAggregateOperator(AggregateOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        List<LogicalVariable> variables = op.getVariables();
        int n = variables.size();
        for (int i = 0; i < n; i++) {
            if (variables.get(i).equals(pair.first)) {
                variables.set(i, pair.second);
            } else {
                op.getExpressions().get(i).getValue().substituteVar(pair.first, pair.second);
            }
        }
        substVarTypes(op, pair);
        return null;
    }

    @Override
    public Void visitAssignOperator(AssignOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        List<LogicalVariable> variables = op.getVariables();
        int n = variables.size();
        for (int i = 0; i < n; i++) {
            if (variables.get(i).equals(pair.first)) {
                variables.set(i, pair.second);
            } else {
                op.getExpressions().get(i).getValue().substituteVar(pair.first, pair.second);
            }
        }
        substVarTypes(op, pair);
        return null;
    }

    @Override
    public Void visitDataScanOperator(DataSourceScanOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        List<LogicalVariable> variables = op.getVariables();
        for (int i = 0; i < variables.size(); i++) {
            if (variables.get(i) == pair.first) {
                variables.set(i, pair.second);
                return null;
            }
        }
        substVarTypes(op, pair);
        return null;
    }

    @Override
    public Void visitDistinctOperator(DistinctOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        for (Mutable<ILogicalExpression> eRef : op.getExpressions()) {
            eRef.getValue().substituteVar(pair.first, pair.second);
        }
        substVarTypes(op, pair);
        return null;
    }

    @Override
    public Void visitEmptyTupleSourceOperator(EmptyTupleSourceOperator op, Pair<LogicalVariable, LogicalVariable> pair) {
        // does not use any variable
        return null;
    }

    @Override
    public Void visitExchangeOperator(ExchangeOperator op, Pair<LogicalVariable, LogicalVariable> pair) {
        // does not use any variable
        return null;
    }

    @Override
    public Void visitGroupByOperator(GroupByOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        subst(pair.first, pair.second, op.getGroupByList());
        subst(pair.first, pair.second, op.getDecorList());
        for (ILogicalPlan p : op.getNestedPlans()) {
            for (Mutable<ILogicalOperator> r : p.getRoots()) {
                OperatorManipulationUtil.substituteVarRec((AbstractLogicalOperator) r.getValue(), pair.first,
                        pair.second, goThroughNts, ctx);
            }
        }
        substVarTypes(op, pair);
        return null;
    }

    @Override
    public Void visitInnerJoinOperator(InnerJoinOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        op.getCondition().getValue().substituteVar(pair.first, pair.second);
        substVarTypes(op, pair);
        return null;
    }

    @Override
    public Void visitLeftOuterJoinOperator(LeftOuterJoinOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        op.getCondition().getValue().substituteVar(pair.first, pair.second);
        substVarTypes(op, pair);
        return null;
    }

    @Override
    public Void visitLimitOperator(LimitOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        op.getMaxObjects().getValue().substituteVar(pair.first, pair.second);
        ILogicalExpression offset = op.getOffset().getValue();
        if (offset != null) {
            offset.substituteVar(pair.first, pair.second);
        }
        substVarTypes(op, pair);
        return null;
    }

    @Override
    public Void visitNestedTupleSourceOperator(NestedTupleSourceOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitOrderOperator(OrderOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        for (Pair<IOrder, Mutable<ILogicalExpression>> oe : op.getOrderExpressions()) {
            oe.second.getValue().substituteVar(pair.first, pair.second);
        }
        substVarTypes(op, pair);
        return null;
    }

    @Override
    public Void visitPartitioningSplitOperator(PartitioningSplitOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        for (Mutable<ILogicalExpression> e : op.getExpressions()) {
            e.getValue().substituteVar(pair.first, pair.second);
        }
        substVarTypes(op, pair);
        return null;
    }

    @Override
    public Void visitProjectOperator(ProjectOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        List<LogicalVariable> usedVariables = op.getVariables();
        int n = usedVariables.size();
        for (int i = 0; i < n; i++) {
            LogicalVariable v = usedVariables.get(i);
            if (v.equals(pair.first)) {
                usedVariables.set(i, pair.second);
            }
        }
        substVarTypes(op, pair);
        return null;
    }

    @Override
    public Void visitRunningAggregateOperator(RunningAggregateOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        List<LogicalVariable> variables = op.getVariables();
        int n = variables.size();
        for (int i = 0; i < n; i++) {
            if (variables.get(i).equals(pair.first)) {
                variables.set(i, pair.second);
            } else {
                op.getExpressions().get(i).getValue().substituteVar(pair.first, pair.second);
            }
        }
        substVarTypes(op, pair);
        return null;
    }

    @Override
    public Void visitScriptOperator(ScriptOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        substInArray(op.getInputVariables(), pair.first, pair.second);
        substInArray(op.getOutputVariables(), pair.first, pair.second);
        substVarTypes(op, pair);
        return null;
    }

    @Override
    public Void visitSelectOperator(SelectOperator op, Pair<LogicalVariable, LogicalVariable> pair) {
        op.getCondition().getValue().substituteVar(pair.first, pair.second);
        return null;
    }

    @Override
    public Void visitSubplanOperator(SubplanOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        for (ILogicalPlan p : op.getNestedPlans()) {
            for (Mutable<ILogicalOperator> r : p.getRoots()) {
                OperatorManipulationUtil.substituteVarRec((AbstractLogicalOperator) r.getValue(), pair.first,
                        pair.second, goThroughNts, ctx);
            }
        }
        return null;
    }

    @Override
    public Void visitUnionOperator(UnionAllOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        List<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> varMap = op.getVariableMappings();
        for (Triple<LogicalVariable, LogicalVariable, LogicalVariable> t : varMap) {
            if (t.first.equals(pair.first)) {
                t.first = pair.second;
            }
            if (t.second.equals(pair.first)) {
                t.second = pair.second;
            }
            if (t.third.equals(pair.first)) {
                t.third = pair.second;
            }
        }
        substVarTypes(op, pair);
        return null;
    }

    @Override
    public Void visitUnnestMapOperator(UnnestMapOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        List<LogicalVariable> variables = op.getVariables();
        for (int i = 0; i < variables.size(); i++) {
            if (variables.get(i) == pair.first) {
                variables.set(i, pair.second);
                return null;
            }
        }
        op.getExpressionRef().getValue().substituteVar(pair.first, pair.second);
        substVarTypes(op, pair);
        return null;
    }

    @Override
    public Void visitUnnestOperator(UnnestOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        List<LogicalVariable> variables = op.getVariables();
        for (int i = 0; i < variables.size(); i++) {
            if (variables.get(i) == pair.first) {
                variables.set(i, pair.second);
                return null;
            }
        }
        op.getExpressionRef().getValue().substituteVar(pair.first, pair.second);
        substVarTypes(op, pair);
        return null;
    }

    @Override
    public Void visitWriteOperator(WriteOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        for (Mutable<ILogicalExpression> e : op.getExpressions()) {
            e.getValue().substituteVar(pair.first, pair.second);
        }
        substVarTypes(op, pair);
        return null;
    }

    @Override
    public Void visitDistributeResultOperator(DistributeResultOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        for (Mutable<ILogicalExpression> e : op.getExpressions()) {
            e.getValue().substituteVar(pair.first, pair.second);
        }
        substVarTypes(op, pair);
        return null;
    }

    @Override
    public Void visitWriteResultOperator(WriteResultOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        op.getPayloadExpression().getValue().substituteVar(pair.first, pair.second);
        for (Mutable<ILogicalExpression> e : op.getKeyExpressions()) {
            e.getValue().substituteVar(pair.first, pair.second);
        }
        substVarTypes(op, pair);
        return null;
    }

    private void subst(LogicalVariable v1, LogicalVariable v2,
            List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> varExprPairList) {
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> ve : varExprPairList) {
            if (ve.first != null && ve.first.equals(v1)) {
                ve.first = v2;
                return;
            }
            ve.second.getValue().substituteVar(v1, v2);
        }
    }

    private void substInArray(ArrayList<LogicalVariable> varArray, LogicalVariable v1, LogicalVariable v2) {
        for (int i = 0; i < varArray.size(); i++) {
            LogicalVariable v = varArray.get(i);
            if (v == v1) {
                varArray.set(i, v2);
            }
        }
    }

    @Override
    public Void visitReplicateOperator(ReplicateOperator op, Pair<LogicalVariable, LogicalVariable> arg)
            throws AlgebricksException {
        op.substituteVar(arg.first, arg.second);
        return null;
    }

    @Override
    public Void visitInsertDeleteOperator(InsertDeleteOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        op.getPayloadExpression().getValue().substituteVar(pair.first, pair.second);
        for (Mutable<ILogicalExpression> e : op.getPrimaryKeyExpressions()) {
            e.getValue().substituteVar(pair.first, pair.second);
        }
        substVarTypes(op, pair);
        return null;
    }

    @Override
    public Void visitIndexInsertDeleteOperator(IndexInsertDeleteOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        for (Mutable<ILogicalExpression> e : op.getPrimaryKeyExpressions()) {
            e.getValue().substituteVar(pair.first, pair.second);
        }
        for (Mutable<ILogicalExpression> e : op.getSecondaryKeyExpressions()) {
            e.getValue().substituteVar(pair.first, pair.second);
        }
        substVarTypes(op, pair);
        return null;
    }

    @Override
    public Void visitSinkOperator(SinkOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        return null;
    }

    private void substVarTypes(ILogicalOperator op, Pair<LogicalVariable, LogicalVariable> arg)
            throws AlgebricksException {
        if (ctx == null) {
            return;
        }
        IVariableTypeEnvironment env = ctx.getOutputTypeEnvironment(op);
        env.substituteProducedVariable(arg.first, arg.second);
    }

    @Override
    public Void visitExtensionOperator(ExtensionOperator op, Pair<LogicalVariable, LogicalVariable> arg)
            throws AlgebricksException {
        return null;
    }
}
