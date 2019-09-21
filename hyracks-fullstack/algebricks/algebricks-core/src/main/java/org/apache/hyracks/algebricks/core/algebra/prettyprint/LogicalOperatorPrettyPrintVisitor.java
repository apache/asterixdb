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
package org.apache.hyracks.algebricks.core.algebra.prettyprint;

import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IPhysicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DelegateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistributeResultOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ForwardOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IndexInsertDeleteUpsertOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteUpsertOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteUpsertOperator.Kind;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IntersectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterUnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.MaterializeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.RunningAggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ScriptOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SinkOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SplitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.TokenizeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WindowOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WriteOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WriteResultOperator;

public class LogicalOperatorPrettyPrintVisitor extends AbstractLogicalOperatorPrettyPrintVisitor<Integer>
        implements IPlanPrettyPrinter {

    private static final int INIT_INDENT = 2;
    private static final int SUBPLAN_INDENT = INIT_INDENT * 5;

    LogicalOperatorPrettyPrintVisitor() {
        super(new LogicalExpressionPrettyPrintVisitor<>());
    }

    @Override
    public final IPlanPrettyPrinter reset() throws AlgebricksException {
        resetState();
        return this;
    }

    @Override
    public final IPlanPrettyPrinter printPlan(ILogicalPlan plan) throws AlgebricksException {
        printPlanImpl(plan, 0);
        return this;
    }

    @Override
    public final IPlanPrettyPrinter printOperator(AbstractLogicalOperator op) throws AlgebricksException {
        printOperatorImpl(op, 0);
        return this;
    }

    private void printPlanImpl(ILogicalPlan plan, int indent) throws AlgebricksException {
        for (Mutable<ILogicalOperator> root : plan.getRoots()) {
            printOperatorImpl((AbstractLogicalOperator) root.getValue(), indent);
        }
    }

    private void printOperatorImpl(AbstractLogicalOperator op, int indent) throws AlgebricksException {
        op.accept(this, indent);
        IPhysicalOperator pOp = op.getPhysicalOperator();

        if (pOp != null) {
            buffer.append("\n");
            pad(buffer, indent);
            appendln(buffer, "-- " + pOp.toString() + "  |" + op.getExecutionMode() + "|");
        } else {
            appendln(buffer, " -- |" + op.getExecutionMode() + "|");
        }

        for (Mutable<ILogicalOperator> i : op.getInputs()) {
            printOperatorImpl((AbstractLogicalOperator) i.getValue(), indent + INIT_INDENT);
        }
    }

    @Override
    public Void visitAggregateOperator(AggregateOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("aggregate ").append(str(op.getVariables())).append(" <- ");
        pprintExprList(op.getExpressions(), indent);
        return null;
    }

    @Override
    public Void visitRunningAggregateOperator(RunningAggregateOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("running-aggregate ").append(str(op.getVariables())).append(" <- ");
        pprintExprList(op.getExpressions(), indent);
        return null;
    }

    @Override
    public Void visitEmptyTupleSourceOperator(EmptyTupleSourceOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("empty-tuple-source");
        return null;
    }

    @Override
    public Void visitGroupByOperator(GroupByOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("group by" + (op.isGroupAll() ? " (all)" : "") + " (");
        pprintVeList(op.getGroupByList(), indent);
        buffer.append(") decor (");
        pprintVeList(op.getDecorList(), indent);
        buffer.append(") {");
        printNestedPlans(op, indent);
        return null;
    }

    @Override
    public Void visitDistinctOperator(DistinctOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("distinct (");
        pprintExprList(op.getExpressions(), indent);
        buffer.append(")");
        return null;
    }

    @Override
    public Void visitInnerJoinOperator(InnerJoinOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("join (").append(op.getCondition().getValue().accept(exprVisitor, indent)).append(")");
        return null;
    }

    @Override
    public Void visitLeftOuterJoinOperator(LeftOuterJoinOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("left outer join (").append(op.getCondition().getValue().accept(exprVisitor, indent))
                .append(")");
        return null;
    }

    @Override
    public Void visitNestedTupleSourceOperator(NestedTupleSourceOperator op, Integer indent)
            throws AlgebricksException {
        addIndent(indent).append("nested tuple source");
        return null;
    }

    @Override
    public Void visitOrderOperator(OrderOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("order ");
        if (op.getTopK() != -1) {
            buffer.append("(topK: " + op.getTopK() + ") ");
        }
        pprintOrderList(op.getOrderExpressions(), indent);
        return null;
    }

    private String getOrderString(OrderOperator.IOrder first) {
        switch (first.getKind()) {
            case ASC:
                return "ASC";
            case DESC:
                return "DESC";
            default:
                return first.getExpressionRef().toString();
        }
    }

    @Override
    public Void visitAssignOperator(AssignOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("assign ").append(str(op.getVariables())).append(" <- ");
        pprintExprList(op.getExpressions(), indent);
        return null;
    }

    @Override
    public Void visitWriteOperator(WriteOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("write ");
        pprintExprList(op.getExpressions(), indent);
        return null;
    }

    @Override
    public Void visitDistributeResultOperator(DistributeResultOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("distribute result ");
        pprintExprList(op.getExpressions(), indent);
        return null;
    }

    @Override
    public Void visitWriteResultOperator(WriteResultOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("load ").append(str(op.getDataSource())).append(" from ")
                .append(op.getPayloadExpression().getValue().accept(exprVisitor, indent)).append(" partitioned by ");
        pprintExprList(op.getKeyExpressions(), indent);
        return null;
    }

    @Override
    public Void visitSelectOperator(SelectOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("select (").append(op.getCondition().getValue().accept(exprVisitor, indent))
                .append(")");
        return null;
    }

    @Override
    public Void visitProjectOperator(ProjectOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("project " + "(" + op.getVariables() + ")");
        return null;
    }

    @Override
    public Void visitSubplanOperator(SubplanOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("subplan {");
        printNestedPlans(op, indent);
        return null;
    }

    @Override
    public Void visitUnionOperator(UnionAllOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("union");
        for (Triple<LogicalVariable, LogicalVariable, LogicalVariable> v : op.getVariableMappings()) {
            buffer.append(" (" + v.first + ", " + v.second + ", " + v.third + ")");
        }
        return null;
    }

    @Override
    public Void visitIntersectOperator(IntersectOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("intersect ");
        pprintVarList(op.getOutputCompareVariables());
        if (op.hasExtraVariables()) {
            buffer.append(" extra ");
            pprintVarList(op.getOutputExtraVariables());
        }
        buffer.append(" <- [");
        for (int i = 0, n = op.getNumInput(); i < n; i++) {
            if (i > 0) {
                buffer.append(", ");
            }
            pprintVarList(op.getInputCompareVariables(i));
            if (op.hasExtraVariables()) {
                buffer.append(" extra ");
                pprintVarList(op.getInputExtraVariables(i));
            }
        }
        buffer.append(']');
        return null;
    }

    @Override
    public Void visitUnnestOperator(UnnestOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("unnest " + op.getVariable());
        if (op.getPositionalVariable() != null) {
            buffer.append(" at " + op.getPositionalVariable());
        }
        buffer.append(" <- " + op.getExpressionRef().getValue().accept(exprVisitor, indent));
        return null;
    }

    @Override
    public Void visitLeftOuterUnnestOperator(LeftOuterUnnestOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("outer-unnest " + op.getVariable());
        if (op.getPositionalVariable() != null) {
            buffer.append(" at " + op.getPositionalVariable());
        }
        buffer.append(" <- " + op.getExpressionRef().getValue().accept(exprVisitor, indent));
        return null;
    }

    @Override
    public Void visitUnnestMapOperator(UnnestMapOperator op, Integer indent) throws AlgebricksException {
        AlgebricksStringBuilderWriter plan = printAbstractUnnestMapOperator(op, indent, "unnest-map");
        appendSelectConditionInformation(plan, op.getSelectCondition(), indent);
        appendLimitInformation(plan, op.getOutputLimit());
        return null;
    }

    @Override
    public Void visitLeftOuterUnnestMapOperator(LeftOuterUnnestMapOperator op, Integer indent)
            throws AlgebricksException {
        printAbstractUnnestMapOperator(op, indent, "left-outer-unnest-map");
        return null;
    }

    private AlgebricksStringBuilderWriter printAbstractUnnestMapOperator(AbstractUnnestMapOperator op, Integer indent,
            String opSignature) throws AlgebricksException {
        AlgebricksStringBuilderWriter plan = addIndent(indent).append(opSignature + " " + op.getVariables() + " <- "
                + op.getExpressionRef().getValue().accept(exprVisitor, indent));
        appendFilterInformation(plan, op.getMinFilterVars(), op.getMaxFilterVars());
        return plan;
    }

    @Override
    public Void visitDataScanOperator(DataSourceScanOperator op, Integer indent) throws AlgebricksException {
        AlgebricksStringBuilderWriter plan = addIndent(indent).append(
                "data-scan " + op.getProjectVariables() + "<-" + op.getVariables() + " <- " + op.getDataSource());
        appendFilterInformation(plan, op.getMinFilterVars(), op.getMaxFilterVars());
        appendSelectConditionInformation(plan, op.getSelectCondition(), indent);
        appendLimitInformation(plan, op.getOutputLimit());
        return null;
    }

    private void appendSelectConditionInformation(AlgebricksStringBuilderWriter plan,
            Mutable<ILogicalExpression> selectCondition, Integer indent) throws AlgebricksException {
        if (selectCondition != null) {
            plan.append(" condition (").append(selectCondition.getValue().accept(exprVisitor, indent)).append(")");
        }
    }

    private void appendLimitInformation(AlgebricksStringBuilderWriter plan, long outputLimit)
            throws AlgebricksException {
        if (outputLimit >= 0) {
            plan.append(" limit ").append(String.valueOf(outputLimit));
        }
    }

    private void appendFilterInformation(AlgebricksStringBuilderWriter plan, List<LogicalVariable> minFilterVars,
            List<LogicalVariable> maxFilterVars) throws AlgebricksException {
        if (minFilterVars != null || maxFilterVars != null) {
            plan.append(" with filter on");
        }
        if (minFilterVars != null) {
            plan.append(" min:" + minFilterVars);
        }
        if (maxFilterVars != null) {
            plan.append(" max:" + maxFilterVars);
        }
    }

    @Override
    public Void visitLimitOperator(LimitOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("limit " + op.getMaxObjects().getValue().accept(exprVisitor, indent));
        ILogicalExpression offset = op.getOffset().getValue();
        if (offset != null) {
            buffer.append(", " + offset.accept(exprVisitor, indent));
        }
        return null;
    }

    @Override
    public Void visitExchangeOperator(ExchangeOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("exchange");
        return null;
    }

    @Override
    public Void visitScriptOperator(ScriptOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("script (in: " + op.getInputVariables() + ") (out: " + op.getOutputVariables() + ")");
        return null;
    }

    @Override
    public Void visitReplicateOperator(ReplicateOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("replicate");
        return null;
    }

    @Override
    public Void visitSplitOperator(SplitOperator op, Integer indent) throws AlgebricksException {
        Mutable<ILogicalExpression> branchingExpression = op.getBranchingExpression();
        addIndent(indent).append("split (" + branchingExpression.getValue().accept(exprVisitor, indent) + ")");
        return null;
    }

    @Override
    public Void visitMaterializeOperator(MaterializeOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("materialize");
        return null;
    }

    @Override
    public Void visitInsertDeleteUpsertOperator(InsertDeleteUpsertOperator op, Integer indent)
            throws AlgebricksException {
        String header = getIndexOpString(op.getOperation());
        addIndent(indent).append(header).append(str(op.getDataSource())).append(" from record: ")
                .append(op.getPayloadExpression().getValue().accept(exprVisitor, indent));
        if (op.getAdditionalNonFilteringExpressions() != null) {
            buffer.append(", meta: ");
            pprintExprList(op.getAdditionalNonFilteringExpressions(), indent);
        }
        buffer.append(" partitioned by ");
        pprintExprList(op.getPrimaryKeyExpressions(), indent);
        if (op.getOperation() == Kind.UPSERT) {
            buffer.append(" out: ([record-before-upsert:" + op.getBeforeOpRecordVar()
                    + ((op.getBeforeOpAdditionalNonFilteringVars() != null)
                            ? (", additional-before-upsert: " + op.getBeforeOpAdditionalNonFilteringVars()) : "")
                    + "]) ");
        }
        if (op.isBulkload()) {
            buffer.append(" [bulkload]");
        }
        return null;
    }

    @Override
    public Void visitIndexInsertDeleteUpsertOperator(IndexInsertDeleteUpsertOperator op, Integer indent)
            throws AlgebricksException {
        String header = getIndexOpString(op.getOperation());
        addIndent(indent).append(header).append(op.getIndexName()).append(" on ")
                .append(str(op.getDataSourceIndex().getDataSource())).append(" from ");
        if (op.getOperation() == Kind.UPSERT) {
            buffer.append(" replace:");
            pprintExprList(op.getPrevSecondaryKeyExprs(), indent);
            buffer.append(" with:");
            pprintExprList(op.getSecondaryKeyExpressions(), indent);
        } else {
            pprintExprList(op.getSecondaryKeyExpressions(), indent);
        }
        if (op.isBulkload()) {
            buffer.append(" [bulkload]");
        }
        return null;
    }

    public String getIndexOpString(Kind opKind) {
        switch (opKind) {
            case DELETE:
                return "delete from ";
            case INSERT:
                return "insert into ";
            case UPSERT:
                return "upsert into ";
        }
        return null;
    }

    @Override
    public Void visitTokenizeOperator(TokenizeOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("tokenize ").append(str(op.getTokenizeVars())).append(" <- ");
        pprintExprList(op.getSecondaryKeyExpressions(), indent);
        return null;
    }

    @Override
    public Void visitForwardOperator(ForwardOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent)
                .append("forward: range-map = " + op.getSideDataExpression().getValue().accept(exprVisitor, indent));
        return null;
    }

    @Override
    public Void visitSinkOperator(SinkOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("sink");
        return null;
    }

    @Override
    public Void visitDelegateOperator(DelegateOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append(op.toString());
        return null;
    }

    @Override
    public Void visitWindowOperator(WindowOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("window-aggregate ").append(str(op.getVariables())).append(" <- ");
        pprintExprList(op.getExpressions(), indent);
        if (!op.getPartitionExpressions().isEmpty()) {
            buffer.append(" partition ");
            pprintExprList(op.getPartitionExpressions(), indent);
        }
        if (!op.getOrderExpressions().isEmpty()) {
            buffer.append(" order ");
            pprintOrderList(op.getOrderExpressions(), indent);
        }
        if (op.hasNestedPlans()) {
            buffer.append(" frame on ");
            pprintOrderList(op.getFrameValueExpressions(), indent);
            buffer.append(" start ");
            List<Mutable<ILogicalExpression>> frameStartExpressions = op.getFrameStartExpressions();
            if (!frameStartExpressions.isEmpty()) {
                pprintExprList(frameStartExpressions, indent);
                List<Mutable<ILogicalExpression>> frameStartValidationExpressions =
                        op.getFrameStartValidationExpressions();
                if (!frameStartValidationExpressions.isEmpty()) {
                    buffer.append(" if ");
                    pprintExprList(frameStartValidationExpressions, indent);
                }
            } else {
                buffer.append("unbounded");
            }
            buffer.append(" end ");
            List<Mutable<ILogicalExpression>> frameEndExpressions = op.getFrameEndExpressions();
            if (!frameEndExpressions.isEmpty()) {
                pprintExprList(frameEndExpressions, indent);
                List<Mutable<ILogicalExpression>> frameEndValidationExpressions = op.getFrameEndValidationExpressions();
                if (!frameEndValidationExpressions.isEmpty()) {
                    buffer.append(" if ");
                    pprintExprList(frameEndValidationExpressions, indent);
                }
            } else {
                buffer.append("unbounded");
            }
            List<Mutable<ILogicalExpression>> frameExcludeExpressions = op.getFrameExcludeExpressions();
            if (!frameExcludeExpressions.isEmpty()) {
                buffer.append(" exclude ");
                int negStartIdx = op.getFrameExcludeNegationStartIdx();
                if (negStartIdx >= 0 && op.getFrameExcludeNegationStartIdx() < frameExcludeExpressions.size()) {
                    pprintExprList(frameExcludeExpressions.subList(0, negStartIdx), indent);
                    buffer.append(" and not ");
                    pprintExprList(frameExcludeExpressions.subList(negStartIdx, frameExcludeExpressions.size()),
                            indent);
                } else {
                    pprintExprList(frameExcludeExpressions, indent);
                }
            }
            Mutable<ILogicalExpression> frameExcludeUnaryExpression = op.getFrameExcludeUnaryExpression();
            if (frameExcludeUnaryExpression.getValue() != null) {
                buffer.append(" exclude unary ");
                buffer.append(frameExcludeUnaryExpression.getValue().accept(exprVisitor, indent));
            }
            Mutable<ILogicalExpression> frameOffsetExpression = op.getFrameOffsetExpression();
            if (frameOffsetExpression.getValue() != null) {
                buffer.append(" offset ");
                buffer.append(frameOffsetExpression.getValue().accept(exprVisitor, indent));
            }
            int frameMaxObjects = op.getFrameMaxObjects();
            if (frameMaxObjects != -1) {
                buffer.append(" maxObjects " + frameMaxObjects);
            }

            buffer.append(" {");
            printNestedPlans(op, indent);
        }
        return null;
    }

    protected void printNestedPlans(AbstractOperatorWithNestedPlans op, Integer indent) throws AlgebricksException {
        boolean first = true;
        if (op.getNestedPlans().isEmpty()) {
            buffer.append("}");
        } else {
            for (ILogicalPlan p : op.getNestedPlans()) {
                // PrettyPrintUtil.indent(buffer, level + 10).append("var " +
                // p.first + ":\n");
                buffer.append("\n");
                if (first) {
                    first = false;
                } else {
                    addIndent(indent).append("       {\n");
                }
                printPlanImpl(p, indent + SUBPLAN_INDENT);
                addIndent(indent).append("       }");
            }
        }
    }

    protected void pprintVarList(List<LogicalVariable> variables) throws AlgebricksException {
        buffer.append('[');
        boolean first = true;
        for (LogicalVariable var : variables) {
            if (first) {
                first = false;
            } else {
                buffer.append(", ");
            }
            buffer.append(str(var));
        }
        buffer.append(']');
    }

    protected void pprintExprList(List<Mutable<ILogicalExpression>> expressions, Integer indent)
            throws AlgebricksException {
        buffer.append("[");
        boolean first = true;
        for (Mutable<ILogicalExpression> exprRef : expressions) {
            if (first) {
                first = false;
            } else {
                buffer.append(", ");
            }
            buffer.append(exprRef.getValue().accept(exprVisitor, indent));
        }
        buffer.append("]");
    }

    protected void pprintVeList(List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> vePairList, Integer indent)
            throws AlgebricksException {
        buffer.append("[");
        boolean fst = true;
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> ve : vePairList) {
            if (fst) {
                fst = false;
            } else {
                buffer.append("; ");
            }
            if (ve.first != null) {
                buffer.append(ve.first + " := " + ve.second);
            } else {
                buffer.append(ve.second.getValue().accept(exprVisitor, indent));
            }
        }
        buffer.append("]");
    }

    private void pprintOrderList(List<Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>>> orderList,
            Integer indent) throws AlgebricksException {
        for (Iterator<Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>>> i = orderList.iterator(); i.hasNext();) {
            Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>> p = i.next();
            String fst = getOrderString(p.first);
            buffer.append("(" + fst + ", " + p.second.getValue().accept(exprVisitor, indent) + ")");
            if (i.hasNext()) {
                buffer.append(' ');
            }
        }
    }
}
