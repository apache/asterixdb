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

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistributeResultOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExtensionOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IndexInsertDeleteUpsertOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteUpsertOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IntersectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterUnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.MaterializeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.PartitioningSplitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.RunningAggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ScriptOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SinkOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.TokenizeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WriteOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WriteResultOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteUpsertOperator.Kind;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionVisitor;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public class LogicalOperatorPrettyPrintVisitor implements ILogicalOperatorVisitor<Void, Integer> {

    ILogicalExpressionVisitor<String, Integer> exprVisitor;
    AlgebricksAppendable buffer;

    public LogicalOperatorPrettyPrintVisitor() {
        this(new AlgebricksAppendable());
    }

    public LogicalOperatorPrettyPrintVisitor(Appendable app) {
        this(new AlgebricksAppendable(app), new LogicalExpressionPrettyPrintVisitor());
    }

    public LogicalOperatorPrettyPrintVisitor(AlgebricksAppendable buffer) {
        this(buffer, new LogicalExpressionPrettyPrintVisitor());
    }

    public LogicalOperatorPrettyPrintVisitor(AlgebricksAppendable buffer,
            ILogicalExpressionVisitor<String, Integer> exprVisitor) {
        reset(buffer);
        this.exprVisitor = exprVisitor;
    }

    public AlgebricksAppendable reset(AlgebricksAppendable buffer) {
        AlgebricksAppendable old = this.buffer;
        this.buffer = buffer;
        return old;
    }

    public AlgebricksAppendable get() {
        return buffer;
    }

    @Override
    public String toString() {
        return buffer.toString();
    }

    CharSequence str(Object o) {
        return String.valueOf(o);
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
        addIndent(indent).append("join (").append(op.getCondition().getValue().accept(exprVisitor, indent)).
        append(")");
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
        for (Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>> p : op.getOrderExpressions()) {
            if (op.getTopK() != -1) {
                buffer.append("(topK: " + op.getTopK() + ") ");
            }
            String fst = getOrderString(p.first);
            buffer.append("(" + fst + ", " + p.second.getValue().accept(exprVisitor, indent) + ") ");
        }
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
    public Void visitPartitioningSplitOperator(PartitioningSplitOperator op, Integer indent)
            throws AlgebricksException {
        addIndent(indent).append("partitioning-split (");
        pprintExprList(op.getExpressions(), indent);
        buffer.append(")");
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
        addIndent(indent).append("intersect (");

        buffer.append('[');
        for (int i = 0; i < op.getOutputVars().size(); i++) {
            if (i > 0) {
                buffer.append(", ");
            }
            buffer.append(str(op.getOutputVars().get(i)));
        }
        buffer.append("] <- [");
        for (int i = 0; i < op.getNumInput(); i++) {
            if (i > 0) {
                buffer.append(", ");
            }
            buffer.append('[');
            for (int j = 0; j < op.getInputVariables(i).size(); j++) {
                if (j > 0) {
                    buffer.append(", ");
                }
                buffer.append(str(op.getInputVariables(i).get(j)));
            }
            buffer.append(']');
        }
        buffer.append("])");
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
        return printAbstractUnnestMapOperator(op, indent, "unnest-map");
    }

    @Override
    public Void visitLeftOuterUnnestMapOperator(LeftOuterUnnestMapOperator op, Integer indent)
            throws AlgebricksException {
        return printAbstractUnnestMapOperator(op, indent, "left-outer-unnest-map");
    }

    private Void printAbstractUnnestMapOperator(AbstractUnnestMapOperator op, Integer indent, String opSignature)
            throws AlgebricksException {
        addIndent(indent).append(opSignature + " " + op.getVariables() + " <- "
                + op.getExpressionRef().getValue().accept(exprVisitor, indent));
        return null;
    }

    @Override
    public Void visitDataScanOperator(DataSourceScanOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append(
                "data-scan " + op.getProjectVariables() + "<-" + op.getVariables() + " <- " + op.getDataSource());
        return null;
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
            buffer.append(
                    " out: ([record-before-upsert:" + op.getBeforeOpRecordVar()
                            + ((op.getBeforeOpAdditionalNonFilteringVars() != null)
                                    ? (", additional-before-upsert: " + op.getBeforeOpAdditionalNonFilteringVars())
                                    : "")
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
    public Void visitSinkOperator(SinkOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("sink");
        return null;
    }

    @Override
    public Void visitExtensionOperator(ExtensionOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append(op.toString());
        return null;
    }

    protected AlgebricksAppendable addIndent(int level) throws AlgebricksException {
        for (int i = 0; i < level; ++i) {
            buffer.append(' ');
        }
        return buffer;
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
                PlanPrettyPrinter.printPlan(p, this, indent + 10);
                addIndent(indent).append("       }");
            }
        }
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
}
