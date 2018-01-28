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
package org.apache.hyracks.algebricks.core.utils;

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DelegateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistributeResultOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
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
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WriteOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WriteResultOperator;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public class LogicalOperatorDotVisitor implements ILogicalOperatorVisitor<String, Void> {

    private final StringBuilder stringBuilder;

    public LogicalOperatorDotVisitor() {
        stringBuilder = new StringBuilder();
    }

    @Override
    public String toString() {
        return "";
    }

    private CharSequence str(Object o) {
        return String.valueOf(o);
    }

    @Override
    public String visitAggregateOperator(AggregateOperator op, Void noArgs) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("aggregate ").append(str(op.getVariables())).append(" <- ");
        pprintExprList(op.getExpressions());
        return stringBuilder.toString();
    }

    @Override
    public String visitRunningAggregateOperator(RunningAggregateOperator op, Void noArgs) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("running-aggregate ").append(str(op.getVariables())).append(" <- ");
        pprintExprList(op.getExpressions());
        return stringBuilder.toString();
    }

    @Override
    public String visitEmptyTupleSourceOperator(EmptyTupleSourceOperator op, Void noArgs) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("empty-tuple-source");
        return stringBuilder.toString();
    }

    @Override
    public String visitGroupByOperator(GroupByOperator op, Void noArgs) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("group by").append(op.isGroupAll() ? " (all)" : "").append(" (");
        pprintVeList(op.getGroupByList());
        stringBuilder.append(") decor (");
        pprintVeList(op.getDecorList());
        stringBuilder.append(")");
        return stringBuilder.toString();
    }

    @Override
    public String visitDistinctOperator(DistinctOperator op, Void noArgs) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("distinct (");
        pprintExprList(op.getExpressions());
        stringBuilder.append(")");
        return stringBuilder.toString();
    }

    @Override
    public String visitInnerJoinOperator(InnerJoinOperator op, Void noArgs) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("join (").append(op.getCondition().getValue().toString()).append(")");
        return stringBuilder.toString();
    }

    @Override
    public String visitLeftOuterJoinOperator(LeftOuterJoinOperator op, Void noArgs) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("left outer join (").append(op.getCondition().getValue().toString()).append(")");
        return stringBuilder.toString();
    }

    @Override
    public String visitNestedTupleSourceOperator(NestedTupleSourceOperator op, Void noArgs) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("nested tuple source");
        return stringBuilder.toString();
    }

    @Override
    public String visitOrderOperator(OrderOperator op, Void noArgs) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("order ");
        for (Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>> p : op.getOrderExpressions()) {
            if (op.getTopK() != -1) {
                stringBuilder.append("(topK: ").append(op.getTopK()).append(") ");
            }
            String fst = getOrderString(p.first);
            stringBuilder.append("(").append(fst).append(", ").append(p.second.getValue().toString()).append(") ");
        }
        return stringBuilder.toString();
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
    public String visitAssignOperator(AssignOperator op, Void noArgs) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("assign ").append(str(op.getVariables())).append(" <- ");
        pprintExprList(op.getExpressions());
        return stringBuilder.toString();
    }

    @Override
    public String visitWriteOperator(WriteOperator op, Void noArgs) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("write ");
        pprintExprList(op.getExpressions());
        return stringBuilder.toString();
    }

    @Override
    public String visitDistributeResultOperator(DistributeResultOperator op, Void noArgs) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("distribute result ");
        pprintExprList(op.getExpressions());
        return stringBuilder.toString();
    }

    @Override
    public String visitWriteResultOperator(WriteResultOperator op, Void noArgs) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("load ").append(str(op.getDataSource())).append(" from ")
                .append(op.getPayloadExpression().getValue().toString()).append(" partitioned by ");
        pprintExprList(op.getKeyExpressions());
        return stringBuilder.toString();
    }

    @Override
    public String visitSelectOperator(SelectOperator op, Void noArgs) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("select (").append(op.getCondition().getValue().toString()).append(")");
        return stringBuilder.toString();
    }

    @Override
    public String visitProjectOperator(ProjectOperator op, Void noArgs) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("project ").append("(").append(op.getVariables()).append(")");
        return stringBuilder.toString();
    }

    @Override
    public String visitSubplanOperator(SubplanOperator op, Void noArgs) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("subplan {}");
        return stringBuilder.toString();
    }

    @Override
    public String visitUnionOperator(UnionAllOperator op, Void noArgs) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("union");
        for (Triple<LogicalVariable, LogicalVariable, LogicalVariable> v : op.getVariableMappings()) {
            stringBuilder.append(" (").append(v.first).append(", ").append(v.second).append(", ").append(v.third)
                    .append(")");
        }
        return stringBuilder.toString();
    }

    @Override
    public String visitIntersectOperator(IntersectOperator op, Void noArgs) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("intersect (");
        stringBuilder.append('[');
        for (int i = 0; i < op.getOutputVars().size(); i++) {
            if (i > 0) {
                stringBuilder.append(", ");
            }
            stringBuilder.append(str(op.getOutputVars().get(i)));
        }
        stringBuilder.append("] <- [");
        for (int i = 0; i < op.getNumInput(); i++) {
            if (i > 0) {
                stringBuilder.append(", ");
            }
            stringBuilder.append('[');
            for (int j = 0; j < op.getInputVariables(i).size(); j++) {
                if (j > 0) {
                    stringBuilder.append(", ");
                }
                stringBuilder.append(str(op.getInputVariables(i).get(j)));
            }
            stringBuilder.append(']');
        }
        stringBuilder.append("])");
        return stringBuilder.toString();
    }

    @Override
    public String visitUnnestOperator(UnnestOperator op, Void noArgs) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("unnest ").append(op.getVariable());
        if (op.getPositionalVariable() != null) {
            stringBuilder.append(" at ").append(op.getPositionalVariable());
        }
        stringBuilder.append(" <- ").append(op.getExpressionRef().getValue().toString());
        return stringBuilder.toString();
    }

    @Override
    public String visitLeftOuterUnnestOperator(LeftOuterUnnestOperator op, Void noArgs) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("outer-unnest ").append(op.getVariable());
        if (op.getPositionalVariable() != null) {
            stringBuilder.append(" at ").append(op.getPositionalVariable());
        }
        stringBuilder.append(" <- ").append(op.getExpressionRef().getValue().toString());
        return stringBuilder.toString();
    }

    @Override
    public String visitUnnestMapOperator(UnnestMapOperator op, Void noArgs) throws AlgebricksException {
        stringBuilder.setLength(0);
        return printAbstractUnnestMapOperator(op, "unnest-map");
    }

    @Override
    public String visitLeftOuterUnnestMapOperator(LeftOuterUnnestMapOperator op, Void noArgs)
            throws AlgebricksException {
        stringBuilder.setLength(0);
        return printAbstractUnnestMapOperator(op, "left-outer-unnest-map");
    }

    private String printAbstractUnnestMapOperator(AbstractUnnestMapOperator op, String opSignature) {
        stringBuilder.append(opSignature).append(" ").append(op.getVariables()).append(" <- ")
                .append(op.getExpressionRef().getValue().toString());
        appendFilterInformation(stringBuilder, op.getMinFilterVars(), op.getMaxFilterVars());
        return stringBuilder.toString();
    }

    @Override
    public String visitDataScanOperator(DataSourceScanOperator op, Void noArgs) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("data-scan ").append(op.getProjectVariables()).append("<-").append(op.getVariables())
                .append(" <- ").append(op.getDataSource());
        appendFilterInformation(stringBuilder, op.getMinFilterVars(), op.getMaxFilterVars());
        return stringBuilder.toString();
    }

    private String appendFilterInformation(StringBuilder plan, List<LogicalVariable> minFilterVars,
            List<LogicalVariable> maxFilterVars) {
        if (minFilterVars != null || maxFilterVars != null) {
            plan.append(" with filter on");
        }
        if (minFilterVars != null) {
            plan.append(" min:").append(minFilterVars);
        }
        if (maxFilterVars != null) {
            plan.append(" max:").append(maxFilterVars);
        }
        return stringBuilder.toString();
    }

    @Override
    public String visitLimitOperator(LimitOperator op, Void noArgs) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("limit ").append(op.getMaxObjects().getValue().toString());
        ILogicalExpression offset = op.getOffset().getValue();
        if (offset != null) {
            stringBuilder.append(", ").append(offset.toString());
        }
        return stringBuilder.toString();
    }

    @Override
    public String visitExchangeOperator(ExchangeOperator op, Void noArgs) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("exchange");
        return stringBuilder.toString();
    }

    @Override
    public String visitScriptOperator(ScriptOperator op, Void noArgs) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("script (in: ").append(op.getInputVariables()).append(") (out: ")
                .append(op.getOutputVariables()).append(")");
        return stringBuilder.toString();
    }

    @Override
    public String visitReplicateOperator(ReplicateOperator op, Void noArgs) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("replicate");
        return stringBuilder.toString();
    }

    @Override
    public String visitSplitOperator(SplitOperator op, Void noArgs) throws AlgebricksException {
        stringBuilder.setLength(0);
        Mutable<ILogicalExpression> branchingExpression = op.getBranchingExpression();
        stringBuilder.append("split ").append(branchingExpression.getValue().toString());
        return stringBuilder.toString();
    }

    @Override
    public String visitMaterializeOperator(MaterializeOperator op, Void noArgs) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("materialize");
        return stringBuilder.toString();
    }

    @Override
    public String visitInsertDeleteUpsertOperator(InsertDeleteUpsertOperator op, Void noArgs)
            throws AlgebricksException {
        stringBuilder.setLength(0);
        String header = getIndexOpString(op.getOperation());
        stringBuilder.append(header).append(str(op.getDataSource())).append(" from record: ")
                .append(op.getPayloadExpression().getValue().toString());
        if (op.getAdditionalNonFilteringExpressions() != null) {
            stringBuilder.append(", meta: ");
            pprintExprList(op.getAdditionalNonFilteringExpressions());
        }
        stringBuilder.append(" partitioned by ");
        pprintExprList(op.getPrimaryKeyExpressions());
        if (op.getOperation() == Kind.UPSERT) {
            stringBuilder.append(" out: ([record-before-upsert:").append(op.getBeforeOpRecordVar());
            if (op.getBeforeOpAdditionalNonFilteringVars() != null) {
                stringBuilder.append(", additional-before-upsert: ").append(op.getBeforeOpAdditionalNonFilteringVars());
            }
            stringBuilder.append("]) ");
        }
        if (op.isBulkload()) {
            stringBuilder.append(" [bulkload]");
        }
        return stringBuilder.toString();
    }

    @Override
    public String visitIndexInsertDeleteUpsertOperator(IndexInsertDeleteUpsertOperator op, Void noArgs)
            throws AlgebricksException {
        stringBuilder.setLength(0);
        String header = getIndexOpString(op.getOperation());
        stringBuilder.append(header).append(op.getIndexName()).append(" on ")
                .append(str(op.getDataSourceIndex().getDataSource())).append(" from ");
        if (op.getOperation() == Kind.UPSERT) {
            stringBuilder.append(" replace:");
            pprintExprList(op.getPrevSecondaryKeyExprs());
            stringBuilder.append(" with:");
            pprintExprList(op.getSecondaryKeyExpressions());
        } else {
            pprintExprList(op.getSecondaryKeyExpressions());
        }
        if (op.isBulkload()) {
            stringBuilder.append(" [bulkload]");
        }
        return stringBuilder.toString();
    }

    private String getIndexOpString(Kind opKind) {
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
    public String visitTokenizeOperator(TokenizeOperator op, Void noArgs) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("tokenize ").append(str(op.getTokenizeVars())).append(" <- ");
        pprintExprList(op.getSecondaryKeyExpressions());
        return stringBuilder.toString();
    }

    @Override
    public String visitSinkOperator(SinkOperator op, Void noArgs) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("sink");
        return stringBuilder.toString();
    }

    @Override
    public String visitDelegateOperator(DelegateOperator op, Void noArgs) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append(op.toString());
        return stringBuilder.toString();
    }

    private void pprintExprList(List<Mutable<ILogicalExpression>> expressions) {
        stringBuilder.append("[");
        boolean first = true;
        for (Mutable<ILogicalExpression> exprRef : expressions) {
            if (first) {
                first = false;
            } else {
                stringBuilder.append(", ");
            }
            stringBuilder.append(exprRef.getValue().toString());
        }
        stringBuilder.append("]");
    }

    private void pprintVeList(List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> vePairList) {
        stringBuilder.append("[");
        boolean fst = true;
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> ve : vePairList) {
            if (fst) {
                fst = false;
            } else {
                stringBuilder.append("; ");
            }
            if (ve.first != null) {
                stringBuilder.append(ve.first).append(" := ").append(ve.second);
            } else {
                stringBuilder.append(ve.second.getValue().toString());
            }
        }
        stringBuilder.append("]");
    }
}
