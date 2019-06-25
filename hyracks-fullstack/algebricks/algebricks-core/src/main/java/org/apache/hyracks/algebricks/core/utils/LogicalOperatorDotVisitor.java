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

import static org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty.PropertyType.LOCAL_GROUPING_PROPERTY;
import static org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty.PropertyType.LOCAL_ORDER_PROPERTY;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.IPhysicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
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
import org.apache.hyracks.algebricks.core.algebra.properties.DefaultNodeGroupDomain;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public class LogicalOperatorDotVisitor implements ILogicalOperatorVisitor<String, Boolean> {

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
    public String visitAggregateOperator(AggregateOperator op, Boolean showDetails) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("aggregate ").append(str(op.getVariables())).append(" <- ");
        printExprList(op.getExpressions());
        appendSchema(op, showDetails);
        appendAnnotations(op, showDetails);
        appendPhysicalOperatorInfo(op, showDetails);
        return stringBuilder.toString();
    }

    @Override
    public String visitRunningAggregateOperator(RunningAggregateOperator op, Boolean showDetails)
            throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("running-aggregate ").append(str(op.getVariables())).append(" <- ");
        printExprList(op.getExpressions());
        appendSchema(op, showDetails);
        appendAnnotations(op, showDetails);
        appendPhysicalOperatorInfo(op, showDetails);
        return stringBuilder.toString();
    }

    @Override
    public String visitEmptyTupleSourceOperator(EmptyTupleSourceOperator op, Boolean showDetails) {
        stringBuilder.setLength(0);
        stringBuilder.append("empty-tuple-source");
        appendSchema(op, showDetails);
        appendAnnotations(op, showDetails);
        appendPhysicalOperatorInfo(op, showDetails);
        return stringBuilder.toString();
    }

    @Override
    public String visitGroupByOperator(GroupByOperator op, Boolean showDetails) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("group by").append(op.isGroupAll() ? " (all)" : "").append(" (");
        printVariableAndExprList(op.getGroupByList());
        stringBuilder.append(") decor (");
        printVariableAndExprList(op.getDecorList());
        stringBuilder.append(")");
        appendSchema(op, showDetails);
        appendAnnotations(op, showDetails);
        appendPhysicalOperatorInfo(op, showDetails);
        return stringBuilder.toString();
    }

    @Override
    public String visitDistinctOperator(DistinctOperator op, Boolean showDetails) {
        stringBuilder.setLength(0);
        stringBuilder.append("distinct (");
        printExprList(op.getExpressions());
        stringBuilder.append(")");
        appendSchema(op, showDetails);
        appendAnnotations(op, showDetails);
        appendPhysicalOperatorInfo(op, showDetails);
        return stringBuilder.toString();
    }

    @Override
    public String visitInnerJoinOperator(InnerJoinOperator op, Boolean showDetails) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("join (").append(op.getCondition().getValue().toString()).append(")");
        appendSchema(op, showDetails);
        appendAnnotations(op, showDetails);
        appendPhysicalOperatorInfo(op, showDetails);
        return stringBuilder.toString();
    }

    @Override
    public String visitLeftOuterJoinOperator(LeftOuterJoinOperator op, Boolean showDetails) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("left outer join (").append(op.getCondition().getValue().toString()).append(")");
        appendSchema(op, showDetails);
        appendAnnotations(op, showDetails);
        appendPhysicalOperatorInfo(op, showDetails);
        return stringBuilder.toString();
    }

    @Override
    public String visitNestedTupleSourceOperator(NestedTupleSourceOperator op, Boolean showDetails)
            throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("nested tuple source");
        appendSchema(op, showDetails);
        appendAnnotations(op, showDetails);
        appendPhysicalOperatorInfo(op, showDetails);
        return stringBuilder.toString();
    }

    @Override
    public String visitOrderOperator(OrderOperator op, Boolean showDetails) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("order ");
        int topK = op.getTopK();
        if (topK != -1) {
            stringBuilder.append("(topK: ").append(topK).append(") ");
        }
        printOrderExprList(op.getOrderExpressions());
        appendSchema(op, showDetails);
        appendAnnotations(op, showDetails);
        appendPhysicalOperatorInfo(op, showDetails);
        return stringBuilder.toString();
    }

    private void appendOrder(OrderOperator.IOrder order) {
        switch (order.getKind()) {
            case ASC:
                stringBuilder.append("ASC");
                break;
            case DESC:
                stringBuilder.append("DESC");
                break;
            default:
                final Mutable<ILogicalExpression> expressionRef = order.getExpressionRef();
                stringBuilder.append(expressionRef == null ? "null" : expressionRef.toString());
        }
    }

    @Override
    public String visitAssignOperator(AssignOperator op, Boolean showDetails) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("assign ").append(str(op.getVariables())).append(" <- ");
        printExprList(op.getExpressions());
        appendSchema(op, showDetails);
        appendAnnotations(op, showDetails);
        appendPhysicalOperatorInfo(op, showDetails);
        return stringBuilder.toString();
    }

    @Override
    public String visitWriteOperator(WriteOperator op, Boolean showDetails) {
        stringBuilder.setLength(0);
        stringBuilder.append("write ");
        printExprList(op.getExpressions());
        appendSchema(op, showDetails);
        appendAnnotations(op, showDetails);
        appendPhysicalOperatorInfo(op, showDetails);
        return stringBuilder.toString();
    }

    @Override
    public String visitDistributeResultOperator(DistributeResultOperator op, Boolean showDetails) {
        stringBuilder.setLength(0);
        stringBuilder.append("distribute result ");
        printExprList(op.getExpressions());
        appendSchema(op, showDetails);
        appendAnnotations(op, showDetails);
        appendPhysicalOperatorInfo(op, showDetails);
        return stringBuilder.toString();
    }

    @Override
    public String visitWriteResultOperator(WriteResultOperator op, Boolean showDetails) {
        stringBuilder.setLength(0);
        stringBuilder.append("load ").append(str(op.getDataSource())).append(" from ")
                .append(op.getPayloadExpression().getValue().toString()).append(" partitioned by ");
        printExprList(op.getKeyExpressions());
        appendSchema(op, showDetails);
        appendAnnotations(op, showDetails);
        appendPhysicalOperatorInfo(op, showDetails);
        return stringBuilder.toString();
    }

    @Override
    public String visitSelectOperator(SelectOperator op, Boolean showDetails) {
        stringBuilder.setLength(0);
        stringBuilder.append("select (").append(op.getCondition().getValue().toString()).append(")");
        appendSchema(op, showDetails);
        appendAnnotations(op, showDetails);
        appendPhysicalOperatorInfo(op, showDetails);
        return stringBuilder.toString();
    }

    @Override
    public String visitProjectOperator(ProjectOperator op, Boolean showDetails) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("project ").append("(").append(op.getVariables()).append(")");
        appendSchema(op, showDetails);
        appendAnnotations(op, showDetails);
        appendPhysicalOperatorInfo(op, showDetails);
        return stringBuilder.toString();
    }

    @Override
    public String visitSubplanOperator(SubplanOperator op, Boolean showDetails) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("subplan {}");
        appendSchema(op, showDetails);
        appendAnnotations(op, showDetails);
        appendPhysicalOperatorInfo(op, showDetails);
        return stringBuilder.toString();
    }

    @Override
    public String visitUnionOperator(UnionAllOperator op, Boolean showDetails) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("union");
        for (Triple<LogicalVariable, LogicalVariable, LogicalVariable> v : op.getVariableMappings()) {
            stringBuilder.append(" (").append(v.first).append(", ").append(v.second).append(", ").append(v.third)
                    .append(")");
        }
        appendSchema(op, showDetails);
        appendAnnotations(op, showDetails);
        appendPhysicalOperatorInfo(op, showDetails);
        return stringBuilder.toString();
    }

    @Override
    public String visitIntersectOperator(IntersectOperator op, Boolean showDetails) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("intersect (");
        pprintVarList(op.getOutputCompareVariables());
        if (op.hasExtraVariables()) {
            stringBuilder.append(" extra ");
            pprintVarList(op.getOutputExtraVariables());
        }
        stringBuilder.append(" <- [");
        for (int i = 0, n = op.getNumInput(); i < n; i++) {
            if (i > 0) {
                stringBuilder.append(", ");
            }
            pprintVarList(op.getInputCompareVariables(i));
            if (op.hasExtraVariables()) {
                stringBuilder.append(" extra ");
                pprintVarList(op.getInputExtraVariables(i));
            }
        }
        stringBuilder.append("])");
        appendSchema(op, showDetails);
        appendAnnotations(op, showDetails);
        appendPhysicalOperatorInfo(op, showDetails);
        return stringBuilder.toString();
    }

    @Override
    public String visitUnnestOperator(UnnestOperator op, Boolean showDetails) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("unnest ").append(op.getVariable());
        if (op.getPositionalVariable() != null) {
            stringBuilder.append(" at ").append(op.getPositionalVariable());
        }
        stringBuilder.append(" <- ").append(op.getExpressionRef().getValue().toString());
        appendSchema(op, showDetails);
        appendAnnotations(op, showDetails);
        appendPhysicalOperatorInfo(op, showDetails);
        return stringBuilder.toString();
    }

    @Override
    public String visitLeftOuterUnnestOperator(LeftOuterUnnestOperator op, Boolean showDetails)
            throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("outer-unnest ").append(op.getVariable());
        if (op.getPositionalVariable() != null) {
            stringBuilder.append(" at ").append(op.getPositionalVariable());
        }
        stringBuilder.append(" <- ").append(op.getExpressionRef().getValue().toString());
        appendSchema(op, showDetails);
        appendAnnotations(op, showDetails);
        appendPhysicalOperatorInfo(op, showDetails);
        return stringBuilder.toString();
    }

    @Override
    public String visitUnnestMapOperator(UnnestMapOperator op, Boolean showDetails) throws AlgebricksException {
        stringBuilder.setLength(0);
        printAbstractUnnestMapOperator(op, "unnest-map", showDetails);
        appendSelectConditionInformation(op.getSelectCondition());
        appendLimitInformation(op.getOutputLimit());
        return stringBuilder.toString();
    }

    @Override
    public String visitLeftOuterUnnestMapOperator(LeftOuterUnnestMapOperator op, Boolean showDetails)
            throws AlgebricksException {
        stringBuilder.setLength(0);
        printAbstractUnnestMapOperator(op, "left-outer-unnest-map", showDetails);
        return stringBuilder.toString();
    }

    private void printAbstractUnnestMapOperator(AbstractUnnestMapOperator op, String opSignature, boolean show) {
        stringBuilder.append(opSignature).append(" ").append(op.getVariables()).append(" <- ")
                .append(op.getExpressionRef().getValue().toString());
        appendFilterInformation(op.getMinFilterVars(), op.getMaxFilterVars());
        appendSchema(op, show);
        appendAnnotations(op, show);
        appendPhysicalOperatorInfo(op, show);
    }

    @Override
    public String visitDataScanOperator(DataSourceScanOperator op, Boolean showDetails) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("data-scan ").append(op.getProjectVariables()).append("<-").append(op.getVariables())
                .append(" <- ").append(op.getDataSource());
        appendFilterInformation(op.getMinFilterVars(), op.getMaxFilterVars());
        appendSelectConditionInformation(op.getSelectCondition());
        appendLimitInformation(op.getOutputLimit());
        appendSchema(op, showDetails);
        appendAnnotations(op, showDetails);
        appendPhysicalOperatorInfo(op, showDetails);
        return stringBuilder.toString();
    }

    private void appendFilterInformation(List<LogicalVariable> minFilterVars, List<LogicalVariable> maxFilterVars) {
        if (minFilterVars != null || maxFilterVars != null) {
            stringBuilder.append(" with filter on");
        }
        if (minFilterVars != null) {
            stringBuilder.append(" min:").append(minFilterVars);
        }
        if (maxFilterVars != null) {
            stringBuilder.append(" max:").append(maxFilterVars);
        }
    }

    private void appendSelectConditionInformation(Mutable<ILogicalExpression> condition) throws AlgebricksException {
        if (condition != null) {
            stringBuilder.append(" condition:").append(condition.getValue().toString());
        }
    }

    private void appendLimitInformation(long outputLimit) throws AlgebricksException {
        if (outputLimit >= 0) {
            stringBuilder.append(" limit:").append(String.valueOf(outputLimit));
        }
    }

    @Override
    public String visitLimitOperator(LimitOperator op, Boolean showDetails) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("limit ").append(op.getMaxObjects().getValue().toString());
        ILogicalExpression offset = op.getOffset().getValue();
        if (offset != null) {
            stringBuilder.append(", ").append(offset.toString());
        }
        appendSchema(op, showDetails);
        appendAnnotations(op, showDetails);
        appendPhysicalOperatorInfo(op, showDetails);
        return stringBuilder.toString();
    }

    @Override
    public String visitExchangeOperator(ExchangeOperator op, Boolean showDetails) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("exchange");
        appendSchema(op, showDetails);
        appendAnnotations(op, showDetails);
        appendPhysicalOperatorInfo(op, showDetails);
        return stringBuilder.toString();
    }

    @Override
    public String visitScriptOperator(ScriptOperator op, Boolean showDetails) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("script (in: ").append(op.getInputVariables()).append(") (out: ")
                .append(op.getOutputVariables()).append(")");
        appendSchema(op, showDetails);
        appendAnnotations(op, showDetails);
        appendPhysicalOperatorInfo(op, showDetails);
        return stringBuilder.toString();
    }

    @Override
    public String visitReplicateOperator(ReplicateOperator op, Boolean showDetails) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("replicate");
        appendSchema(op, showDetails);
        appendAnnotations(op, showDetails);
        appendPhysicalOperatorInfo(op, showDetails);
        return stringBuilder.toString();
    }

    @Override
    public String visitSplitOperator(SplitOperator op, Boolean showDetails) throws AlgebricksException {
        stringBuilder.setLength(0);
        Mutable<ILogicalExpression> branchingExpression = op.getBranchingExpression();
        stringBuilder.append("split ").append(branchingExpression.getValue().toString());
        appendSchema(op, showDetails);
        appendAnnotations(op, showDetails);
        appendPhysicalOperatorInfo(op, showDetails);
        return stringBuilder.toString();
    }

    @Override
    public String visitMaterializeOperator(MaterializeOperator op, Boolean showDetails) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("materialize");
        appendSchema(op, showDetails);
        appendAnnotations(op, showDetails);
        appendPhysicalOperatorInfo(op, showDetails);
        return stringBuilder.toString();
    }

    @Override
    public String visitInsertDeleteUpsertOperator(InsertDeleteUpsertOperator op, Boolean showDetails) {
        stringBuilder.setLength(0);
        String header = getIndexOpString(op.getOperation());
        stringBuilder.append(header).append(str(op.getDataSource())).append(" from record: ")
                .append(op.getPayloadExpression().getValue().toString());
        if (op.getAdditionalNonFilteringExpressions() != null) {
            stringBuilder.append(", meta: ");
            printExprList(op.getAdditionalNonFilteringExpressions());
        }
        stringBuilder.append(" partitioned by ");
        printExprList(op.getPrimaryKeyExpressions());
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
        appendSchema(op, showDetails);
        appendAnnotations(op, showDetails);
        appendPhysicalOperatorInfo(op, showDetails);
        return stringBuilder.toString();
    }

    @Override
    public String visitIndexInsertDeleteUpsertOperator(IndexInsertDeleteUpsertOperator op, Boolean showDetails) {
        stringBuilder.setLength(0);
        String header = getIndexOpString(op.getOperation());
        stringBuilder.append(header).append(op.getIndexName()).append(" on ")
                .append(str(op.getDataSourceIndex().getDataSource())).append(" from ");
        if (op.getOperation() == Kind.UPSERT) {
            stringBuilder.append(" replace:");
            printExprList(op.getPrevSecondaryKeyExprs());
            stringBuilder.append(" with:");
            printExprList(op.getSecondaryKeyExpressions());
        } else {
            printExprList(op.getSecondaryKeyExpressions());
        }
        if (op.isBulkload()) {
            stringBuilder.append(" [bulkload]");
        }
        appendSchema(op, showDetails);
        appendAnnotations(op, showDetails);
        appendPhysicalOperatorInfo(op, showDetails);
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
        return "";
    }

    @Override
    public String visitTokenizeOperator(TokenizeOperator op, Boolean showDetails) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("tokenize ").append(str(op.getTokenizeVars())).append(" <- ");
        printExprList(op.getSecondaryKeyExpressions());
        appendSchema(op, showDetails);
        appendAnnotations(op, showDetails);
        appendPhysicalOperatorInfo(op, showDetails);
        return stringBuilder.toString();
    }

    @Override
    public String visitSinkOperator(SinkOperator op, Boolean showDetails) {
        stringBuilder.setLength(0);
        stringBuilder.append("sink");
        appendSchema(op, showDetails);
        appendAnnotations(op, showDetails);
        appendPhysicalOperatorInfo(op, showDetails);
        return stringBuilder.toString();
    }

    @Override
    public String visitDelegateOperator(DelegateOperator op, Boolean showDetails) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append(op.toString());
        appendSchema(op, showDetails);
        appendAnnotations(op, showDetails);
        appendPhysicalOperatorInfo(op, showDetails);
        return stringBuilder.toString();
    }

    @Override
    public String visitForwardOperator(ForwardOperator op, Boolean showDetails) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("forward(").append(op.getSideDataExpression().getValue().toString()).append(")");
        appendSchema(op, showDetails);
        appendAnnotations(op, showDetails);
        appendPhysicalOperatorInfo(op, showDetails);
        return stringBuilder.toString();
    }

    @Override
    public String visitWindowOperator(WindowOperator op, Boolean showDetails) throws AlgebricksException {
        stringBuilder.setLength(0);
        stringBuilder.append("window (").append(str(op.getVariables())).append(" <- ");
        printExprList(op.getExpressions());
        stringBuilder.append(") partition by (");
        printExprList(op.getPartitionExpressions());
        stringBuilder.append(") order by (");
        printOrderExprList(op.getOrderExpressions());
        if (op.hasNestedPlans()) {
            stringBuilder.append(") frame on (");
            printOrderExprList(op.getFrameValueExpressions());
            List<Mutable<ILogicalExpression>> frameStartExpressions = op.getFrameStartExpressions();
            if (!frameStartExpressions.isEmpty()) {
                stringBuilder.append(") frame start (");
                printExprList(frameStartExpressions);
            }
            List<Mutable<ILogicalExpression>> frameStartValidationExpressions = op.getFrameStartValidationExpressions();
            if (!frameStartValidationExpressions.isEmpty()) {
                stringBuilder.append(") if (");
                printExprList(frameStartValidationExpressions);
            }
            List<Mutable<ILogicalExpression>> frameEndExpressions = op.getFrameEndExpressions();
            if (!frameEndExpressions.isEmpty()) {
                stringBuilder.append(") frame end (");
                printExprList(frameEndExpressions);
            }
            List<Mutable<ILogicalExpression>> frameEndValidationExpressions = op.getFrameEndValidationExpressions();
            if (!frameEndValidationExpressions.isEmpty()) {
                stringBuilder.append(") if (");
                printExprList(frameEndValidationExpressions);
            }
            List<Mutable<ILogicalExpression>> frameExcludeExpressions = op.getFrameExcludeExpressions();
            if (!frameExcludeExpressions.isEmpty()) {
                stringBuilder.append(") frame exclude (");
                stringBuilder.append(" (negation start: ").append(op.getFrameExcludeNegationStartIdx()).append(") ");
                printExprList(frameExcludeExpressions);
            }
            Mutable<ILogicalExpression> frameExcludeUnaryExpression = op.getFrameExcludeUnaryExpression();
            if (frameExcludeUnaryExpression.getValue() != null) {
                stringBuilder.append(") frame exclude unary (");
                stringBuilder.append(frameExcludeUnaryExpression.getValue());
                stringBuilder.append(") ");
            }
            Mutable<ILogicalExpression> frameOffsetExpression = op.getFrameOffsetExpression();
            if (frameOffsetExpression.getValue() != null) {
                stringBuilder.append(") frame offset (");
                stringBuilder.append(frameOffsetExpression.getValue());
                stringBuilder.append(") ");
            }
            int frameMaxObjects = op.getFrameMaxObjects();
            if (frameMaxObjects != -1) {
                stringBuilder.append("(frame maxObjects: ").append(frameMaxObjects).append(") ");
            }
        }
        stringBuilder.append(")");
        appendSchema(op, showDetails);
        appendAnnotations(op, showDetails);
        appendPhysicalOperatorInfo(op, showDetails);
        return stringBuilder.toString();
    }

    protected void pprintVarList(List<LogicalVariable> variables) {
        stringBuilder.append("[");
        variables.forEach(var -> stringBuilder.append(str(var)).append(", "));
        stringBuilder.append("]");
    }

    private void printExprList(List<Mutable<ILogicalExpression>> expressions) {
        stringBuilder.append("[");
        expressions.forEach(exprRef -> stringBuilder.append(exprRef.getValue().toString()).append(", "));
        stringBuilder.append("]");
    }

    private void printVariableAndExprList(List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> variableExprList) {
        stringBuilder.append("[");
        boolean first = true;
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> variableExpressionPair : variableExprList) {
            if (first) {
                first = false;
            } else {
                stringBuilder.append("; ");
            }
            if (variableExpressionPair.first != null) {
                stringBuilder.append(variableExpressionPair.first).append(" := ").append(variableExpressionPair.second);
            } else {
                stringBuilder.append(variableExpressionPair.second.getValue().toString());
            }
        }
        stringBuilder.append("]");
    }

    private void printOrderExprList(List<Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>>> orderExprList) {
        for (Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>> p : orderExprList) {
            stringBuilder.append("(");
            appendOrder(p.first);
            stringBuilder.append(", ").append(p.second.getValue().toString()).append(") ");
        }
    }

    private void appendSchema(AbstractLogicalOperator op, boolean show) {
        if (show) {
            stringBuilder.append("\\nSchema: ");
            final List<LogicalVariable> schema = op.getSchema();
            stringBuilder.append(schema == null ? "null" : schema);
        }
    }

    private void appendAnnotations(AbstractLogicalOperator op, boolean show) {
        if (show) {
            final Map<String, Object> annotations = op.getAnnotations();
            if (!annotations.isEmpty()) {
                stringBuilder.append("\\nAnnotations: ").append(annotations);
            }
        }
    }

    private void appendPhysicalOperatorInfo(AbstractLogicalOperator op, boolean show) {
        IPhysicalOperator physicalOp = op.getPhysicalOperator();
        stringBuilder.append("\\n").append(physicalOp == null ? "null" : physicalOp.toString().trim());
        stringBuilder.append(", Exec: ").append(op.getExecutionMode());
        if (show) {
            IPhysicalPropertiesVector properties = physicalOp == null ? null : physicalOp.getDeliveredProperties();
            List<ILocalStructuralProperty> localProp = properties == null ? null : properties.getLocalProperties();
            IPartitioningProperty partitioningProp = properties == null ? null : properties.getPartitioningProperty();
            if (localProp != null) {
                stringBuilder.append("\\nProperties in each partition: [");
                for (ILocalStructuralProperty property : localProp) {
                    if (property == null) {
                        stringBuilder.append("null, ");
                    } else if (property.getPropertyType() == LOCAL_ORDER_PROPERTY) {
                        stringBuilder.append("ordered by ");
                    } else if (property.getPropertyType() == LOCAL_GROUPING_PROPERTY) {
                        stringBuilder.append("group by ");
                    }
                    stringBuilder.append(property).append(", ");
                }
                stringBuilder.append("]");
            }

            if (partitioningProp != null) {
                stringBuilder.append("\\n").append(partitioningProp.getPartitioningType()).append(":");
                INodeDomain nodeDomain = partitioningProp.getNodeDomain();
                stringBuilder.append("\\n ");
                if (nodeDomain != null && nodeDomain.cardinality() != null) {
                    stringBuilder.append(nodeDomain.cardinality()).append(" partitions. ");
                }
                switch (partitioningProp.getPartitioningType()) {
                    case BROADCAST:
                        stringBuilder.append("Data is broadcast to partitions.");
                        break;
                    case RANDOM:
                        stringBuilder.append("Data is randomly partitioned.");
                        break;
                    case ORDERED_PARTITIONED:
                        stringBuilder.append("Data is orderly partitioned via a range.");
                        break;
                    case UNORDERED_PARTITIONED:
                        stringBuilder.append("Data is hash partitioned.");
                        break;
                    case UNPARTITIONED:
                        stringBuilder.append("Data is in one place.");
                }
                if (nodeDomain instanceof DefaultNodeGroupDomain) {
                    DefaultNodeGroupDomain nd = (DefaultNodeGroupDomain) nodeDomain;
                    stringBuilder.append("\\n").append(nd);
                }
            }
        }

    }
}
