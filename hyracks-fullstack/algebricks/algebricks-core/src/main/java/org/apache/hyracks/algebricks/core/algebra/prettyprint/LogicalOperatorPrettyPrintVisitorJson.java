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

import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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

public class LogicalOperatorPrettyPrintVisitorJson extends AbstractLogicalOperatorPrettyPrintVisitor {
    Map<AbstractLogicalOperator, String> operatorIdentity = new HashMap<>();

    public LogicalOperatorPrettyPrintVisitorJson() {
    }

    public LogicalOperatorPrettyPrintVisitorJson(Appendable app) {
        super(app);
    }

    IdCounter idCounter = new IdCounter();

    public class IdCounter {
        private int id;
        private final Deque<Integer> prefix;

        public IdCounter() {
            prefix = new LinkedList<Integer>();
            prefix.add(1);
            this.id = 0;
        }

        public void previousPrefix() {
            this.id = prefix.removeLast();
        }

        public void nextPrefix() {
            prefix.add(this.id);
            this.id = 0;
        }

        public String printOperatorId(AbstractLogicalOperator op) {
            String stringPrefix = "";
            Object[] values = this.prefix.toArray();
            for (Object val : values) {
                stringPrefix = stringPrefix.isEmpty() ? val.toString() : stringPrefix + "." + val.toString();
            }
            if (!operatorIdentity.containsKey(op)) {
                String opId = stringPrefix.isEmpty() ? "" + Integer.toString(++id)
                        : stringPrefix + "." + Integer.toString(++id);
                operatorIdentity.put(op, opId);
            }
            return operatorIdentity.get(op);
        }
    }

    @Override
    public AlgebricksAppendable reset(AlgebricksAppendable buffer) {
        operatorIdentity.clear();
        return super.reset(buffer);
    }

    @Override
    public void printOperator(AbstractLogicalOperator op, int indent) throws AlgebricksException {
        int currentIndent = indent;
        final AlgebricksAppendable out = get();
        pad(out, currentIndent);
        appendln(out, "{");
        currentIndent++;
        op.accept(this, currentIndent);
        appendln(out, ",");
        pad(out, currentIndent);
        append(out, "\"operatorId\": \"" + idCounter.printOperatorId(op) + "\"");
        IPhysicalOperator pOp = op.getPhysicalOperator();
        if (pOp != null) {
            appendln(out, ",");
            pad(out, currentIndent);
            String pOperator = "\"physical-operator\": \"" + pOp.toString() + "\"";
            append(out, pOperator);
        }
        appendln(out, ",");
        pad(out, currentIndent);
        append(out, "\"execution-mode\": \"" + op.getExecutionMode() + '"');
        if (!op.getInputs().isEmpty()) {
            appendln(out, ",");
            pad(out, currentIndent);
            appendln(out, "\"inputs\": [");
            boolean moreInputes = false;
            for (Mutable<ILogicalOperator> k : op.getInputs()) {
                if (moreInputes) {
                    append(out, ",");
                }
                printOperator((AbstractLogicalOperator) k.getValue(), currentIndent + 4);
                moreInputes = true;
            }
            pad(out, currentIndent + 2);
            appendln(out, "]");
        } else {
            out.append("\n");
        }
        pad(out, currentIndent - 1);
        appendln(out, "}");
    }

    public void variablePrintHelper(List<LogicalVariable> variables, Integer indent) throws AlgebricksException {
        if (!variables.isEmpty()) {
            addIndent(0).append(",\n");
            addIndent(indent).append("\"variables\": [");
            appendVars(variables);
            buffer.append("]");
        }
    }

    @Override
    public Void visitAggregateOperator(AggregateOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\": \"aggregate\"");
        if (!op.getExpressions().isEmpty()) {
            addIndent(0).append(",\n");
            pprintExprList(op.getExpressions(), indent);
        }
        variablePrintHelper(op.getVariables(), indent);
        return null;
    }

    @Override
    public Void visitRunningAggregateOperator(RunningAggregateOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\": \"running-aggregate\"");
        variablePrintHelper(op.getVariables(), indent);
        if (!op.getExpressions().isEmpty()) {
            addIndent(0).append(",\n");
            pprintExprList(op.getExpressions(), indent);
        }
        return null;
    }

    @Override
    public Void visitEmptyTupleSourceOperator(EmptyTupleSourceOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\": \"empty-tuple-source\"");
        return null;
    }

    @Override
    public Void visitGroupByOperator(GroupByOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\": \"group-by\"");

        if (op.isGroupAll()) {
            buffer.append(",\n");
            addIndent(indent).append("\"option\": \"all\"");
        }
        if (!op.getGroupByList().isEmpty()) {
            buffer.append(",\n");
            addIndent(indent).append("\"group-by-list\": ");
            pprintVeList(op.getGroupByList(), indent);
        }
        if (!op.getDecorList().isEmpty()) {
            buffer.append(",\n");
            addIndent(indent).append("\"decor-list\": ");
            pprintVeList(op.getDecorList(), indent);
        }
        if (!op.getNestedPlans().isEmpty()) {
            buffer.append(",\n");
            addIndent(indent).append("\"subplan\": ");
            printNestedPlans(op, indent);
        }
        return null;
    }

    @Override
    public Void visitDistinctOperator(DistinctOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\": \"distinct\"");
        if (!op.getExpressions().isEmpty()) {
            addIndent(0).append(",\n");
            pprintExprList(op.getExpressions(), indent);
        }
        return null;
    }

    @Override
    public Void visitInnerJoinOperator(InnerJoinOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\": \"join\",\n");
        addIndent(indent).append("\"condition\": \"" + op.getCondition().getValue().accept(exprVisitor, indent) + "\"");
        return null;
    }

    @Override
    public Void visitLeftOuterJoinOperator(LeftOuterJoinOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\": \"left-outer-join\",\n");
        addIndent(indent).append("\"condition\": \"" + op.getCondition().getValue().accept(exprVisitor, indent) + "\"");
        return null;
    }

    @Override
    public Void visitNestedTupleSourceOperator(NestedTupleSourceOperator op, Integer indent)
            throws AlgebricksException {
        addIndent(indent).append("\"operator\": \"nested-tuple-source\"");
        return null;
    }

    @Override
    public Void visitOrderOperator(OrderOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\": \"order\"");
        buffer.append(",\n");
        int topK = op.getTopK();
        if (topK != -1) {
            addIndent(indent).append("\"topK\": \"" + topK + "\",\n");
        }
        addIndent(indent).append("\"order-by-list\": ");
        pprintOrderExprList(op.getOrderExpressions(), indent);
        return null;
    }

    @Override
    public Void visitAssignOperator(AssignOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\": \"assign\"");
        variablePrintHelper(op.getVariables(), indent);
        if (!op.getExpressions().isEmpty()) {
            addIndent(0).append(",\n");
            pprintExprList(op.getExpressions(), indent);
        }
        return null;
    }

    @Override
    public Void visitWriteOperator(WriteOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\": \"write\"");
        if (!op.getExpressions().isEmpty()) {
            addIndent(0).append(",\n");
            pprintExprList(op.getExpressions(), indent);
        }
        return null;
    }

    @Override
    public Void visitDistributeResultOperator(DistributeResultOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\": \"distribute-result\"");
        if (!op.getExpressions().isEmpty()) {
            addIndent(0).append(",\n");
            pprintExprList(op.getExpressions(), indent);
        }
        return null;
    }

    @Override
    public Void visitWriteResultOperator(WriteResultOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\": \"load\",\n");
        addIndent(indent).append(str(op.getDataSource())).append("\"from\":")
                .append(op.getPayloadExpression().getValue().accept(exprVisitor, indent) + ",\n");
        addIndent(indent).append("\"partitioned-by\": {");
        pprintExprList(op.getKeyExpressions(), indent);
        addIndent(indent).append("}");
        return null;
    }

    @Override
    public Void visitSelectOperator(SelectOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\": \"select\",\n");
        addIndent(indent).append("\"expressions\": \""
                + op.getCondition().getValue().accept(exprVisitor, indent).replace('"', ' ') + "\"");
        return null;
    }

    @Override
    public Void visitProjectOperator(ProjectOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\": \"project\"");
        variablePrintHelper(op.getVariables(), indent);
        return null;
    }

    @Override
    public Void visitSubplanOperator(SubplanOperator op, Integer indent) throws AlgebricksException {
        if (!op.getNestedPlans().isEmpty()) {
            addIndent(indent).append("\"subplan\": ");
            printNestedPlans(op, indent);
        }
        return null;
    }

    @Override
    public Void visitUnionOperator(UnionAllOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\": \"union\"");
        for (Triple<LogicalVariable, LogicalVariable, LogicalVariable> v : op.getVariableMappings()) {
            buffer.append(",\n");
            addIndent(indent).append(
                    "\"values\": [" + "\"" + v.first + "\"," + "\"" + v.second + "\"," + "\"" + v.third + "\"]");
        }
        return null;
    }

    @Override
    public Void visitIntersectOperator(IntersectOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\": \"intersect\",\n");

        addIndent(indent).append("\"output-compare-variables\": [");
        appendVars(op.getOutputCompareVariables());
        buffer.append(']');
        if (op.hasExtraVariables()) {
            buffer.append(",\n");
            addIndent(indent).append("\"output-extra-variables\": [");
            appendVars(op.getOutputExtraVariables());
            buffer.append(']');
        }
        buffer.append(",\n");
        addIndent(indent).append("\"input-compare-variables\": [");
        for (int i = 0, n = op.getNumInput(); i < n; i++) {
            if (i > 0) {
                buffer.append(", ");
            }
            buffer.append('[');
            appendVars(op.getInputCompareVariables(i));
            buffer.append(']');
        }
        buffer.append(']');
        if (op.hasExtraVariables()) {
            buffer.append(",\n");
            addIndent(indent).append("\"input-extra-variables\": [");
            for (int i = 0, n = op.getNumInput(); i < n; i++) {
                if (i > 0) {
                    buffer.append(", ");
                }
                buffer.append('[');
                appendVars(op.getInputExtraVariables(i));
                buffer.append(']');
            }
            buffer.append(']');
        }
        return null;
    }

    @Override
    public Void visitUnnestOperator(UnnestOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\": \"unnest\"");
        variablePrintHelper(op.getVariables(), indent);
        if (op.getPositionalVariable() != null) {
            buffer.append(",\n");
            addIndent(indent).append("\"position\": \"" + op.getPositionalVariable() + "\"");
        }
        buffer.append(",\n");
        addIndent(indent).append("\"expressions\": \""
                + op.getExpressionRef().getValue().accept(exprVisitor, indent).replace('"', ' ') + "\"");
        return null;
    }

    @Override
    public Void visitLeftOuterUnnestOperator(LeftOuterUnnestOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\": \"outer-unnest\",\n");
        addIndent(indent).append("\"variables\": [\"" + op.getVariable() + "\"]");
        if (op.getPositionalVariable() != null) {
            buffer.append(",\n");
            addIndent(indent).append("\"position\": " + op.getPositionalVariable());
        }
        buffer.append(",\n");
        addIndent(indent).append("\"expressions\": \""
                + op.getExpressionRef().getValue().accept(exprVisitor, indent).replace('"', ' ') + "\"");
        return null;
    }

    @Override
    public Void visitUnnestMapOperator(UnnestMapOperator op, Integer indent) throws AlgebricksException {
        AlgebricksAppendable plan = printAbstractUnnestMapOperator(op, indent, "unnest-map");
        appendSelectConditionInformation(plan, op.getSelectCondition(), indent);
        appendLimitInformation(plan, op.getOutputLimit(), indent);
        return null;
    }

    @Override
    public Void visitLeftOuterUnnestMapOperator(LeftOuterUnnestMapOperator op, Integer indent)
            throws AlgebricksException {
        printAbstractUnnestMapOperator(op, indent, "left-outer-unnest-map");
        return null;
    }

    private AlgebricksAppendable printAbstractUnnestMapOperator(AbstractUnnestMapOperator op, Integer indent,
            String opSignature) throws AlgebricksException {
        AlgebricksAppendable plan = addIndent(indent).append("\"operator\": \"" + opSignature + "\"");
        variablePrintHelper(op.getVariables(), indent);
        buffer.append(",\n");
        addIndent(indent).append("\"expressions\": \""
                + op.getExpressionRef().getValue().accept(exprVisitor, indent).replace('"', ' ') + "\"");
        appendFilterInformation(plan, op.getMinFilterVars(), op.getMaxFilterVars(), indent);
        return plan;
    }

    @Override
    public Void visitDataScanOperator(DataSourceScanOperator op, Integer indent) throws AlgebricksException {
        AlgebricksAppendable plan = addIndent(indent).append("\"operator\": \"data-scan\"");
        if (!op.getProjectVariables().isEmpty()) {
            addIndent(0).append(",\n");
            addIndent(indent).append("\"project-variables\": [");
            appendVars(op.getProjectVariables());
            buffer.append("]");
        }
        variablePrintHelper(op.getVariables(), indent);
        if (op.getDataSource() != null) {
            addIndent(0).append(",\n");
            addIndent(indent).append("\"data-source\": \"" + op.getDataSource() + "\"");
        }
        appendFilterInformation(plan, op.getMinFilterVars(), op.getMaxFilterVars(), indent);
        appendSelectConditionInformation(plan, op.getSelectCondition(), indent);
        appendLimitInformation(plan, op.getOutputLimit(), indent);
        return null;
    }

    private Void appendFilterInformation(AlgebricksAppendable plan, List<LogicalVariable> minFilterVars,
            List<LogicalVariable> maxFilterVars, Integer indent) throws AlgebricksException {
        if (minFilterVars != null || maxFilterVars != null) {
            plan.append(",\n");
            addIndent(indent);
            plan.append("\"with-filter-on\": {");
        }
        if (minFilterVars != null) {
            buffer.append("\n");
            addIndent(indent).append("\"min\": [");
            appendVars(minFilterVars);
            buffer.append("]");
        }
        if (minFilterVars != null && maxFilterVars != null) {
            buffer.append(",");
        }
        if (maxFilterVars != null) {
            buffer.append("\n");
            addIndent(indent).append("\"max\": [");
            appendVars(maxFilterVars);
            buffer.append("]");
        }
        if (minFilterVars != null || maxFilterVars != null) {
            plan.append("\n");
            addIndent(indent).append("}");
        }
        return null;
    }

    private Void appendSelectConditionInformation(AlgebricksAppendable plan, Mutable<ILogicalExpression> condition,
            Integer indent) throws AlgebricksException {
        if (condition != null) {
            plan.append(",\n");
            addIndent(indent).append(
                    "\"condition\": \"" + condition.getValue().accept(exprVisitor, indent).replace('"', ' ') + "\"");
        }
        return null;
    }

    private Void appendLimitInformation(AlgebricksAppendable plan, long outputLimit, Integer indent)
            throws AlgebricksException {
        if (outputLimit >= 0) {
            plan.append(",\n");
            addIndent(indent).append("\"limit\": \"" + outputLimit + "\"");
        }
        return null;
    }

    private void appendVars(List<LogicalVariable> minFilterVars) throws AlgebricksException {
        boolean first = true;
        for (LogicalVariable v : minFilterVars) {
            if (!first) {
                buffer.append(",");
            }
            buffer.append("\"" + str(v) + "\"");
            first = false;
        }
    }

    @Override
    public Void visitLimitOperator(LimitOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\": \"limit\",\n");
        addIndent(indent).append("\"value\": \"" + op.getMaxObjects().getValue().accept(exprVisitor, indent) + "\"");
        ILogicalExpression offset = op.getOffset().getValue();
        if (offset != null) {
            buffer.append(",\n");
            addIndent(indent).append("\"offset\": \"" + offset.accept(exprVisitor, indent) + "\"");
        }
        return null;
    }

    @Override
    public Void visitExchangeOperator(ExchangeOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\": \"exchange\"");
        return null;
    }

    @Override
    public Void visitScriptOperator(ScriptOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\": \"script\"");
        if (!op.getInputVariables().isEmpty()) {
            addIndent(0).append(",\n");
            addIndent(indent).append("\"in\": [");
            appendVars(op.getInputVariables());
            buffer.append("]");
        }
        if (!op.getOutputVariables().isEmpty()) {
            addIndent(0).append(",\n");
            addIndent(indent).append("\"out\": [");
            appendVars(op.getOutputVariables());
            buffer.append("]");
        }
        return null;
    }

    @Override
    public Void visitReplicateOperator(ReplicateOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\": \"replicate\"");
        return null;
    }

    @Override
    public Void visitSplitOperator(SplitOperator op, Integer indent) throws AlgebricksException {
        Mutable<ILogicalExpression> branchingExpression = op.getBranchingExpression();
        addIndent(indent).append("\"operator\": \"split\",\n");
        addIndent(indent).append("\"expressions\": \""
                + branchingExpression.getValue().accept(exprVisitor, indent).replace('"', ' ') + "\"");
        return null;
    }

    @Override
    public Void visitMaterializeOperator(MaterializeOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\": \"materialize\"");
        return null;
    }

    @Override
    public Void visitInsertDeleteUpsertOperator(InsertDeleteUpsertOperator op, Integer indent)
            throws AlgebricksException {
        String header = "\"operator\": \"" + getIndexOpString(op.getOperation()) + "\",\n";
        addIndent(indent).append(header);
        addIndent(indent).append(str("\"data-source\": \"" + op.getDataSource() + "\",\n"));
        addIndent(indent).append("\"from-record\": \"")
                .append(op.getPayloadExpression().getValue().accept(exprVisitor, indent) + "\"");
        if (op.getAdditionalNonFilteringExpressions() != null) {
            buffer.append(",\n\"meta\": {");
            pprintExprList(op.getAdditionalNonFilteringExpressions(), 0);
            buffer.append("}");
        }
        buffer.append(",\n");
        addIndent(indent).append("\"partitioned-by\": {");
        pprintExprList(op.getPrimaryKeyExpressions(), 0);
        buffer.append("}");
        if (op.getOperation() == Kind.UPSERT) {
            addIndent(indent).append(",\n\"out\": {\n");
            addIndent(indent).append("\"record-before-upsert\": \"" + op.getBeforeOpRecordVar() + "\"");
            if (op.getBeforeOpAdditionalNonFilteringVars() != null) {
                buffer.append(",\n");
                addIndent(indent)
                        .append("\"additional-before-upsert\": \"" + op.getBeforeOpAdditionalNonFilteringVars() + "\"");
            }
            addIndent(indent).append("}");
        }
        if (op.isBulkload()) {
            buffer.append(",\n");
            addIndent(indent).append("\"bulkload\": true");
        }
        return null;
    }

    @Override
    public Void visitIndexInsertDeleteUpsertOperator(IndexInsertDeleteUpsertOperator op, Integer indent)
            throws AlgebricksException {
        String header = getIndexOpString(op.getOperation());
        addIndent(indent).append("\"operator\": \"" + header + "\",\n");
        addIndent(indent).append("\"index\": \"" + op.getIndexName() + "\",\n");
        addIndent(indent).append("\"on\": \"").append(str(op.getDataSourceIndex().getDataSource()) + "\",\n");
        addIndent(indent).append("\"from\": {");

        if (op.getOperation() == Kind.UPSERT) {

            addIndent(indent).append("[\"replace\": \"");
            pprintExprList(op.getPrevSecondaryKeyExprs(), 0);
            buffer.append("\",\n");
            addIndent(indent).append("\"with\": \"");
            pprintExprList(op.getSecondaryKeyExpressions(), 0);
            buffer.append("\"}");
        } else {
            pprintExprList(op.getSecondaryKeyExpressions(), 0);
        }
        buffer.append("\n");
        addIndent(indent).append("}");
        if (op.isBulkload()) {
            buffer.append(",\n");
            buffer.append("\"bulkload\": true");
        }
        return null;
    }

    public String getIndexOpString(Kind opKind) {
        switch (opKind) {
            case DELETE:
                return "delete-from";
            case INSERT:
                return "insert-into";
            case UPSERT:
                return "upsert-into";
        }
        return null;
    }

    @Override
    public Void visitTokenizeOperator(TokenizeOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\": \"tokenize\"");
        variablePrintHelper(op.getTokenizeVars(), indent);
        if (!op.getSecondaryKeyExpressions().isEmpty()) {
            addIndent(0).append(",\n");
            pprintExprList(op.getSecondaryKeyExpressions(), indent);
        }
        return null;
    }

    @Override
    public Void visitForwardOperator(ForwardOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\": \"forward\"");
        addIndent(0).append(",\n");
        addIndent(indent).append("\"expressions\": \""
                + op.getSideDataExpression().getValue().accept(exprVisitor, indent).replace('"', ' ') + "\"");
        return null;
    }

    @Override
    public Void visitSinkOperator(SinkOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\": \"sink\"");
        return null;
    }

    @Override
    public Void visitDelegateOperator(DelegateOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\": \"" + op.toString() + "\"");
        return null;
    }

    @Override
    public Void visitWindowOperator(WindowOperator op, Integer indent) throws AlgebricksException {
        Integer fldIndent = indent + 2;
        addIndent(indent).append("\"operator\": \"window-aggregate\"");
        variablePrintHelper(op.getVariables(), indent);
        List<Mutable<ILogicalExpression>> expressions = op.getExpressions();
        if (!expressions.isEmpty()) {
            buffer.append(",\n");
            pprintExprList(expressions, indent);
        }
        List<Mutable<ILogicalExpression>> partitionExpressions = op.getPartitionExpressions();
        if (!partitionExpressions.isEmpty()) {
            buffer.append(",\n");
            addIndent(indent).append("\"partition-by\": {\n");
            pprintExprList(partitionExpressions, fldIndent);
            buffer.append("\n");
            addIndent(indent).append("}");
        }
        List<Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>>> orderExpressions = op.getOrderExpressions();
        if (!orderExpressions.isEmpty()) {
            buffer.append(",\n");
            addIndent(indent).append("\"order-by\": ");
            pprintOrderExprList(orderExpressions, fldIndent);
        }
        if (op.hasNestedPlans()) {
            List<Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>>> frameValueExpressions =
                    op.getFrameValueExpressions();
            if (!frameValueExpressions.isEmpty()) {
                buffer.append(",\n");
                addIndent(indent).append("\"frame-on\": ");
                pprintOrderExprList(frameValueExpressions, fldIndent);
            }
            List<Mutable<ILogicalExpression>> frameStartExpressions = op.getFrameStartExpressions();
            if (!frameStartExpressions.isEmpty()) {
                buffer.append(",\n");
                addIndent(indent).append("\"frame-start\": {\n");
                pprintExprList(frameStartExpressions, fldIndent);
                buffer.append("\n");
                addIndent(indent).append("}");
            }
            List<Mutable<ILogicalExpression>> frameStartValidationExpressions = op.getFrameStartValidationExpressions();
            if (!frameStartValidationExpressions.isEmpty()) {
                buffer.append(",\n");
                addIndent(indent).append("\"frame-start-if\": {\n");
                pprintExprList(frameStartValidationExpressions, fldIndent);
                buffer.append("\n");
                addIndent(indent).append("}");
            }
            List<Mutable<ILogicalExpression>> frameEndExpressions = op.getFrameEndExpressions();
            if (!frameEndExpressions.isEmpty()) {
                buffer.append(",\n");
                addIndent(indent).append("\"frame-end\": {\n");
                pprintExprList(frameEndExpressions, fldIndent);
                buffer.append("\n");
                addIndent(indent).append("}");
            }
            List<Mutable<ILogicalExpression>> frameEndValidationExpressions = op.getFrameEndValidationExpressions();
            if (!frameEndValidationExpressions.isEmpty()) {
                buffer.append(",\n");
                addIndent(indent).append("\"frame-end-if\": {\n");
                pprintExprList(frameEndValidationExpressions, fldIndent);
                buffer.append("\n");
                addIndent(indent).append("}");
            }
            List<Mutable<ILogicalExpression>> frameExcludeExpressions = op.getFrameExcludeExpressions();
            if (!frameExcludeExpressions.isEmpty()) {
                buffer.append(",\n");
                addIndent(indent).append("\"frame-exclude\": {\n");
                pprintExprList(frameExcludeExpressions, fldIndent);
                buffer.append("\n");
                addIndent(indent).append("},\n");
                addIndent(indent).append("\"frame-exclude-negation-start\": ")
                        .append(String.valueOf(op.getFrameExcludeNegationStartIdx()));
            }
            Mutable<ILogicalExpression> frameExcludeUnaryExpression = op.getFrameExcludeUnaryExpression();
            if (frameExcludeUnaryExpression.getValue() != null) {
                buffer.append(",\n");
                addIndent(indent).append("\"frame-exclude-unary\": ");
                pprintExpr(frameExcludeUnaryExpression, fldIndent);
            }
            Mutable<ILogicalExpression> frameOffsetExpression = op.getFrameOffsetExpression();
            if (frameOffsetExpression.getValue() != null) {
                buffer.append(",\n");
                addIndent(indent).append("\"frame-offset\": ");
                pprintExpr(frameOffsetExpression, fldIndent);
            }
            int frameMaxObjects = op.getFrameMaxObjects();
            if (frameMaxObjects != -1) {
                buffer.append(",\n");
                addIndent(indent).append("\"frame-max-objects\": ").append(String.valueOf(frameMaxObjects));
            }
            buffer.append(",\n");
            addIndent(indent).append("\"subplan\": ");
            printNestedPlans(op, fldIndent);
        }
        return null;
    }

    protected void printNestedPlans(AbstractOperatorWithNestedPlans op, Integer indent) throws AlgebricksException {
        idCounter.nextPrefix();
        buffer.append("[\n");
        boolean first = true;
        for (ILogicalPlan p : op.getNestedPlans()) {
            if (!first) {
                buffer.append(",");
            }
            printPlan(p, indent + 4);
            first = false;
        }
        addIndent(indent).append("]");
        idCounter.previousPrefix();
    }

    protected void pprintExprList(List<Mutable<ILogicalExpression>> expressions, Integer indent)
            throws AlgebricksException {
        addIndent(indent);
        buffer.append("\"expressions\": \"");
        boolean first = true;
        for (Mutable<ILogicalExpression> exprRef : expressions) {
            if (first) {
                first = false;
            } else {
                buffer.append(", ");
            }
            pprintExpr(exprRef, indent);
        }
        buffer.append("\"");
    }

    protected void pprintExpr(Mutable<ILogicalExpression> exprRef, Integer indent) throws AlgebricksException {
        buffer.append(exprRef.getValue().accept(exprVisitor, indent).replace('"', ' '));
    }

    protected void pprintVeList(List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> vePairList, Integer indent)
            throws AlgebricksException {
        buffer.append("[");
        boolean first = true;
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> ve : vePairList) {
            if (first) {
                first = false;
            } else {
                buffer.append(",");
            }
            if (ve.first != null) {
                buffer.append("{\"variable\": \"" + ve.first.toString().replace('"', ' ') + "\"," + "\"expression\": \""
                        + ve.second.toString().replace('"', ' ') + "\"}");
            } else {
                buffer.append("{\"expression\": \"" + ve.second.getValue().accept(exprVisitor, indent).replace('"', ' ')
                        + "\"}");
            }
        }
        buffer.append("]");
    }

    private void pprintOrderExprList(List<Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>>> orderExpressions,
            Integer indent) throws AlgebricksException {
        buffer.append("[");
        boolean first = true;
        for (Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>> p : orderExpressions) {
            if (first) {
                first = false;
            } else {
                buffer.append(",");
            }
            buffer.append("{\"order\": \"" + getOrderString(p.first, indent) + "\"," + "\"expression\": \""
                    + p.second.getValue().accept(exprVisitor, indent).replace('"', ' ') + "\"}");
        }
        buffer.append("]");
    }

    private String getOrderString(OrderOperator.IOrder order, Integer indent) throws AlgebricksException {
        switch (order.getKind()) {
            case ASC:
                return "ASC";
            case DESC:
                return "DESC";
            default:
                return order.getExpressionRef().getValue().accept(exprVisitor, indent).replace('"', ' ');
        }
    }
}
