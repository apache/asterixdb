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

import java.io.IOException;
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
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractUnnestNonMapOperator;
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
import org.apache.hyracks.api.exceptions.ErrorCode;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;

public class LogicalOperatorPrettyPrintVisitorJson extends AbstractLogicalOperatorPrettyPrintVisitor<Void>
        implements IPlanPrettyPrinter {

    private static final JsonFactory JSON_FACTORY = new JsonFactory();
    private static final DefaultIndenter OBJECT_INDENT = new DefaultIndenter("   ", DefaultIndenter.SYS_LF);
    private static final String OPERATOR_FIELD = "operator";
    private static final String VARIABLES_FIELD = "variables";
    // printing using the "expressions" field has to be an array of strings of the form ["str1", "str2", ...]
    private static final String EXPRESSIONS_FIELD = "expressions";
    private static final String EXPRESSION_FIELD = "expression";
    private static final String CONDITION_FIELD = "condition";

    private final Map<AbstractLogicalOperator, String> operatorIdentity = new HashMap<>();
    private final IdCounter idCounter = new IdCounter();
    private final JsonGenerator jsonGenerator;

    LogicalOperatorPrettyPrintVisitorJson() {
        super(new LogicalExpressionPrettyPrintVisitor<>());
        DefaultPrettyPrinter prettyPrinter = new DefaultPrettyPrinter(DefaultIndenter.SYS_LF);
        prettyPrinter.indentObjectsWith(OBJECT_INDENT);
        try {
            jsonGenerator = JSON_FACTORY.createGenerator(buffer).setPrettyPrinter(prettyPrinter);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private class IdCounter {
        private int id;
        private final Deque<Integer> prefix;

        private IdCounter() {
            prefix = new LinkedList<>();
            prefix.add(1);
            this.id = 0;
        }

        private void previousPrefix() {
            this.id = prefix.removeLast();
        }

        private void nextPrefix() {
            prefix.add(this.id);
            this.id = 0;
        }

        private String printOperatorId(AbstractLogicalOperator op) {
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
    public final IPlanPrettyPrinter reset() throws AlgebricksException {
        flushContentToWriter();
        resetState();
        operatorIdentity.clear();
        return this;
    }

    @Override
    public final IPlanPrettyPrinter printPlan(ILogicalPlan plan) throws AlgebricksException {
        printPlanImpl(plan);
        flushContentToWriter();
        return this;
    }

    @Override
    public final IPlanPrettyPrinter printOperator(AbstractLogicalOperator op) throws AlgebricksException {
        printOperatorImpl(op);
        flushContentToWriter();
        return this;
    }

    private void printPlanImpl(ILogicalPlan plan) throws AlgebricksException {
        try {
            boolean writeArrayOfRoots = plan.getRoots().size() > 1;
            if (writeArrayOfRoots) {
                jsonGenerator.writeStartArray();
            }
            for (Mutable<ILogicalOperator> root : plan.getRoots()) {
                printOperatorImpl((AbstractLogicalOperator) root.getValue());
            }
            if (writeArrayOfRoots) {
                jsonGenerator.writeEndArray();
            }
        } catch (IOException e) {
            throw new AlgebricksException(e, ErrorCode.ERROR_PRINTING_PLAN);
        }
    }

    private void printOperatorImpl(AbstractLogicalOperator op) throws AlgebricksException {
        try {
            jsonGenerator.writeStartObject();
            op.accept(this, null);
            jsonGenerator.writeStringField("operatorId", idCounter.printOperatorId(op));
            IPhysicalOperator pOp = op.getPhysicalOperator();
            if (pOp != null) {
                jsonGenerator.writeStringField("physical-operator", pOp.toString());
            }
            jsonGenerator.writeStringField("execution-mode", op.getExecutionMode().toString());
            if (!op.getInputs().isEmpty()) {
                jsonGenerator.writeArrayFieldStart("inputs");
                for (Mutable<ILogicalOperator> k : op.getInputs()) {
                    printOperatorImpl((AbstractLogicalOperator) k.getValue());
                }
                jsonGenerator.writeEndArray();
            }
            jsonGenerator.writeEndObject();
        } catch (IOException e) {
            throw new AlgebricksException(e, ErrorCode.ERROR_PRINTING_PLAN);
        }
    }

    @Override
    public Void visitAggregateOperator(AggregateOperator op, Void indent) throws AlgebricksException {
        try {
            jsonGenerator.writeStringField(OPERATOR_FIELD, "aggregate");
            writeVariablesAndExpressions(op.getVariables(), op.getExpressions(), indent);
            return null;
        } catch (IOException e) {
            throw new AlgebricksException(e, ErrorCode.ERROR_PRINTING_PLAN);
        }
    }

    @Override
    public Void visitRunningAggregateOperator(RunningAggregateOperator op, Void indent) throws AlgebricksException {
        try {
            jsonGenerator.writeStringField(OPERATOR_FIELD, "running-aggregate");
            writeVariablesAndExpressions(op.getVariables(), op.getExpressions(), indent);
            return null;
        } catch (IOException e) {
            throw new AlgebricksException(e, ErrorCode.ERROR_PRINTING_PLAN);
        }
    }

    @Override
    public Void visitEmptyTupleSourceOperator(EmptyTupleSourceOperator op, Void indent) throws AlgebricksException {
        try {
            jsonGenerator.writeStringField(OPERATOR_FIELD, "empty-tuple-source");
            return null;
        } catch (IOException e) {
            throw new AlgebricksException(e, ErrorCode.ERROR_PRINTING_PLAN);
        }
    }

    @Override
    public Void visitGroupByOperator(GroupByOperator op, Void indent) throws AlgebricksException {
        try {
            jsonGenerator.writeStringField(OPERATOR_FIELD, "group-by");
            if (op.isGroupAll()) {
                jsonGenerator.writeStringField("option", "all");
            }
            if (!op.getGroupByList().isEmpty()) {
                writeArrayFieldOfVariableExpressionPairs("group-by-list", op.getGroupByList(), indent);
            }
            if (!op.getDecorList().isEmpty()) {
                writeArrayFieldOfVariableExpressionPairs("decor-list", op.getDecorList(), indent);
            }
            if (!op.getNestedPlans().isEmpty()) {
                writeNestedPlans(op, indent);
            }
            return null;
        } catch (IOException e) {
            throw new AlgebricksException(e, ErrorCode.ERROR_PRINTING_PLAN);
        }
    }

    @Override
    public Void visitDistinctOperator(DistinctOperator op, Void indent) throws AlgebricksException {
        try {
            jsonGenerator.writeStringField(OPERATOR_FIELD, "distinct");
            List<Mutable<ILogicalExpression>> expressions = op.getExpressions();
            if (!expressions.isEmpty()) {
                writeArrayFieldOfExpressions(EXPRESSIONS_FIELD, expressions, indent);
            }
            return null;
        } catch (IOException e) {
            throw new AlgebricksException(e, ErrorCode.ERROR_PRINTING_PLAN);
        }
    }

    @Override
    public Void visitInnerJoinOperator(InnerJoinOperator op, Void indent) throws AlgebricksException {
        try {
            jsonGenerator.writeStringField(OPERATOR_FIELD, "join");
            writeStringFieldExpression(CONDITION_FIELD, op.getCondition(), indent);
            return null;
        } catch (IOException e) {
            throw new AlgebricksException(e, ErrorCode.ERROR_PRINTING_PLAN);
        }
    }

    @Override
    public Void visitLeftOuterJoinOperator(LeftOuterJoinOperator op, Void indent) throws AlgebricksException {
        try {
            jsonGenerator.writeStringField(OPERATOR_FIELD, "left-outer-join");
            writeStringFieldExpression(CONDITION_FIELD, op.getCondition(), indent);
            return null;
        } catch (IOException e) {
            throw new AlgebricksException(e, ErrorCode.ERROR_PRINTING_PLAN);
        }
    }

    @Override
    public Void visitNestedTupleSourceOperator(NestedTupleSourceOperator op, Void indent) throws AlgebricksException {
        try {
            jsonGenerator.writeStringField(OPERATOR_FIELD, "nested-tuple-source");
            return null;
        } catch (IOException e) {
            throw new AlgebricksException(e, ErrorCode.ERROR_PRINTING_PLAN);
        }
    }

    @Override
    public Void visitOrderOperator(OrderOperator op, Void indent) throws AlgebricksException {
        try {
            jsonGenerator.writeStringField(OPERATOR_FIELD, "order");
            int topK = op.getTopK();
            if (topK != -1) {
                jsonGenerator.writeStringField("topK", String.valueOf(topK));
            }
            writeArrayFieldOfOrderExprList("order-by-list", op.getOrderExpressions(), indent);
            return null;
        } catch (IOException e) {
            throw new AlgebricksException(e, ErrorCode.ERROR_PRINTING_PLAN);
        }
    }

    @Override
    public Void visitAssignOperator(AssignOperator op, Void indent) throws AlgebricksException {
        try {
            jsonGenerator.writeStringField(OPERATOR_FIELD, "assign");
            writeVariablesAndExpressions(op.getVariables(), op.getExpressions(), indent);
            return null;
        } catch (IOException e) {
            throw new AlgebricksException(e, ErrorCode.ERROR_PRINTING_PLAN);
        }
    }

    @Override
    public Void visitWriteOperator(WriteOperator op, Void indent) throws AlgebricksException {
        try {
            jsonGenerator.writeStringField(OPERATOR_FIELD, "write");
            List<Mutable<ILogicalExpression>> expressions = op.getExpressions();
            if (!expressions.isEmpty()) {
                writeArrayFieldOfExpressions(EXPRESSIONS_FIELD, expressions, indent);
            }
            return null;
        } catch (IOException e) {
            throw new AlgebricksException(e, ErrorCode.ERROR_PRINTING_PLAN);
        }
    }

    @Override
    public Void visitDistributeResultOperator(DistributeResultOperator op, Void indent) throws AlgebricksException {
        try {
            jsonGenerator.writeStringField(OPERATOR_FIELD, "distribute-result");
            List<Mutable<ILogicalExpression>> expressions = op.getExpressions();
            if (!expressions.isEmpty()) {
                writeArrayFieldOfExpressions(EXPRESSIONS_FIELD, expressions, indent);
            }
            return null;
        } catch (IOException e) {
            throw new AlgebricksException(e, ErrorCode.ERROR_PRINTING_PLAN);
        }
    }

    @Override
    public Void visitWriteResultOperator(WriteResultOperator op, Void indent) throws AlgebricksException {
        try {
            jsonGenerator.writeStringField(OPERATOR_FIELD, "load");
            jsonGenerator.writeStringField("data-source", String.valueOf(op.getDataSource()));
            writeStringFieldExpression("from", op.getPayloadExpression(), indent);
            writeObjectFieldWithExpressions("partitioned-by", op.getKeyExpressions(), indent);
            return null;
        } catch (IOException e) {
            throw new AlgebricksException(e, ErrorCode.ERROR_PRINTING_PLAN);
        }
    }

    @Override
    public Void visitSelectOperator(SelectOperator op, Void indent) throws AlgebricksException {
        try {
            jsonGenerator.writeStringField(OPERATOR_FIELD, "select");
            writeStringFieldExpression(CONDITION_FIELD, op.getCondition(), indent);
            return null;
        } catch (IOException e) {
            throw new AlgebricksException(e, ErrorCode.ERROR_PRINTING_PLAN);
        }
    }

    @Override
    public Void visitProjectOperator(ProjectOperator op, Void indent) throws AlgebricksException {
        try {
            jsonGenerator.writeStringField(OPERATOR_FIELD, "project");
            List<LogicalVariable> variables = op.getVariables();
            if (!variables.isEmpty()) {
                writeArrayFieldOfVariables(VARIABLES_FIELD, variables);
            }
            return null;
        } catch (IOException e) {
            throw new AlgebricksException(e, ErrorCode.ERROR_PRINTING_PLAN);
        }
    }

    @Override
    public Void visitSubplanOperator(SubplanOperator op, Void indent) throws AlgebricksException {
        if (!op.getNestedPlans().isEmpty()) {
            writeNestedPlans(op, indent);
        }
        return null;
    }

    @Override
    public Void visitUnionOperator(UnionAllOperator op, Void indent) throws AlgebricksException {
        try {
            jsonGenerator.writeStringField(OPERATOR_FIELD, "union");
            jsonGenerator.writeArrayFieldStart("values");
            for (Triple<LogicalVariable, LogicalVariable, LogicalVariable> v : op.getVariableMappings()) {
                jsonGenerator.writeStartArray();
                jsonGenerator.writeString(String.valueOf(v.first));
                jsonGenerator.writeString(String.valueOf(v.second));
                jsonGenerator.writeString(String.valueOf(v.third));
                jsonGenerator.writeEndArray();
            }
            jsonGenerator.writeEndArray();
            return null;
        } catch (IOException e) {
            throw new AlgebricksException(e, ErrorCode.ERROR_PRINTING_PLAN);
        }
    }

    @Override
    public Void visitIntersectOperator(IntersectOperator op, Void indent) throws AlgebricksException {
        try {
            jsonGenerator.writeStringField(OPERATOR_FIELD, "intersect");
            writeArrayFieldOfVariables("output-compare-variables", op.getOutputCompareVariables());
            if (op.hasExtraVariables()) {
                writeArrayFieldOfVariables("output-extra-variables", op.getOutputExtraVariables());
            }
            writeArrayFieldOfNestedVariablesList("input-compare-variables", op.getAllInputsCompareVariables());
            if (op.hasExtraVariables()) {
                writeArrayFieldOfNestedVariablesList("input-extra-variables", op.getAllInputsExtraVariables());
            }
            return null;
        } catch (IOException e) {
            throw new AlgebricksException(e, ErrorCode.ERROR_PRINTING_PLAN);
        }
    }

    @Override
    public Void visitUnnestOperator(UnnestOperator op, Void indent) throws AlgebricksException {
        writeUnnestNonMapOperator(op, "unnest", indent);
        return null;
    }

    @Override
    public Void visitLeftOuterUnnestOperator(LeftOuterUnnestOperator op, Void indent) throws AlgebricksException {
        writeUnnestNonMapOperator(op, "outer-unnest", indent);
        return null;
    }

    @Override
    public Void visitUnnestMapOperator(UnnestMapOperator op, Void indent) throws AlgebricksException {
        try {
            writeUnnestMapOperator(op, indent, "unnest-map");
            writeSelectLimitInformation(op.getSelectCondition(), op.getOutputLimit(), indent);
            return null;
        } catch (IOException e) {
            throw new AlgebricksException(e, ErrorCode.ERROR_PRINTING_PLAN);
        }
    }

    @Override
    public Void visitLeftOuterUnnestMapOperator(LeftOuterUnnestMapOperator op, Void indent) throws AlgebricksException {
        writeUnnestMapOperator(op, indent, "left-outer-unnest-map");
        return null;
    }

    @Override
    public Void visitDataScanOperator(DataSourceScanOperator op, Void indent) throws AlgebricksException {
        try {
            jsonGenerator.writeStringField(OPERATOR_FIELD, "data-scan");
            List<LogicalVariable> projectVariables = op.getProjectVariables();
            if (!projectVariables.isEmpty()) {
                writeArrayFieldOfVariables("project-variables", projectVariables);
            }
            List<LogicalVariable> variables = op.getVariables();
            if (!variables.isEmpty()) {
                writeArrayFieldOfVariables(VARIABLES_FIELD, variables);
            }
            if (op.getDataSource() != null) {
                jsonGenerator.writeStringField("data-source", String.valueOf(op.getDataSource()));
            }
            writeFilterInformation(op.getMinFilterVars(), op.getMaxFilterVars());
            writeSelectLimitInformation(op.getSelectCondition(), op.getOutputLimit(), indent);
            return null;
        } catch (IOException e) {
            throw new AlgebricksException(e, ErrorCode.ERROR_PRINTING_PLAN);
        }
    }

    @Override
    public Void visitLimitOperator(LimitOperator op, Void indent) throws AlgebricksException {
        try {
            jsonGenerator.writeStringField(OPERATOR_FIELD, "limit");
            writeStringFieldExpression("value", op.getMaxObjects(), indent);
            Mutable<ILogicalExpression> offsetRef = op.getOffset();
            if (offsetRef != null && offsetRef.getValue() != null) {
                writeStringFieldExpression("offset", offsetRef, indent);
            }
            return null;
        } catch (IOException e) {
            throw new AlgebricksException(e, ErrorCode.ERROR_PRINTING_PLAN);
        }
    }

    @Override
    public Void visitExchangeOperator(ExchangeOperator op, Void indent) throws AlgebricksException {
        try {
            jsonGenerator.writeStringField(OPERATOR_FIELD, "exchange");
            return null;
        } catch (IOException e) {
            throw new AlgebricksException(e, ErrorCode.ERROR_PRINTING_PLAN);
        }
    }

    @Override
    public Void visitScriptOperator(ScriptOperator op, Void indent) throws AlgebricksException {
        try {
            jsonGenerator.writeStringField(OPERATOR_FIELD, "script");
            List<LogicalVariable> inputVariables = op.getInputVariables();
            if (!inputVariables.isEmpty()) {
                writeArrayFieldOfVariables("in", inputVariables);
            }
            List<LogicalVariable> outputVariables = op.getOutputVariables();
            if (!outputVariables.isEmpty()) {
                writeArrayFieldOfVariables("out", outputVariables);
            }
            return null;
        } catch (IOException e) {
            throw new AlgebricksException(e, ErrorCode.ERROR_PRINTING_PLAN);
        }
    }

    @Override
    public Void visitReplicateOperator(ReplicateOperator op, Void indent) throws AlgebricksException {
        try {
            jsonGenerator.writeStringField(OPERATOR_FIELD, "replicate");
            return null;
        } catch (IOException e) {
            throw new AlgebricksException(e, ErrorCode.ERROR_PRINTING_PLAN);
        }
    }

    @Override
    public Void visitSplitOperator(SplitOperator op, Void indent) throws AlgebricksException {
        try {
            jsonGenerator.writeStringField(OPERATOR_FIELD, "split");
            writeStringFieldExpression(EXPRESSION_FIELD, op.getBranchingExpression(), indent);
            return null;
        } catch (IOException e) {
            throw new AlgebricksException(e, ErrorCode.ERROR_PRINTING_PLAN);
        }
    }

    @Override
    public Void visitMaterializeOperator(MaterializeOperator op, Void indent) throws AlgebricksException {
        try {
            jsonGenerator.writeStringField(OPERATOR_FIELD, "materialize");
            return null;
        } catch (IOException e) {
            throw new AlgebricksException(e, ErrorCode.ERROR_PRINTING_PLAN);
        }
    }

    @Override
    public Void visitInsertDeleteUpsertOperator(InsertDeleteUpsertOperator op, Void indent) throws AlgebricksException {
        try {
            jsonGenerator.writeStringField(OPERATOR_FIELD, getIndexOpString(op.getOperation()));
            jsonGenerator.writeStringField("data-source", String.valueOf(op.getDataSource()));
            writeStringFieldExpression("from-record", op.getPayloadExpression(), indent);
            if (op.getAdditionalNonFilteringExpressions() != null) {
                writeObjectFieldWithExpressions("meta", op.getAdditionalNonFilteringExpressions(), indent);
            }
            writeObjectFieldWithExpressions("partitioned-by", op.getPrimaryKeyExpressions(), indent);
            if (op.getOperation() == Kind.UPSERT) {
                jsonGenerator.writeObjectFieldStart("out");
                jsonGenerator.writeStringField("record-before-upsert", String.valueOf(op.getBeforeOpRecordVar()));
                if (op.getBeforeOpAdditionalNonFilteringVars() != null) {
                    jsonGenerator.writeStringField("additional-before-upsert",
                            String.valueOf(op.getBeforeOpAdditionalNonFilteringVars()));
                }
                jsonGenerator.writeEndObject();
            }
            if (op.isBulkload()) {
                jsonGenerator.writeBooleanField("bulkload", true);
            }
            return null;
        } catch (IOException e) {
            throw new AlgebricksException(e, ErrorCode.ERROR_PRINTING_PLAN);
        }
    }

    @Override
    public Void visitIndexInsertDeleteUpsertOperator(IndexInsertDeleteUpsertOperator op, Void indent)
            throws AlgebricksException {
        try {
            jsonGenerator.writeStringField(OPERATOR_FIELD, getIndexOpString(op.getOperation()));
            jsonGenerator.writeStringField("index", op.getIndexName());
            jsonGenerator.writeStringField("on", String.valueOf(op.getDataSourceIndex().getDataSource()));
            jsonGenerator.writeObjectFieldStart("from");
            if (op.getOperation() == Kind.UPSERT) {
                writeArrayFieldOfExpressions("replace", op.getPrevSecondaryKeyExprs(), indent);
                writeArrayFieldOfExpressions("with", op.getSecondaryKeyExpressions(), indent);
            } else {
                writeArrayFieldOfExpressions(EXPRESSIONS_FIELD, op.getSecondaryKeyExpressions(), indent);
            }
            jsonGenerator.writeEndObject();
            if (op.isBulkload()) {
                jsonGenerator.writeBooleanField("bulkload", true);
            }
            return null;
        } catch (IOException e) {
            throw new AlgebricksException(e, ErrorCode.ERROR_PRINTING_PLAN);
        }
    }

    @Override
    public Void visitTokenizeOperator(TokenizeOperator op, Void indent) throws AlgebricksException {
        try {
            jsonGenerator.writeStringField(OPERATOR_FIELD, "tokenize");
            writeVariablesAndExpressions(op.getTokenizeVars(), op.getSecondaryKeyExpressions(), indent);
            return null;
        } catch (IOException e) {
            throw new AlgebricksException(e, ErrorCode.ERROR_PRINTING_PLAN);
        }
    }

    @Override
    public Void visitForwardOperator(ForwardOperator op, Void indent) throws AlgebricksException {
        try {
            jsonGenerator.writeStringField(OPERATOR_FIELD, "forward");
            writeStringFieldExpression(EXPRESSION_FIELD, op.getSideDataExpression(), indent);
            return null;
        } catch (IOException e) {
            throw new AlgebricksException(e, ErrorCode.ERROR_PRINTING_PLAN);
        }
    }

    @Override
    public Void visitSinkOperator(SinkOperator op, Void indent) throws AlgebricksException {
        try {
            jsonGenerator.writeStringField(OPERATOR_FIELD, "sink");
            return null;
        } catch (IOException e) {
            throw new AlgebricksException(e, ErrorCode.ERROR_PRINTING_PLAN);
        }
    }

    @Override
    public Void visitDelegateOperator(DelegateOperator op, Void indent) throws AlgebricksException {
        try {
            jsonGenerator.writeStringField(OPERATOR_FIELD, op.toString());
            return null;
        } catch (IOException e) {
            throw new AlgebricksException(e, ErrorCode.ERROR_PRINTING_PLAN);
        }
    }

    @Override
    public Void visitWindowOperator(WindowOperator op, Void indent) throws AlgebricksException {
        try {
            jsonGenerator.writeStringField(OPERATOR_FIELD, "window-aggregate");
            writeVariablesAndExpressions(op.getVariables(), op.getExpressions(), indent);
            List<Mutable<ILogicalExpression>> partitionExpressions = op.getPartitionExpressions();
            if (!partitionExpressions.isEmpty()) {
                writeObjectFieldWithExpressions("partition-by", partitionExpressions, indent);
            }
            List<Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>>> orderExpressions = op.getOrderExpressions();
            if (!orderExpressions.isEmpty()) {
                writeArrayFieldOfOrderExprList("order-by", orderExpressions, indent);
            }
            if (op.hasNestedPlans()) {
                List<Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>>> frameValueExpressions =
                        op.getFrameValueExpressions();
                if (!frameValueExpressions.isEmpty()) {
                    writeArrayFieldOfOrderExprList("frame-on", frameValueExpressions, indent);
                }
                List<Mutable<ILogicalExpression>> frameStartExpressions = op.getFrameStartExpressions();
                if (!frameStartExpressions.isEmpty()) {
                    writeObjectFieldWithExpressions("frame-start", frameStartExpressions, indent);
                }
                List<Mutable<ILogicalExpression>> frameStartValidationExpressions =
                        op.getFrameStartValidationExpressions();
                if (!frameStartValidationExpressions.isEmpty()) {
                    writeObjectFieldWithExpressions("frame-start-if", frameStartValidationExpressions, indent);
                }
                List<Mutable<ILogicalExpression>> frameEndExpressions = op.getFrameEndExpressions();
                if (!frameEndExpressions.isEmpty()) {
                    writeObjectFieldWithExpressions("frame-end", frameEndExpressions, indent);
                }
                List<Mutable<ILogicalExpression>> frameEndValidationExpressions = op.getFrameEndValidationExpressions();
                if (!frameEndValidationExpressions.isEmpty()) {
                    writeObjectFieldWithExpressions("frame-end-if", frameEndValidationExpressions, indent);
                }
                List<Mutable<ILogicalExpression>> frameExcludeExpressions = op.getFrameExcludeExpressions();
                if (!frameExcludeExpressions.isEmpty()) {
                    writeObjectFieldWithExpressions("frame-exclude", frameExcludeExpressions, indent);
                    jsonGenerator.writeStringField("frame-exclude-negation-start",
                            String.valueOf(op.getFrameExcludeNegationStartIdx()));
                }
                Mutable<ILogicalExpression> frameExcludeUnaryExpression = op.getFrameExcludeUnaryExpression();
                if (frameExcludeUnaryExpression.getValue() != null) {
                    writeStringFieldExpression("frame-exclude-unary", frameExcludeUnaryExpression, indent);
                }
                Mutable<ILogicalExpression> frameOffsetExpression = op.getFrameOffsetExpression();
                if (frameOffsetExpression.getValue() != null) {
                    writeStringFieldExpression("frame-offset", frameOffsetExpression, indent);
                }
                int frameMaxObjects = op.getFrameMaxObjects();
                if (frameMaxObjects != -1) {
                    jsonGenerator.writeStringField("frame-max-objects", String.valueOf(frameMaxObjects));
                }
                writeNestedPlans(op, indent);
            }
            return null;
        } catch (IOException e) {
            throw new AlgebricksException(e, ErrorCode.ERROR_PRINTING_PLAN);
        }
    }

    private void writeNestedPlans(AbstractOperatorWithNestedPlans op, Void indent) throws AlgebricksException {
        try {
            idCounter.nextPrefix();
            jsonGenerator.writeArrayFieldStart("subplan");
            List<ILogicalPlan> nestedPlans = op.getNestedPlans();
            for (int i = 0, size = nestedPlans.size(); i < size; i++) {
                printPlanImpl(nestedPlans.get(i));
            }
            jsonGenerator.writeEndArray();
            idCounter.previousPrefix();
        } catch (IOException e) {
            throw new AlgebricksException(e, ErrorCode.ERROR_PRINTING_PLAN);
        }
    }

    private void writeUnnestNonMapOperator(AbstractUnnestNonMapOperator op, String opName, Void indent)
            throws AlgebricksException {
        try {
            jsonGenerator.writeStringField(OPERATOR_FIELD, opName);
            List<LogicalVariable> variables = op.getVariables();
            if (!variables.isEmpty()) {
                writeArrayFieldOfVariables(VARIABLES_FIELD, variables);
            }
            LogicalVariable positionalVariable = op.getPositionalVariable();
            if (positionalVariable != null) {
                jsonGenerator.writeStringField("position", String.valueOf(positionalVariable));
            }
            writeArrayFieldOfExpression(EXPRESSIONS_FIELD, op.getExpressionRef(), indent);
        } catch (IOException e) {
            throw new AlgebricksException(e, ErrorCode.ERROR_PRINTING_PLAN);
        }
    }

    private void writeUnnestMapOperator(AbstractUnnestMapOperator op, Void indent, String opName)
            throws AlgebricksException {
        try {
            jsonGenerator.writeStringField(OPERATOR_FIELD, opName);
            List<LogicalVariable> variables = op.getVariables();
            if (!variables.isEmpty()) {
                writeArrayFieldOfVariables(VARIABLES_FIELD, variables);
            }
            writeArrayFieldOfExpression(EXPRESSIONS_FIELD, op.getExpressionRef(), indent);
            writeFilterInformation(op.getMinFilterVars(), op.getMaxFilterVars());
        } catch (IOException e) {
            throw new AlgebricksException(e, ErrorCode.ERROR_PRINTING_PLAN);
        }
    }

    private void writeFilterInformation(List<LogicalVariable> minFilterVars, List<LogicalVariable> maxFilterVars)
            throws AlgebricksException {
        try {
            if (minFilterVars != null || maxFilterVars != null) {
                jsonGenerator.writeObjectFieldStart("with-filter-on");
                if (minFilterVars != null) {
                    writeArrayFieldOfVariables("min", minFilterVars);
                }
                if (maxFilterVars != null) {
                    writeArrayFieldOfVariables("max", maxFilterVars);
                }
                jsonGenerator.writeEndObject();
            }
        } catch (IOException e) {
            throw new AlgebricksException(e, ErrorCode.ERROR_PRINTING_PLAN);
        }
    }

    private void writeSelectLimitInformation(Mutable<ILogicalExpression> selectCondition, long outputLimit, Void i)
            throws AlgebricksException, IOException {
        if (selectCondition != null) {
            writeStringFieldExpression(CONDITION_FIELD, selectCondition, i);
        }
        if (outputLimit >= 0) {
            jsonGenerator.writeStringField("limit", String.valueOf(outputLimit));
        }
    }

    private void writeVariablesAndExpressions(List<LogicalVariable> variables,
            List<Mutable<ILogicalExpression>> expressions, Void indent) throws IOException, AlgebricksException {
        if (!variables.isEmpty()) {
            writeArrayFieldOfVariables(VARIABLES_FIELD, variables);
        }
        if (!expressions.isEmpty()) {
            writeArrayFieldOfExpressions(EXPRESSIONS_FIELD, expressions, indent);
        }
    }

    private String getIndexOpString(Kind opKind) {
        switch (opKind) {
            case DELETE:
                return "delete-from";
            case INSERT:
                return "insert-into";
            case UPSERT:
                return "upsert-into";
            default:
                throw new IllegalStateException();

        }
    }

    private String getOrderString(OrderOperator.IOrder order, Void indent) throws AlgebricksException {
        switch (order.getKind()) {
            case ASC:
                return "ASC";
            case DESC:
                return "DESC";
            default:
                return order.getExpressionRef().getValue().accept(exprVisitor, indent);
        }
    }

    /////////////// string fields ///////////////
    /** Writes "fieldName": "expr" */
    private void writeStringFieldExpression(String fieldName, Mutable<ILogicalExpression> expression, Void indent)
            throws AlgebricksException, IOException {
        jsonGenerator.writeStringField(fieldName, expression.getValue().accept(exprVisitor, indent));
    }

    /////////////// array fields ///////////////
    /** Writes "fieldName": [ "var1", "var2", ... ] */
    private void writeArrayFieldOfVariables(String fieldName, List<LogicalVariable> variables) throws IOException {
        jsonGenerator.writeArrayFieldStart(fieldName);
        for (int i = 0, size = variables.size(); i < size; i++) {
            jsonGenerator.writeString(String.valueOf(variables.get(i)));
        }
        jsonGenerator.writeEndArray();
    }

    /** Writes "fieldName": [ ["var1", "var2", ...], ["var1", "var2", ...] ] */
    private void writeArrayFieldOfNestedVariablesList(String fieldName, List<List<LogicalVariable>> nestedVarList)
            throws IOException {
        jsonGenerator.writeArrayFieldStart(fieldName);
        for (int i = 0, size = nestedVarList.size(); i < size; i++) {
            List<LogicalVariable> nextList = nestedVarList.get(i);
            for (int k = 0, varSize = nextList.size(); k < varSize; k++) {
                jsonGenerator.writeString(String.valueOf(nextList.get(k)));
            }
        }
        jsonGenerator.writeEndArray();
    }

    /** Writes "fieldName" : [ "expr" ] */
    private void writeArrayFieldOfExpression(String fieldName, Mutable<ILogicalExpression> expr, Void indent)
            throws IOException, AlgebricksException {
        jsonGenerator.writeArrayFieldStart(fieldName);
        jsonGenerator.writeString(expr.getValue().accept(exprVisitor, indent));
        jsonGenerator.writeEndArray();
    }

    /** Writes "fieldName" : [ "expr1", "expr2", ...] */
    private void writeArrayFieldOfExpressions(String fieldName, List<Mutable<ILogicalExpression>> exprs, Void indent)
            throws IOException, AlgebricksException {
        jsonGenerator.writeArrayFieldStart(fieldName);
        for (int i = 0, size = exprs.size(); i < size; i++) {
            jsonGenerator.writeString(exprs.get(i).getValue().accept(exprVisitor, indent));
        }
        jsonGenerator.writeEndArray();
    }

    /** Writes "fieldName" : [ { "variable": "var1", "expression": "expr1" }, ... ] */
    private void writeArrayFieldOfVariableExpressionPairs(String fieldName,
            List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> varExprPairs, Void indent)
            throws AlgebricksException, IOException {
        jsonGenerator.writeArrayFieldStart(fieldName);
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> ve : varExprPairs) {
            jsonGenerator.writeStartObject();
            if (ve.first != null) {
                jsonGenerator.writeStringField("variable", ve.first.toString());
            }
            writeStringFieldExpression(EXPRESSION_FIELD, ve.second, indent);
            jsonGenerator.writeEndObject();
        }
        jsonGenerator.writeEndArray();
    }

    /** Writes "fieldName" : [ { "order": "", "expression": "" }, ... ] */
    private void writeArrayFieldOfOrderExprList(String fieldName,
            List<Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>>> orderList, Void indent)
            throws AlgebricksException, IOException {
        jsonGenerator.writeArrayFieldStart(fieldName);
        for (Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>> p : orderList) {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField("order", getOrderString(p.first, indent));
            writeStringFieldExpression(EXPRESSION_FIELD, p.second, indent);
            jsonGenerator.writeEndObject();
        }
        jsonGenerator.writeEndArray();
    }

    /////////////// object fields ///////////////
    /** Writes "fieldName" : { "expressions": [ "expr1", "expr2", ...] } */
    private void writeObjectFieldWithExpressions(String fieldName, List<Mutable<ILogicalExpression>> exprs, Void indent)
            throws IOException, AlgebricksException {
        jsonGenerator.writeObjectFieldStart(fieldName);
        writeArrayFieldOfExpressions(EXPRESSIONS_FIELD, exprs, indent);
        jsonGenerator.writeEndObject();
    }

    private void flushContentToWriter() throws AlgebricksException {
        try {
            jsonGenerator.flush();
        } catch (IOException e) {
            throw new AlgebricksException(e, ErrorCode.ERROR_PRINTING_PLAN);
        }
    }
}
