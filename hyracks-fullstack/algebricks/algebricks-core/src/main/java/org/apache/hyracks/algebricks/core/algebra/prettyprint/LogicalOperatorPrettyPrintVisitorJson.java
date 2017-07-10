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

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.*;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteUpsertOperator.Kind;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionVisitor;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

import java.util.List;

public class LogicalOperatorPrettyPrintVisitorJson implements ILogicalOperatorVisitor<Void,Integer> {
    ILogicalExpressionVisitor<String, Integer> exprVisitor;
    AlgebricksAppendable buffer;

    public LogicalOperatorPrettyPrintVisitorJson(Appendable app) {
        this(new AlgebricksAppendable(app), new LogicalExpressionPrettyPrintVisitor());
    }

    public LogicalOperatorPrettyPrintVisitorJson(AlgebricksAppendable buffer) {
        this(buffer, new LogicalExpressionPrettyPrintVisitor());
    }

    public LogicalOperatorPrettyPrintVisitorJson(AlgebricksAppendable buffer, ILogicalExpressionVisitor<String, Integer> exprVisitor) {
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
        addIndent(indent).append("\"operator\":\"aggregate\"");
        if(op.getVariables().size() > 0) {
            addIndent(0).append(",\n");
            addIndent(indent).append("\"variables\" :[");
            boolean first = true;
            for (LogicalVariable v : op.getVariables()){
                if(!first)
                    buffer.append(",");
                buffer.append("\""+str(v)+"\"");
                first=false;
            }
            buffer.append("]");
        }

        return null;
    }

    @Override
    public Void visitRunningAggregateOperator(RunningAggregateOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\":\"running-aggregate\"");
        if(op.getVariables().size() > 0) {
                addIndent(0).append(",\n");
                addIndent(indent).append("\"variables\" :[");
                boolean first = true;
                for (LogicalVariable v : op.getVariables()){
                    if(!first)
                        buffer.append(",");
                    buffer.append("\""+str(v)+"\"");
                    first=false;
                }
                buffer.append("]");
            }
            if(op.getExpressions().size()>0) {
                addIndent(0).append(",\n");
                pprintExprList(op.getExpressions(), indent);
        }
        return null;
    }

    @Override
    public Void visitEmptyTupleSourceOperator(EmptyTupleSourceOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\":\"empty-tuple-source\"");
        return null;
    }


    @Override
    public Void visitGroupByOperator(GroupByOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\":\"group-by\"" );

        if(op.isGroupAll()) {
            buffer.append(",\n");
            addIndent(indent).append( "\"option\":\"all\"");
        }
            if(op.getGroupByList().size()>0) {
                buffer.append(",\n");
                addIndent(indent).append("\"group-by-list\":");
                pprintVeList(op.getGroupByList(), indent);
        }
        if(op.getDecorList().size()>0) {
            buffer.append(",\n");
            addIndent(indent).append("\"decor-list\":");
            pprintVeList(op.getDecorList(), indent);
        }
        if(op.getNestedPlans().size()>0) {
            buffer.append(",\n");
            addIndent(indent).append("\"subplan\":");
            PlanPrettyPrinter.reset(op);
            printNestedPlans(op, indent);
        }
        return null;
    }

    @Override
    public Void visitDistinctOperator(DistinctOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\":\"distinct\"");
        if(op.getExpressions().size()>0) {
            addIndent(0).append(",\n");
            pprintExprList(op.getExpressions(), indent);
        }
        return null;
    }

    @Override
    public Void visitInnerJoinOperator(InnerJoinOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\":\"join\",\n");
        addIndent(indent).append("\"condition\":"+"\""+op.getCondition().getValue().accept(exprVisitor, indent)+"\"");
        return null;
    }

    @Override
    public Void visitLeftOuterJoinOperator(LeftOuterJoinOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\":\"left-outer-join\",\n");
        addIndent(indent).append("\"condition\":"+"\""+op.getCondition().getValue().accept(exprVisitor, indent)+"\"");
        return null;
    }

    @Override
    public Void visitNestedTupleSourceOperator(NestedTupleSourceOperator op, Integer indent)
            throws AlgebricksException {
        addIndent(indent).append("\"operator\":\"nested-tuple-source\"");
        return null;
    }

    @Override
    public Void visitOrderOperator(OrderOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\":\"order\"");
        for (Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>> p : op.getOrderExpressions()) {
            buffer.append(",\n");
            if (op.getTopK() != -1) {
                addIndent(indent).append("\"topK\":\"" + op.getTopK() + "\",\n");
            }
            String fst = getOrderString(p.first);
            addIndent(indent).append("\"first\":" + fst + ",\n");
            addIndent(indent).append("\"second\":\""+p.second.getValue().accept(exprVisitor, indent).replace('"',' ') + "\"");
        }
        return null;
    }

    private String getOrderString(OrderOperator.IOrder first) {
        switch (first.getKind()) {
            case ASC:
                return "\"ASC\"";
            case DESC:
                return "\"DESC\"";
            default:
                return first.getExpressionRef().toString();
        }
    }

    @Override
    public Void visitAssignOperator(AssignOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\":\"assign\"");
        if(op.getVariables().size() > 0) {
            addIndent(0).append(",\n");
            addIndent(indent).append("\"variables\":[");
            boolean first = true;
            for (LogicalVariable v : op.getVariables()){
                if(!first)
                    buffer.append(",");
                buffer.append("\""+str(v)+"\"");
                first=false;
            }
            buffer.append("]");
        }
        if(op.getExpressions().size()>0) {
            addIndent(0).append(",\n");
            pprintExprList(op.getExpressions(), indent);
        }
        return null;
    }

    @Override
    public Void visitWriteOperator(WriteOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\":\"write\"");
        if(op.getExpressions().size()>0) {
            addIndent(0).append(",\n");
            pprintExprList(op.getExpressions(), indent);
        }
        return null;
    }

    @Override
    public Void visitDistributeResultOperator(DistributeResultOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\":\"distribute-result\"");
        if(op.getExpressions().size()>0) {
            addIndent(0).append(",\n");
            pprintExprList(op.getExpressions(), indent);
        }
        return null;
    }

    @Override
    public Void visitWriteResultOperator(WriteResultOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\":\"load\",\n");
        addIndent(indent).append(str(op.getDataSource())).append("\"from\":")
                .append(op.getPayloadExpression().getValue().accept(exprVisitor, indent)+",\n");
        addIndent(indent).append("\"partitioned-by\":{");
        pprintExprList(op.getKeyExpressions(), indent);
        addIndent(indent).append("}");
        return null;
    }

    @Override
    public Void visitSelectOperator(SelectOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\":\"select\",\n");
        addIndent(indent).append("\"expressions\":\""+op.getCondition().getValue().accept(exprVisitor, indent).replace('"',' ')+"\"");
        return null;
    }

    @Override
    public Void visitProjectOperator(ProjectOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\":\"project\"");
        if(op.getVariables().size() > 0) {
            addIndent(0).append(",\n");
            addIndent(indent).append("\"variables\":[");
            boolean first = true;
            for (LogicalVariable v : op.getVariables()){
                if(!first)
                    buffer.append(",");
                buffer.append("\""+str(v)+"\"");
                first=false;
            }
            buffer.append("]");
        }
        return null;
    }

    @Override
    public Void visitSubplanOperator(SubplanOperator op, Integer indent) throws AlgebricksException {
        if(!op.getNestedPlans().isEmpty()) {
            addIndent(indent).append("\"subplan\":");
            PlanPrettyPrinter.reset(op);
            printNestedPlans(op, indent);
        }
        return null;
    }
    @Override
    public Void visitUnionOperator(UnionAllOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\":\"union\"");
        for (Triple<LogicalVariable, LogicalVariable, LogicalVariable> v : op.getVariableMappings()) {
            buffer.append(",\n");
            addIndent(indent).append("\"values\":[" +"\""+ v.first + "\"," + "\""+v.second + "\"," + "\""+v.third + "\"]");
        }
        return null;
    }

    @Override
    public Void visitIntersectOperator(IntersectOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\":\"intersect\",\n");

        addIndent(indent).append("\"output-variables\":[");
        for (int i = 0; i < op.getOutputVars().size(); i++) {
            if (i > 0) {
                buffer.append(", ");
            }
            buffer.append("\""+str(op.getOutputVars().get(i))+"\"");
        }
        buffer.append("],");
        addIndent(indent).append("\"input_variables\":[");
        for (int i = 0; i < op.getNumInput(); i++) {
            if (i > 0) {
                buffer.append(",\n");
            }
            buffer.append("[");
            for (int j = 0; j < op.getInputVariables(i).size(); j++) {
                if (j > 0) {
                    buffer.append(", ");
                }
                buffer.append("\""+str(op.getInputVariables(i).get(j))+"\"");
            }
            buffer.append(']');
        }
        buffer.append("]");
        return null;
    }

    @Override
    public Void visitUnnestOperator(UnnestOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\":\"unnest\"");
        if(op.getVariables().size() > 0) {
            addIndent(0).append(",\n");
            addIndent(indent).append("\"variables\" :[");
            boolean first = true;
            for (LogicalVariable v : op.getVariables()){
                if(!first)
                    buffer.append(",");
                buffer.append("\""+str(v)+"\"");
                first=false;
            }
            buffer.append("]");
        }
        if (op.getPositionalVariable() != null) {
            buffer.append(",\n");
            addIndent(indent).append("\"position\":\"" + op.getPositionalVariable()+"\"");
        }
        buffer.append(",\n");
        addIndent(indent).append("\"expressions\":\""+op.getExpressionRef().getValue().accept(exprVisitor, indent).replace('"',' ')+"\"");
        return null;
    }

    @Override
    public Void visitLeftOuterUnnestOperator(LeftOuterUnnestOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\":\"outer-unnest\",\n");
        addIndent(indent).append("\"variables\":[\""+ op.getVariable()+"\"]");
        if (op.getPositionalVariable() != null) {
            buffer.append(",\n");
            addIndent(indent).append("\"position\":" + op.getPositionalVariable());
        }
        buffer.append(",\n");
        addIndent(indent).append("\"expressions\":\""+op.getExpressionRef().getValue().accept(exprVisitor, indent).replace('"',' ')+"\"");
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
        AlgebricksAppendable plan = addIndent(indent).append("\"operator\":\""+opSignature + "\"");
        if(op.getVariables().size() > 0) {
            addIndent(0).append(",\n");
            addIndent(indent).append("\"variables\":[");
            boolean first = true;
            for (LogicalVariable v : op.getVariables()){
                if(!first)
                    buffer.append(",");
                buffer.append("\""+str(v)+"\"");
                first=false;
            }
            buffer.append("]");
        }
        buffer.append(",\n");
        addIndent(indent).append("\"expressions\":\""+op.getExpressionRef().getValue().accept(exprVisitor, indent).replace('"',' ')+"\"");
        appendFilterInformation(plan, op.getMinFilterVars(), op.getMaxFilterVars(),indent);
        return null;
    }

    @Override
    public Void visitDataScanOperator(DataSourceScanOperator op, Integer indent) throws AlgebricksException {
        AlgebricksAppendable plan = addIndent(indent).append(
                "\"operator\":\"data-scan\"");
        if(op.getProjectVariables().size() > 0) {
            addIndent(0).append(",\n");
            addIndent(indent).append("\"project-variables\":[");
            boolean first = true;
            for (LogicalVariable v : op.getProjectVariables()){
                if(!first)
                    buffer.append(",");
                buffer.append("\""+str(v)+"\"");
                first=false;
            }
            buffer.append("]");
        }
        if(op.getVariables().size() > 0) {
            addIndent(0).append(",\n");
            addIndent(indent).append("\"variables\":[");
            boolean first = true;
            for (LogicalVariable v : op.getVariables()){
                if(!first)
                    buffer.append(",");
                buffer.append("\""+str(v)+"\"");
                first=false;
            }
            buffer.append("]");
            if(op.getDataSource()!=null){
                addIndent(0).append(",\n");
                addIndent(indent).append("\"data-source\":\""+op.getDataSource()+"\"");
            }
        }
        appendFilterInformation(plan, op.getMinFilterVars(), op.getMaxFilterVars(),indent);
        return null;
    }

    private Void appendFilterInformation(AlgebricksAppendable plan, List<LogicalVariable> minFilterVars,
                                         List<LogicalVariable> maxFilterVars, Integer indent) throws AlgebricksException {
        if (minFilterVars != null || maxFilterVars != null) {
            plan.append(",\n");
            addIndent(indent);
            plan.append("\"with-filter-on\":{");
        }
        if (minFilterVars != null) {
            plan.append("\"min\":[");
            boolean first = true;
            for (LogicalVariable v : minFilterVars){
                if(!first)
                    buffer.append(",");
                buffer.append("\""+str(v)+"\"");
                first=false;
            }
            buffer.append("]");
        }
        if(minFilterVars != null && maxFilterVars != null){
            buffer.append(",\n");
        }
        if (maxFilterVars != null) {
            plan.append("\"max\":[");
            boolean first = true;
            for (LogicalVariable v : maxFilterVars){
                if(!first)
                    buffer.append(",");
                buffer.append("\""+str(v)+"\"");
                first=false;
            }
            buffer.append("]");
        }
        if (minFilterVars != null || maxFilterVars != null) {
            plan.append("\n");
            addIndent(indent).append("}");
        }
        return null;
    }

    @Override
    public Void visitLimitOperator(LimitOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\":\"limit\",\n");
        addIndent(indent).append("\"value\":\""+op.getMaxObjects().getValue().accept(exprVisitor, indent)+"\"");
        ILogicalExpression offset = op.getOffset().getValue();
        if (offset != null) {
            buffer.append(",\n");
            addIndent(indent).append("\"offset\":\""+offset.accept(exprVisitor, indent)+"\"");
        }
        return null;
    }

    @Override
    public Void visitExchangeOperator(ExchangeOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\":\"exchange\"");
        return null;
    }

    @Override
    public Void visitScriptOperator(ScriptOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\":\"script\"");
        if(op.getInputVariables().size() > 0) {
            addIndent(0).append(",\n");
            addIndent(indent).append("\"in\":[");
            boolean first = true;
            for (LogicalVariable v : op.getInputVariables()){
                if(!first)
                    buffer.append(",");
                buffer.append("\""+str(v)+"\"");
                first=false;
            }
            buffer.append("]");
        }
        if(op.getOutputVariables().size() > 0) {
            addIndent(0).append(",\n");
            addIndent(indent).append("\"out\":[");
            boolean first = true;
            for (LogicalVariable v : op.getOutputVariables()){
                if(!first)
                    buffer.append(",");
                buffer.append("\""+str(v)+"\"");
                first=false;
            }
            buffer.append("]");
        }
        return null;
    }

    @Override
    public Void visitReplicateOperator(ReplicateOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\":\"replicate\"");
        return null;
    }

    @Override
    public Void visitSplitOperator(SplitOperator op, Integer indent) throws AlgebricksException {
        Mutable<ILogicalExpression> branchingExpression = op.getBranchingExpression();
        addIndent(indent).append("\"operator\":\"split\",\n");
        addIndent(indent).append("\""+branchingExpression.getValue().accept(exprVisitor, indent)+"\"");
        return null;
    }

    @Override
    public Void visitMaterializeOperator(MaterializeOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\":\"materialize\"");
        return null;
    }

    @Override
    public Void visitInsertDeleteUpsertOperator(InsertDeleteUpsertOperator op, Integer indent)
            throws AlgebricksException {
        String header = "\"operator\":\""+getIndexOpString(op.getOperation())+"\",\n";
        addIndent(indent).append(header);
        addIndent(indent).append(str("\"data-source\":\""+op.getDataSource()+"\",\n"));
        addIndent(indent).append("\"from-record\":\"")
                .append(op.getPayloadExpression().getValue().accept(exprVisitor, indent)+"\"");
        if (op.getAdditionalNonFilteringExpressions() != null) {
            buffer.append(",\n\"meta\":\"");
            pprintExprList(op.getAdditionalNonFilteringExpressions(), 0);
            buffer.append("\"");
        }
        buffer.append(",\n");
        addIndent(indent).append("\"partitioned-by\":{");
        pprintExprList(op.getPrimaryKeyExpressions(), 0);
        buffer.append("}");
        if (op.getOperation() == Kind.UPSERT) {
            addIndent(indent).append(",\n\"out\":{\n");
            addIndent(indent).append("\"record-before-upsert\":\"" + op.getBeforeOpRecordVar()+"\"");
            if((op.getBeforeOpAdditionalNonFilteringVars() != null)){
                buffer.append(",\n");
                addIndent(indent).append("\"additional-before-upsert\":\"" + op.getBeforeOpAdditionalNonFilteringVars()+"\"");
            }
            addIndent(indent).append("},\n");
        }
        if (op.isBulkload()) {
            addIndent(indent).append(",\n\"bulkload\":\"true\"");
        }
        return null;
    }

    @Override
    public Void visitIndexInsertDeleteUpsertOperator(IndexInsertDeleteUpsertOperator op, Integer indent)
            throws AlgebricksException {
        String header = getIndexOpString(op.getOperation());
        addIndent(indent).append("\"operator\":\""+header+"\",\n");
        addIndent(indent).append("\"index\":\""+op.getIndexName()+"\",\n");
        addIndent(indent).append("\"on\":\"")
                .append(str(op.getDataSourceIndex().getDataSource())+"\",\n");
        addIndent(indent).append("\"from\":{");

        if (op.getOperation() == Kind.UPSERT) {

            addIndent(indent).append("[\"replace\":\"");
            pprintExprList(op.getPrevSecondaryKeyExprs(), 0);
            buffer.append("\",\n");
            addIndent(indent).append("\"with\":\"");
            pprintExprList(op.getSecondaryKeyExpressions(), 0);
            buffer.append("\"}");
        } else {
            pprintExprList(op.getSecondaryKeyExpressions(), 0);
        }
        buffer.append("\n");
        addIndent(indent).append("}");
        if (op.isBulkload()) {
            buffer.append(",\n");
            buffer.append("\"bulkload\":\"true\"");
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
        addIndent(indent).append("\"operator\":\"tokenize\"");
        if(op.getTokenizeVars().size() > 0) {
            addIndent(0).append(",\n");
            addIndent(indent).append("\"variables\" :[");
            boolean first = true;
            for (LogicalVariable v : op.getTokenizeVars()){
                if(!first)
                    buffer.append(",");
                buffer.append("\""+str(v)+"\"");
                first=false;
            }
            buffer.append("]");
        }
        if(op.getSecondaryKeyExpressions().size()>0) {
            addIndent(0).append(",\n");
            pprintExprList(op.getSecondaryKeyExpressions(), indent);
        }
        return null;
    }

    @Override
    public Void visitSinkOperator(SinkOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\":\"sink\"");
        return null;
    }

    @Override
    public Void visitDelegateOperator(DelegateOperator op, Integer indent) throws AlgebricksException {
        addIndent(indent).append("\"operator\":\""+op.toString()+"\"");
        return null;
    }

    protected AlgebricksAppendable addIndent(int level) throws AlgebricksException {
        for (int i = 0; i < level; ++i) {
            buffer.append(' ');
        }
        return buffer;
    }

    protected void printNestedPlans(AbstractOperatorWithNestedPlans op, Integer indent) throws AlgebricksException {
        PlanPrettyPrinter.reset(op);
        buffer.append("[\n");
        boolean first=true;
        for (ILogicalPlan p : op.getNestedPlans()) {
            if(!first)
                buffer.append(",");
            PlanPrettyPrinter.printPlanJson(p, this, indent+4);
            first=false;

        }
        addIndent(indent).append("]");
    }
    //Done--Look for exprRef
    protected void pprintExprList(List<Mutable<ILogicalExpression>> expressions, Integer indent)
            throws AlgebricksException {
        addIndent(indent);
        buffer.append("\"expressions\":\"");
        boolean first = true;
        for (Mutable<ILogicalExpression> exprRef : expressions) {
            if (first) {
                first = false;
            } else {
                buffer.append(", ");
            }
            buffer.append(exprRef.getValue().accept(exprVisitor, indent).replace('"',' '));
        }
        buffer.append("\"");
    }

    protected void pprintVeList(List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> vePairList, Integer indent)
            throws AlgebricksException {
        buffer.append("[");
        boolean fst = true;
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> ve : vePairList) {
            if (fst) {
                fst = false;
            } else {
                buffer.append(",");
            }
            if (ve.first != null) {
                buffer.append("{\"variable\":\""+ve.first.toString().replace('"',' ') + "\"," +"\"expression\":\"" + ve.second.toString().replace('"',' ')+"\"}");
            } else {
                buffer.append("{\"expression\":\""+ve.second.getValue().accept(exprVisitor, indent).replace('"',' ')+"\"}");
            }
        }
        buffer.append("]");
    }

}
