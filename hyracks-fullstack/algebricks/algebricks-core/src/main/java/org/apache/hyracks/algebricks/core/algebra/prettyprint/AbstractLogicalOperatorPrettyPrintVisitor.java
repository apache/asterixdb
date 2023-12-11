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

import java.util.Map;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IPhysicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.OperatorAnnotations;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionVisitor;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public abstract class AbstractLogicalOperatorPrettyPrintVisitor<T> implements ILogicalOperatorVisitor<Void, T> {

    protected static final String CARDINALITY = "cardinality";
    protected static final String OP_COST_LOCAL = "op-cost";
    protected static final String OP_COST_TOTAL = "total-cost";
    protected final ILogicalExpressionVisitor<String, T> exprVisitor;
    protected final AlgebricksStringBuilderWriter buffer;

    public AbstractLogicalOperatorPrettyPrintVisitor(ILogicalExpressionVisitor<String, T> exprVisitor) {
        this.buffer = new AlgebricksStringBuilderWriter(PlanPrettyPrinter.INIT_SIZE);
        this.exprVisitor = exprVisitor;
    }

    public static void printPhysicalOps(ILogicalPlan plan, AlgebricksStringBuilderWriter out, int indent,
            boolean verbose) throws AlgebricksException {
        for (Mutable<ILogicalOperator> root : plan.getRoots()) {
            printPhysicalOperator((AbstractLogicalOperator) root.getValue(), indent, out, verbose);
        }
    }

    protected void resetState() {
        buffer.getBuilder().setLength(0);
    }

    protected double getPlanCardinality(ILogicalOperator op) {
        Double planCard = null;
        if (op.getOperatorTag() == LogicalOperatorTag.DISTRIBUTE_RESULT) {
            planCard = (Double) getAnnotationValue(op, OperatorAnnotations.OP_OUTPUT_CARDINALITY);
        }
        return (planCard != null) ? planCard : 0.0;
    }

    protected double getPlanCost(ILogicalOperator op) {
        Double planCost = null;
        if (op.getOperatorTag() == LogicalOperatorTag.DISTRIBUTE_RESULT) {
            planCost = (Double) getAnnotationValue(op, OperatorAnnotations.OP_COST_TOTAL);
        }
        return (planCost != null) ? planCost : 0.0;
    }

    protected double getOpCardinality(ILogicalOperator op) {
        Double opCard;

        if (op.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
            if (((DataSourceScanOperator) op).getSelectCondition() != null) {
                opCard = (Double) getAnnotationValue(op, OperatorAnnotations.OP_OUTPUT_CARDINALITY);
            } else {
                opCard = (Double) getAnnotationValue(op, OperatorAnnotations.OP_INPUT_CARDINALITY);
            }
        } else {
            opCard = (Double) getAnnotationValue(op, OperatorAnnotations.OP_OUTPUT_CARDINALITY);
        }

        return (opCard != null) ? opCard : 0.0;
    }

    protected double getOpLocalCost(ILogicalOperator op) {
        Double opLocalCost = (Double) getAnnotationValue(op, OperatorAnnotations.OP_COST_LOCAL);
        return (opLocalCost != null) ? opLocalCost : 0.0;
    }

    protected double getOpTotalCost(ILogicalOperator op) {
        Double opTotalCost = (Double) getAnnotationValue(op, OperatorAnnotations.OP_COST_TOTAL);
        return (opTotalCost != null) ? opTotalCost : 0.0;
    }

    protected Object getAnnotationValue(ILogicalOperator op, String key) {
        Map<String, Object> annotations = op.getAnnotations();
        if (annotations != null && annotations.containsKey(key)) {
            return annotations.get(key);
        }
        return null;
    }

    @Override
    public String toString() {
        return buffer.toString();
    }

    String str(Object o) {
        return String.valueOf(o);
    }

    protected static void appendln(AlgebricksStringBuilderWriter buf, String s) {
        buf.append(s);
        buf.append("\n");
    }

    protected static void append(AlgebricksStringBuilderWriter buf, String s) {
        buf.append(s);
    }

    protected static void pad(AlgebricksStringBuilderWriter buf, int indent) {
        for (int i = 0; i < indent; ++i) {
            buf.append(' ');
        }
    }

    protected AlgebricksStringBuilderWriter addIndent(int level) {
        for (int i = 0; i < level; ++i) {
            buffer.append(' ');
        }
        return buffer;
    }

    private static void printPhysicalOperator(AbstractLogicalOperator op, int indent, AlgebricksStringBuilderWriter out,
            boolean verbose) throws AlgebricksException {
        IPhysicalOperator pOp = op.getPhysicalOperator();
        pad(out, indent);
        appendln(out, "-- " + pOp.toString(verbose) + "  |" + op.getExecutionMode() + "|");
        if (op.hasNestedPlans()) {
            AbstractOperatorWithNestedPlans opNest = (AbstractOperatorWithNestedPlans) op;
            for (ILogicalPlan p : opNest.getNestedPlans()) {
                pad(out, indent + 8);
                appendln(out, "{");
                printPhysicalOps(p, out, indent + 10, verbose);
                pad(out, indent + 8);
                appendln(out, "}");
            }
        }
        for (Mutable<ILogicalOperator> i : op.getInputs()) {
            printPhysicalOperator((AbstractLogicalOperator) i.getValue(), indent + 2, out, verbose);
        }
    }
}
