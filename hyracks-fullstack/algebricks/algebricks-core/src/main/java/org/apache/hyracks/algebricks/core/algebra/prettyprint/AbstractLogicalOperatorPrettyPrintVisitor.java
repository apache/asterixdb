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
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IPhysicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionVisitor;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public abstract class AbstractLogicalOperatorPrettyPrintVisitor<T> implements ILogicalOperatorVisitor<Void, T> {

    protected final ILogicalExpressionVisitor<String, T> exprVisitor;
    protected final AlgebricksStringBuilderWriter buffer;

    public AbstractLogicalOperatorPrettyPrintVisitor(ILogicalExpressionVisitor<String, T> exprVisitor) {
        this.buffer = new AlgebricksStringBuilderWriter(PlanPrettyPrinter.INIT_SIZE);
        this.exprVisitor = exprVisitor;
    }

    public static void printPhysicalOps(ILogicalPlan plan, AlgebricksStringBuilderWriter out, int indent)
            throws AlgebricksException {
        for (Mutable<ILogicalOperator> root : plan.getRoots()) {
            printPhysicalOperator((AbstractLogicalOperator) root.getValue(), indent, out);
        }
    }

    protected void resetState() {
        buffer.getBuilder().setLength(0);
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

    private static void printPhysicalOperator(AbstractLogicalOperator op, int indent, AlgebricksStringBuilderWriter out)
            throws AlgebricksException {
        IPhysicalOperator pOp = op.getPhysicalOperator();
        pad(out, indent);
        appendln(out, "-- " + pOp.toString() + "  |" + op.getExecutionMode() + "|");
        if (op.hasNestedPlans()) {
            AbstractOperatorWithNestedPlans opNest = (AbstractOperatorWithNestedPlans) op;
            for (ILogicalPlan p : opNest.getNestedPlans()) {
                pad(out, indent + 8);
                appendln(out, "{");
                printPhysicalOps(p, out, indent + 10);
                pad(out, indent + 8);
                appendln(out, "}");
            }
        }
        for (Mutable<ILogicalOperator> i : op.getInputs()) {
            printPhysicalOperator((AbstractLogicalOperator) i.getValue(), indent + 2, out);
        }
    }
}
