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

public abstract class AbstractLogicalOperatorPrettyPrintVisitor implements ILogicalOperatorVisitor<Void, Integer> {
    ILogicalExpressionVisitor<String, Integer> exprVisitor;
    AlgebricksAppendable buffer;

    public AbstractLogicalOperatorPrettyPrintVisitor() {
        this(new AlgebricksAppendable());
    }

    public AbstractLogicalOperatorPrettyPrintVisitor(Appendable app) {
        this(new AlgebricksAppendable(app), new LogicalExpressionPrettyPrintVisitor());
    }

    public AbstractLogicalOperatorPrettyPrintVisitor(AlgebricksAppendable buffer) {
        this(buffer, new LogicalExpressionPrettyPrintVisitor());
    }

    public AbstractLogicalOperatorPrettyPrintVisitor(AlgebricksAppendable buffer,
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

    protected static void appendln(AlgebricksAppendable buf, String s) throws AlgebricksException {
        buf.append(s);
        buf.append("\n");
    }

    protected static void append(AlgebricksAppendable buf, String s) throws AlgebricksException {
        buf.append(s);
    }

    protected static void pad(AlgebricksAppendable buf, int indent) throws AlgebricksException {
        for (int i = 0; i < indent; ++i) {
            buf.append(' ');
        }
    }

    protected AlgebricksAppendable addIndent(int level) throws AlgebricksException {
        for (int i = 0; i < level; ++i) {
            buffer.append(' ');
        }
        return buffer;
    }

    public void printPlan(ILogicalPlan plan, int indent) throws AlgebricksException {
        for (Mutable<ILogicalOperator> root : plan.getRoots()) {
            printOperator((AbstractLogicalOperator) root.getValue(), indent);
        }
    }

    public abstract void printOperator(AbstractLogicalOperator op, int indent) throws AlgebricksException;

    public static void printPhysicalOperator(AbstractLogicalOperator op, int indent, AlgebricksAppendable out)
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

    public static void printPhysicalOps(ILogicalPlan plan, AlgebricksAppendable out, int indent)
            throws AlgebricksException {
        for (Mutable<ILogicalOperator> root : plan.getRoots()) {
            printPhysicalOperator((AbstractLogicalOperator) root.getValue(), indent, out);
        }
    }
}
