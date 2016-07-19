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

public class PlanPrettyPrinter {
    public static void printPlan(ILogicalPlan plan, LogicalOperatorPrettyPrintVisitor pvisitor, int indent)
            throws AlgebricksException {
        for (Mutable<ILogicalOperator> root : plan.getRoots()) {
            printOperator((AbstractLogicalOperator) root.getValue(), pvisitor, indent);
        }
    }

    public static void printPhysicalOps(ILogicalPlan plan, AlgebricksAppendable out, int indent)
            throws AlgebricksException {
        for (Mutable<ILogicalOperator> root : plan.getRoots()) {
            printPhysicalOperator((AbstractLogicalOperator) root.getValue(), indent, out);
        }
    }

    public static void printOperator(AbstractLogicalOperator op, LogicalOperatorPrettyPrintVisitor pvisitor, int indent)
            throws AlgebricksException {
        final AlgebricksAppendable out = pvisitor.get();
        op.accept(pvisitor, indent);
        IPhysicalOperator pOp = op.getPhysicalOperator();

        if (pOp != null) {
            out.append("\n");
            pad(out, indent);
            appendln(out, "-- " + pOp.toString() + "  |" + op.getExecutionMode() + "|");
        } else {
            appendln(out, " -- |" + op.getExecutionMode() + "|");
        }

        for (Mutable<ILogicalOperator> i : op.getInputs()) {
            printOperator((AbstractLogicalOperator) i.getValue(), pvisitor, indent + 2);
        }
    }

    private static void printPhysicalOperator(AbstractLogicalOperator op, int indent, AlgebricksAppendable out)
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

    private static void appendln(AlgebricksAppendable buf, String s) throws AlgebricksException {
        buf.append(s);
        buf.append("\n");
    }

    private static void pad(AlgebricksAppendable buf, int indent) throws AlgebricksException {
        for (int i = 0; i < indent; ++i) {
            buf.append(' ');
        }
    }
}
