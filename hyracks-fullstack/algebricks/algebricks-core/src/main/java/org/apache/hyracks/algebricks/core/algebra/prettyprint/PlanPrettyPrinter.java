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
    public static int operatorID=0;
    public static void printPlanJson(ILogicalPlan plan, LogicalOperatorPrettyPrintVisitorJson pvisitor, int indent)
            throws AlgebricksException {
        for (Mutable<ILogicalOperator> root : plan.getRoots()) {
            printOperatorJson((AbstractLogicalOperator) root.getValue(), pvisitor, indent);
        }
    }
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
    public static void printOperatorJson(AbstractLogicalOperator op, LogicalOperatorPrettyPrintVisitorJson pvisitor, int indent)
            throws AlgebricksException {
        final AlgebricksAppendable out = pvisitor.get();
        pad(out, indent);
        // append(out,"{\"Operator\":\"");
        appendln(out,"{");
        op.accept(pvisitor, indent+1);
        indent++;
        appendln(out,",");
        pad(out, indent);
        if(op.getOperatorID()==0){
            op.setOperatorID(operatorID);
            operatorID++;
        }
        append(out,"\"operator-id\":"+op.getOperatorID());

        IPhysicalOperator pOp = op.getPhysicalOperator();
        if(pOp!=null){
            appendln(out,",");
            pad(out, indent);
            String pOperator = "\"physical-operator\":\"" + pOp.toString() + "\"";
            append(out,pOperator);
        }
        appendln(out,",");
        pad(out,indent);
        append(out,"\"execution-mode\":\"" + op.getExecutionMode()+'"');
        if (op.getInputs().size()>0) {
            appendln(out,",");
            pad(out,indent);
            appendln(out, "\"inputs\":[");
            boolean  moreInputes = false;
            for (Mutable<ILogicalOperator> k : op.getInputs()) {
                if (moreInputes) {
                    append(out, ",");
                }
                printOperatorJson((AbstractLogicalOperator) k.getValue(), pvisitor, indent +10);
                moreInputes = true;
            }
            pad(out, indent+9);
            appendln(out, "]");
        }
        out.append("\n");
        pad(out, indent-1);
        appendln(out,"}");
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
    private static void append(AlgebricksAppendable buf, String s) throws AlgebricksException {
        buf.append(s);
    }

    private static void pad(AlgebricksAppendable buf, int indent) throws AlgebricksException {
        for (int i = 0; i < indent; ++i) {
            buf.append(' ');
        }
    }
    public static void resetOperatorID(ILogicalPlan plan){
        for (Mutable<ILogicalOperator> root : plan.getRoots()) {
            AbstractLogicalOperator op = (AbstractLogicalOperator) root.getValue();
            reset(op);
        }
    }
    private static void reset(AbstractLogicalOperator op){
        op.setOperatorID(0);
        for (Mutable<ILogicalOperator> k : op.getInputs()) {
            reset((AbstractLogicalOperator) k.getValue());
        }
    }
}
