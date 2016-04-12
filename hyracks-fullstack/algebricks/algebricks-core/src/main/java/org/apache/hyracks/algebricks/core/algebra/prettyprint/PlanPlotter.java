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

import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.mutable.Mutable;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;

public class PlanPlotter {

    static Random randomGenerator = new Random();

    public static void printLogicalPlan(ILogicalPlan plan) throws AlgebricksException {
        int indent = 5;
        StringBuilder out = new StringBuilder();
        int randomInt = 10000 + randomGenerator.nextInt(100);
        appendln(out, "digraph G {");
        for (Mutable<ILogicalOperator> root : plan.getRoots()) {
            printVisualizationGraph((AbstractLogicalOperator) root.getValue(), indent, out, "", randomInt);
        }
        appendln(out, "\n}\n}");
        try {
            File file = File.createTempFile("logicalPlan", ".txt");
            FileUtils.writeStringToFile(file, out.toString());
            file.deleteOnExit();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void printOptimizedLogicalPlan(ILogicalPlan plan) throws AlgebricksException {
        int indent = 5;
        StringBuilder out = new StringBuilder();
        int randomInt = 10000 + randomGenerator.nextInt(100);
        appendln(out, "digraph G {");
        for (Mutable<ILogicalOperator> root : plan.getRoots()) {
            printVisualizationGraph((AbstractLogicalOperator) root.getValue(), indent, out, "", randomInt);
        }
        appendln(out, "\n}\n}");
        try {
            File file = File.createTempFile("logicalOptimizedPlan", ".txt");
            FileUtils.writeStringToFile(file, out.toString());
            file.deleteOnExit();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*
     * DFS traversal function. Calls iteratively all children, and for each calls itself recursively
     * Includes slim-maps only (no gathering of mappers to one)
     */
    public static void printVisualizationGraph(AbstractLogicalOperator op, int indent, StringBuilder out,
            String current_supernode_name, int randomInt) {
        if (!op.getInputs().isEmpty()) {
            //String stringToVisualize = op.toStringForVisualizationGraph();
            String stringToVisualize = op.getOperatorTag().name();
            int firstOccurenceOf_ = stringToVisualize.indexOf("_");
            String supernode_current = stringToVisualize.substring(firstOccurenceOf_ + 1, stringToVisualize.length());
            if (current_supernode_name.isEmpty()) {
                current_supernode_name = supernode_current;
                appendln(out, new String("subgraph cluster_" + supernode_current + " {"));
                pad(out, indent);
                appendln(out, new String("node [style=filled, color = lightgray];"));
                pad(out, indent);
                appendln(out, new String("color=blue;"));
                pad(out, indent);
                String op_id = op.toString().substring(op.toString().indexOf("@") + 1, op.toString().length());
                appendln(out, new String("label = \"" + supernode_current + "ID" + op_id + "\";"));
                pad(out, indent);
            }

            for (Mutable<ILogicalOperator> i : op.getInputs()) {
                String op_id = i.toString().substring(i.toString().indexOf("@") + 1, i.toString().length());
                String logOpStr = ((AbstractLogicalOperator) i.getValue()).getOperatorTag().name() + "ID" + op_id;
                firstOccurenceOf_ = logOpStr.indexOf("_");
                String supernode_child = logOpStr.substring(firstOccurenceOf_ + 1, logOpStr.length());
                if (!supernode_current.equals(supernode_child)) {
                    appendln(out, new String("node [style=filled, color = lightgray];"));
                    pad(out, indent);
                    appendln(out, new String("color=blue"));
                    pad(out, indent);
                    appendln(out, new String("label = \"" + supernode_child + "\";"));
                    pad(out, indent);
                }

                op_id = op.toString().substring(op.toString().indexOf("@") + 1, op.toString().length());
                appendln(out, stringToVisualize + "ID" + op_id + "[style = filled]");
                AbstractLogicalOperator child = (AbstractLogicalOperator) i.getValue();

                pad(out, indent);
                String op_id1 = op.toString().substring(op.toString().indexOf("@") + 1, op.toString().length());
                append(out, op.getOperatorTag().name() + "ID" + op_id1 + " -> ");
                String opc_id = child.toString()
                        .substring(child.toString().indexOf("@") + 1, child.toString().length());
                appendln(out, child.getOperatorTag().name() + "ID" + opc_id);

                printVisualizationGraph(child, indent, out, supernode_current, (randomGenerator.nextInt(100) + 10000));

            }
        }

    }

    private static void appendln(StringBuilder buf, String s) {
        buf.append(s);
        buf.append("\n");
    }

    private static void append(StringBuilder buf, String s) {
        buf.append(s);
    }

    private static void pad(StringBuilder buf, int indent) {
        for (int i = 0; i < indent; ++i) {
            buf.append(' ');
        }
    }
}
