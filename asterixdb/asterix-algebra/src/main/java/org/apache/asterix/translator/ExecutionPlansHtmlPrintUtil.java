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
package org.apache.asterix.translator;

import java.io.PrintWriter;

public class ExecutionPlansHtmlPrintUtil {

    private static final String LOGICAL_PLAN_LBL = "Logical plan";
    private static final String EXPRESSION_TREE_LBL = "Expression tree";
    private static final String REWRITTEN_EXPRESSION_TREE_LBL = "Rewritten expression tree";
    private static final String OPTIMIZED_LOGICAL_PLAN_LBL = "Optimized logical plan";
    private static final String JOB_LBL = "Job";

    private ExecutionPlansHtmlPrintUtil() {
    }

    public static void print(PrintWriter output, ExecutionPlans plans) {
        printNonNull(output, EXPRESSION_TREE_LBL, plans.getExpressionTree());
        printNonNull(output, REWRITTEN_EXPRESSION_TREE_LBL, plans.getRewrittenExpressionTree());
        printNonNull(output, LOGICAL_PLAN_LBL, plans.getLogicalPlan());
        printNonNull(output, OPTIMIZED_LOGICAL_PLAN_LBL, plans.getOptimizedLogicalPlan());
        printNonNull(output, JOB_LBL, plans.getJob());
    }

    private static void printNonNull(PrintWriter output, String lbl, String value) {
        if (value != null) {
            printFieldPrefix(output, lbl);
            output.print(value);
            printFieldPostfix(output);
        }
    }

    private static void printFieldPrefix(PrintWriter output, String lbl) {
        output.println();
        output.println("<h4>" + lbl + ":</h4>");
        switch (lbl) {
            case LOGICAL_PLAN_LBL:
                output.println("<pre class=query-plan>");
                break;
            case OPTIMIZED_LOGICAL_PLAN_LBL:
                output.println("<pre class=query-optimized-plan>");
                break;
            default:
                output.println("<pre>");
                break;
        }
    }

    private static void printFieldPostfix(PrintWriter output) {
        output.println("</pre>");
    }
}
