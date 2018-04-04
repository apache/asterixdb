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

import static org.apache.asterix.translator.SessionConfig.PlanFormat.STRING;

import org.apache.hyracks.util.JSONUtil;

public class ExecutionPlansJsonPrintUtil {

    private static final String LOGICAL_PLAN_LBL = "logicalPlan";
    private static final String EXPRESSION_TREE_LBL = "expressionTree";
    private static final String REWRITTEN_EXPRESSION_TREE_LBL = "rewrittenExpressionTree";
    private static final String OPTIMIZED_LOGICAL_PLAN_LBL = "optimizedLogicalPlan";
    private static final String JOB_LBL = "job";

    private ExecutionPlansJsonPrintUtil() {
    }

    public static String asJson(ExecutionPlans plans, SessionConfig.PlanFormat format) {
        final StringBuilder output = new StringBuilder();
        appendOutputPrefix(output);
        // TODO only string is currently supported for expression trees
        appendNonNull(output, EXPRESSION_TREE_LBL, plans.getExpressionTree(), STRING);
        appendNonNull(output, REWRITTEN_EXPRESSION_TREE_LBL, plans.getRewrittenExpressionTree(), STRING);
        appendNonNull(output, LOGICAL_PLAN_LBL, plans.getLogicalPlan(), format);
        appendNonNull(output, OPTIMIZED_LOGICAL_PLAN_LBL, plans.getOptimizedLogicalPlan(), format);
        appendNonNull(output, JOB_LBL, plans.getJob(), format);
        appendOutputPostfix(output);
        return output.toString();
    }

    private static void appendNonNull(StringBuilder builder, String lbl, String value,
            SessionConfig.PlanFormat format) {
        if (value != null) {
            printFieldPrefix(builder, lbl);
            switch (format) {
                case JSON:
                    builder.append(value);
                    break;
                case STRING:
                    JSONUtil.quoteAndEscape(builder, value);
                    break;
                default:
                    throw new IllegalStateException("Unrecognized plan format: " + format);
            }
            printFieldPostfix(builder);
        }
    }

    private static void appendOutputPrefix(StringBuilder builder) {
        builder.append("{");
    }

    private static void printFieldPrefix(StringBuilder builder, String lbl) {
        builder.append("\"" + lbl + "\": ");
    }

    private static void printFieldPostfix(StringBuilder builder) {
        builder.append(",");
    }

    private static void appendOutputPostfix(StringBuilder builder) {
        // remove extra comma if needed
        if (builder.length() > 1) {
            builder.deleteCharAt(builder.length() - 1);
        }
        builder.append("}");
    }
}
