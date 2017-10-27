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

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;

public class PlanPrettyPrinter {
    @FunctionalInterface
    public interface print<T1, T2, T3> {
        void apply(T1 arg1, T2 arg2, T3 arg3) throws AlgebricksException;
    }

    public static void printOperator(AbstractLogicalOperator op, LogicalOperatorPrettyPrintVisitor pvisitor,
            int indent) throws AlgebricksException {
        print<AbstractLogicalOperator, LogicalOperatorPrettyPrintVisitor, Integer> printOperator =
                LogicalOperatorPrettyPrintVisitor::printOperator;
        printOperator.apply(op, pvisitor, indent);
    }

    public static <T extends AbstractLogicalOperatorPrettyPrintVisitor> void printPlan(ILogicalPlan plan,
            T pvisitor, int indent) throws AlgebricksException {
        if (pvisitor.getClass().equals(LogicalOperatorPrettyPrintVisitor.class)) {
            print<ILogicalPlan, LogicalOperatorPrettyPrintVisitor, Integer> printPlan =
                    LogicalOperatorPrettyPrintVisitor::printPlan;
            printPlan.apply(plan,(LogicalOperatorPrettyPrintVisitor) pvisitor, indent);
        }
        else if (pvisitor.getClass().equals(LogicalOperatorPrettyPrintVisitorJson.class)) {
            print<ILogicalPlan, LogicalOperatorPrettyPrintVisitorJson, Integer> printPlan =
                    LogicalOperatorPrettyPrintVisitorJson::printPlanJson;
            printPlan.apply(plan, (LogicalOperatorPrettyPrintVisitorJson)pvisitor, indent);
        }

    }

    public static void printPhysicalOps(ILogicalPlan plan, AlgebricksAppendable out, int indent)
            throws AlgebricksException {
        print<ILogicalPlan, AlgebricksAppendable, Integer> printOperator =
                AbstractLogicalOperatorPrettyPrintVisitor::printPhysicalOps;
        printOperator.apply(plan, out, indent);
    }
}
