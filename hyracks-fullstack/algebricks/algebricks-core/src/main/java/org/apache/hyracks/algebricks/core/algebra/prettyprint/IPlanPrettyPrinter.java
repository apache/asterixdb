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

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Note: Some implementations may be stateful and not thread-safe.
 */
public interface IPlanPrettyPrinter {

    /** Prints the plan rooted at the operator argument. */
    default IPlanPrettyPrinter printOperator(AbstractLogicalOperator operator) throws AlgebricksException {
        return printOperator(operator, true, false);
    }

    /** Prints given operator and optionally it's inputs */
    IPlanPrettyPrinter printOperator(AbstractLogicalOperator operator, boolean printInputs,
            boolean printOptimizerEstimates) throws AlgebricksException;

    /** Prints given expression */
    IPlanPrettyPrinter printExpression(ILogicalExpression expression) throws AlgebricksException;

    /** Prints the whole logical plan. */
    IPlanPrettyPrinter printPlan(ILogicalPlan plan, boolean printOptimizerEstimates) throws AlgebricksException;

    /** Prints the logical plan, annotated with physical operator and connector ids */
    IPlanPrettyPrinter printPlan(ILogicalPlan plan, Map<Object, String> log2phys, boolean printOptimizerEstimates)
            throws AlgebricksException;

    /** Prints the logical plan, annotated with physical operator and connector ids, and profiling info*/
    IPlanPrettyPrinter printPlan(ILogicalPlan plan, Map<Object, String> log2phys, boolean printOptimizerEstimates,
            ObjectNode profile) throws AlgebricksException;

    /** Resets the state of the pretty printer. */
    IPlanPrettyPrinter reset() throws AlgebricksException;

    /** @return the string of the printed plan. */
    String toString();
}
