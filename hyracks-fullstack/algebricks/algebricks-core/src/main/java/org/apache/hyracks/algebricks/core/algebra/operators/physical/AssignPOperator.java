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
package org.apache.hyracks.algebricks.core.algebra.operators.physical;

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionRuntimeProvider;
import org.apache.hyracks.algebricks.core.algebra.operators.AbstractAssignPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.operators.std.AssignRuntimeFactory;

public class AssignPOperator extends AbstractAssignPOperator {

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.ASSIGN;
    }

    protected IPushRuntimeFactory createRuntimeFactory(JobGenContext context, AssignOperator op,
            IOperatorSchema opSchema, IOperatorSchema[] inputSchemas, int[] outColumns, int[] projectionList)
            throws AlgebricksException {
        List<Mutable<ILogicalExpression>> expressions = op.getExpressions();
        IScalarEvaluatorFactory[] evalFactories = new IScalarEvaluatorFactory[expressions.size()];
        IExpressionRuntimeProvider expressionRuntimeProvider = context.getExpressionRuntimeProvider();
        for (int i = 0; i < evalFactories.length; i++) {
            evalFactories[i] = expressionRuntimeProvider.createEvaluatorFactory(expressions.get(i).getValue(),
                    context.getTypeEnvironment(op.getInputs().get(0).getValue()), inputSchemas, context);
        }
        return new AssignRuntimeFactory(outColumns, evalFactories, projectionList, flushFramesRapidly);
    }
}
