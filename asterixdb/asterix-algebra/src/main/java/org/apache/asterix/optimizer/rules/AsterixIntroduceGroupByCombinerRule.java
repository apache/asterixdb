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
package org.apache.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.rewriter.rules.AbstractIntroduceGroupByCombinerRule;

public class AsterixIntroduceGroupByCombinerRule extends AbstractIntroduceGroupByCombinerRule {

    @SuppressWarnings("unchecked")
    @Override
    protected void processNullTest(IOptimizationContext context, GroupByOperator nestedGby,
            List<LogicalVariable> aggregateVarsProducedByCombiner) {
        IFunctionInfo finfoEq = context.getMetadataProvider().lookupFunction(AsterixBuiltinFunctions.IS_SYSTEM_NULL);
        SelectOperator selectNonSystemNull;

        if (aggregateVarsProducedByCombiner.size() == 1) {
            ILogicalExpression isSystemNullTest = new ScalarFunctionCallExpression(finfoEq,
                    new MutableObject<ILogicalExpression>(new VariableReferenceExpression(
                            aggregateVarsProducedByCombiner.get(0))));
            IFunctionInfo finfoNot = context.getMetadataProvider().lookupFunction(AlgebricksBuiltinFunctions.NOT);
            ScalarFunctionCallExpression nonSystemNullTest = new ScalarFunctionCallExpression(finfoNot,
                    new MutableObject<ILogicalExpression>(isSystemNullTest));
            selectNonSystemNull = new SelectOperator(new MutableObject<ILogicalExpression>(nonSystemNullTest), false,
                    null);
        } else {
            List<Mutable<ILogicalExpression>> isSystemNullTestList = new ArrayList<Mutable<ILogicalExpression>>();
            for (LogicalVariable aggVar : aggregateVarsProducedByCombiner) {
                ILogicalExpression isSystemNullTest = new ScalarFunctionCallExpression(finfoEq,
                        new MutableObject<ILogicalExpression>(new VariableReferenceExpression(aggVar)));
                IFunctionInfo finfoNot = context.getMetadataProvider().lookupFunction(AlgebricksBuiltinFunctions.NOT);
                ScalarFunctionCallExpression nonSystemNullTest = new ScalarFunctionCallExpression(finfoNot,
                        new MutableObject<ILogicalExpression>(isSystemNullTest));
                isSystemNullTestList.add(new MutableObject<ILogicalExpression>(nonSystemNullTest));
            }
            IFunctionInfo finfoAnd = context.getMetadataProvider().lookupFunction(AlgebricksBuiltinFunctions.AND);
            selectNonSystemNull = new SelectOperator(new MutableObject<ILogicalExpression>(
                    new ScalarFunctionCallExpression(finfoAnd, isSystemNullTestList)), false, null);
        }

        //add the not-system-null check into the nested pipeline
        Mutable<ILogicalOperator> ntsBeforeNestedGby = nestedGby.getInputs().get(0);
        nestedGby.getInputs().set(0, new MutableObject<ILogicalOperator>(selectNonSystemNull));
        selectNonSystemNull.getInputs().add(ntsBeforeNestedGby);
    }
}
