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

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
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
import org.apache.hyracks.api.exceptions.SourceLocation;

public class AsterixIntroduceGroupByCombinerRule extends AbstractIntroduceGroupByCombinerRule {

    @SuppressWarnings("unchecked")
    @Override
    protected void processNullTest(IOptimizationContext context, GroupByOperator nestedGby,
            List<LogicalVariable> aggregateVarsProducedByCombiner) {
        SourceLocation sourceLoc = nestedGby.getSourceLocation();
        IFunctionInfo finfoEq = context.getMetadataProvider().lookupFunction(BuiltinFunctions.IS_SYSTEM_NULL);
        SelectOperator selectNonSystemNull;

        if (aggregateVarsProducedByCombiner.size() == 1) {
            VariableReferenceExpression aggVarRef =
                    new VariableReferenceExpression(aggregateVarsProducedByCombiner.get(0));
            aggVarRef.setSourceLocation(sourceLoc);
            ScalarFunctionCallExpression isSystemNullTest =
                    new ScalarFunctionCallExpression(finfoEq, new MutableObject<>(aggVarRef));
            isSystemNullTest.setSourceLocation(sourceLoc);
            IFunctionInfo finfoNot = context.getMetadataProvider().lookupFunction(AlgebricksBuiltinFunctions.NOT);
            ScalarFunctionCallExpression nonSystemNullTest =
                    new ScalarFunctionCallExpression(finfoNot, new MutableObject<>(isSystemNullTest));
            nonSystemNullTest.setSourceLocation(sourceLoc);
            selectNonSystemNull = new SelectOperator(new MutableObject<>(nonSystemNullTest), false, null);
            selectNonSystemNull.setSourceLocation(sourceLoc);
        } else {
            List<Mutable<ILogicalExpression>> isSystemNullTestList = new ArrayList<>();
            for (LogicalVariable aggVar : aggregateVarsProducedByCombiner) {
                VariableReferenceExpression aggVarRef = new VariableReferenceExpression(aggVar);
                aggVarRef.setSourceLocation(sourceLoc);
                ScalarFunctionCallExpression isSystemNullTest =
                        new ScalarFunctionCallExpression(finfoEq, new MutableObject<>(aggVarRef));
                isSystemNullTest.setSourceLocation(sourceLoc);
                IFunctionInfo finfoNot = context.getMetadataProvider().lookupFunction(AlgebricksBuiltinFunctions.NOT);
                ScalarFunctionCallExpression nonSystemNullTest =
                        new ScalarFunctionCallExpression(finfoNot, new MutableObject<>(isSystemNullTest));
                nonSystemNullTest.setSourceLocation(sourceLoc);
                isSystemNullTestList.add(new MutableObject<>(nonSystemNullTest));
            }
            IFunctionInfo finfoAnd = context.getMetadataProvider().lookupFunction(AlgebricksBuiltinFunctions.AND);
            selectNonSystemNull = new SelectOperator(
                    new MutableObject<>(new ScalarFunctionCallExpression(finfoAnd, isSystemNullTestList)), false, null);
            selectNonSystemNull.setSourceLocation(sourceLoc);
        }

        //add the not-system-null check into the nested pipeline
        Mutable<ILogicalOperator> ntsBeforeNestedGby = nestedGby.getInputs().get(0);
        nestedGby.getInputs().set(0, new MutableObject<>(selectNonSystemNull));
        selectNonSystemNull.getInputs().add(ntsBeforeNestedGby);
    }
}
