/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.AbstractIntroduceGroupByCombinerRule;

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
