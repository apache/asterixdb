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
package org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSource;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;

public class IsomorphismUtilities {

    public static void mapVariablesTopDown(ILogicalOperator op, ILogicalOperator arg,
            Map<LogicalVariable, LogicalVariable> variableMapping) throws AlgebricksException {
        IsomorphismVariableMappingVisitor visitor = new IsomorphismVariableMappingVisitor(variableMapping);
        op.accept(visitor, arg);
    }

    public static boolean isOperatorIsomorphic(ILogicalOperator op, ILogicalOperator arg) throws AlgebricksException {
        IsomorphismOperatorVisitor visitor = new IsomorphismOperatorVisitor();
        return op.accept(visitor, arg).booleanValue();
    }

    public static boolean isOperatorIsomorphicPlanSegment(ILogicalOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        List<Mutable<ILogicalOperator>> inputs1 = op.getInputs();
        List<Mutable<ILogicalOperator>> inputs2 = arg.getInputs();
        if (inputs1.size() != inputs2.size()) {
            return false;
        }
        for (int i = 0; i < inputs1.size(); i++) {
            ILogicalOperator input1 = inputs1.get(i).getValue();
            ILogicalOperator input2 = inputs2.get(i).getValue();
            boolean isomorphic = isOperatorIsomorphicPlanSegment(input1, input2);
            if (!isomorphic) {
                return false;
            }
        }
        return IsomorphismUtilities.isOperatorIsomorphic(op, arg);
    }

    public static boolean isOperatorIsomorphicPlan(ILogicalPlan plan, ILogicalPlan arg) throws AlgebricksException {
        if (plan.getRoots().size() != arg.getRoots().size()) {
            return false;
        }
        for (int i = 0; i < plan.getRoots().size(); i++) {
            ILogicalOperator topOp1 = plan.getRoots().get(i).getValue();
            ILogicalOperator topOp2 = arg.getRoots().get(i).getValue();
            if (!IsomorphismUtilities.isOperatorIsomorphicPlanSegment(topOp1, topOp2)) {
                return false;
            }
        }
        return true;
    }

    // Return an operator that produced the given PK variable.
    private static ILogicalOperator getOpThatProducesPK(ILogicalOperator rootOp, LogicalVariable pkVar)
            throws AlgebricksException {
        ILogicalOperator prodOp = null;
        boolean produced;
        for (Mutable<ILogicalOperator> opRef : rootOp.getInputs()) {
            produced = false;
            List<LogicalVariable> producedVars = new ArrayList<>();
            VariableUtilities.getProducedVariables(opRef.getValue(), producedVars);
            if (producedVars.contains(pkVar)) {
                prodOp = opRef.getValue();
                produced = true;
            } else if (opRef.getValue().hasInputs()) {
                prodOp = getOpThatProducesPK(opRef.getValue(), pkVar);
                if (prodOp != null) {
                    produced = true;
                }
            }
            if (produced) {
                break;
            }
        }
        return prodOp;
    }

    // Merge the cases where different PKs are derived from the same DATASOURCE
    public static void mergeHomogeneousPK(ILogicalOperator op, List<LogicalVariable> pkVars)
            throws AlgebricksException {
        Map<LogicalVariable, ILogicalOperator> varOpMap = new HashMap<>();
        for (LogicalVariable pk : pkVars) {
            ILogicalOperator mOp = getOpThatProducesPK(op, pk);
            if (mOp == null || !mOp.getOperatorTag().equals(LogicalOperatorTag.DATASOURCESCAN)) {
                throw new AlgebricksException("Illegal variable production.");
            }
            varOpMap.put(pk, mOp);
        }
        // Check the isomorphic variables in pkVars by DataSource, use variableMapping to store each isomorphic pair.
        // For any isomorphic pair <$i, $j>, use $i that is close to the beginning of pkVars as key and let $j as value.
        Map<LogicalVariable, LogicalVariable> variableMapping = new HashMap<>();
        for (int i = 0; i < pkVars.size() - 1; i++) {
            for (int j = i + 1; j < pkVars.size(); j++) {
                IDataSource<?> leftSource = ((DataSourceScanOperator) (varOpMap.get(pkVars.get(i)))).getDataSource();
                IDataSource<?> rightSource = ((DataSourceScanOperator) (varOpMap.get(pkVars.get(j)))).getDataSource();
                if (leftSource.getId().toString().equals(rightSource.getId().toString())) {
                    mapVariablesTopDown(varOpMap.get(pkVars.get(i)), varOpMap.get(pkVars.get(j)), variableMapping);
                }
            }
        }
        // Remove a key variable in pkVars if it has at least one isomorphic variable in variableMapping.
        Iterator<LogicalVariable> itr = pkVars.iterator();
        while (itr.hasNext()) {
            LogicalVariable pk = itr.next();
            if (variableMapping.containsKey(pk)) {
                variableMapping.remove(pk);
                itr.remove();
            }
        }
    }
}
