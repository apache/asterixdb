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

package org.apache.hyracks.algebricks.core.algebra.properties;

import java.util.List;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IMissableTypeComputer;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypeEnvPointer;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;

public abstract class LeftOuterTypePropagationPolicy extends TypePropagationPolicy {

    public static final TypePropagationPolicy MISSABLE = new LeftOuterTypePropagationPolicy() {

        protected Object computeInnerBranchVarType(LogicalVariable var, Object varType, IMissableTypeComputer ntc,
                List<LogicalVariable> nonMissableVariableList,
                List<List<LogicalVariable>> correlatedMissableVariableLists,
                List<LogicalVariable> nonNullableVariableList,
                List<List<LogicalVariable>> correlatedNullableVariableLists) {
            boolean makeMissable =
                    !inCorrelatedVariableList(var, correlatedMissableVariableLists, nonMissableVariableList);
            return makeMissable ? ntc.makeMissableType(varType) : varType;
        }
    };

    public static final TypePropagationPolicy NULLABLE = new LeftOuterTypePropagationPolicy() {

        protected Object computeInnerBranchVarType(LogicalVariable var, Object varType, IMissableTypeComputer ntc,
                List<LogicalVariable> nonMissableVariableList,
                List<List<LogicalVariable>> correlatedMissableVariableLists,
                List<LogicalVariable> nonNullableVariableList,
                List<List<LogicalVariable>> correlatedNullableVariableLists) {
            boolean makeNullable =
                    !inCorrelatedVariableList(var, correlatedNullableVariableLists, nonNullableVariableList);
            return makeNullable ? ntc.makeNullableType(varType) : varType;
        }
    };

    @Override
    public Object getVarType(LogicalVariable var, IMissableTypeComputer ntc,
            List<LogicalVariable> nonMissableVariableList, List<List<LogicalVariable>> correlatedMissableVariableLists,
            List<LogicalVariable> nonNullableVariableList, List<List<LogicalVariable>> correlatedNullableVariableLists,
            ITypeEnvPointer... typeEnvs) throws AlgebricksException {
        int n = typeEnvs.length;
        // Searches from the inner branch to the outer branch.
        // TODO(buyingyi): A split operator could lead to the case that the type for a variable could be
        // found in both inner and outer branches. Fix computeOutputTypeEnvironment() in ProjectOperator
        // and investigate why many test queries fail if only live variables' types are propagated.
        for (int i = n - 1; i >= 0; i--) {
            Object varType = typeEnvs[i].getTypeEnv().getVarType(var, nonMissableVariableList,
                    correlatedMissableVariableLists, nonNullableVariableList, correlatedNullableVariableLists);
            if (varType == null) {
                continue;
            }
            if (i == 0) { // outer branch
                return varType;
            }
            // inner branch
            return computeInnerBranchVarType(var, varType, ntc, nonMissableVariableList,
                    correlatedMissableVariableLists, nonNullableVariableList, correlatedNullableVariableLists);
        }
        return null;
    }

    protected abstract Object computeInnerBranchVarType(LogicalVariable var, Object varType, IMissableTypeComputer ntc,
            List<LogicalVariable> nonMissableVariableList, List<List<LogicalVariable>> correlatedMissableVariableLists,
            List<LogicalVariable> nonNullableVariableList, List<List<LogicalVariable>> correlatedNullableVariableLists);

    protected static boolean inCorrelatedVariableList(LogicalVariable var,
            List<List<LogicalVariable>> correlatedOptionalVariableLists,
            List<LogicalVariable> nonOptionalVariableList) {
        if (!nonOptionalVariableList.isEmpty()) {
            for (List<LogicalVariable> correlatedVariables : correlatedOptionalVariableLists) {
                if (correlatedVariables.contains(var)
                        && !OperatorPropertiesUtil.disjoint(correlatedVariables, nonOptionalVariableList)) {
                    return true;
                }
            }
        }
        return false;
    }
};
