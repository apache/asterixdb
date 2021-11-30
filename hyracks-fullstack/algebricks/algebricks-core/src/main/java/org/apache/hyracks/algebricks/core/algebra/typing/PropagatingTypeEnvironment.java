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
package org.apache.hyracks.algebricks.core.algebra.typing;

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionTypeComputer;
import org.apache.hyracks.algebricks.core.algebra.expressions.IMissableTypeComputer;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import org.apache.hyracks.algebricks.core.algebra.properties.TypePropagationPolicy;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;

public class PropagatingTypeEnvironment extends AbstractTypeEnvironment {

    private final TypePropagationPolicy policy;

    private final IMissableTypeComputer missableTypeComputer;

    private final ITypeEnvPointer[] envPointers;

    private final List<LogicalVariable> nonMissableVariables = new ArrayList<>();

    private final List<List<LogicalVariable>> correlatedMissableVariableLists = new ArrayList<>();

    private final List<LogicalVariable> nonNullableVariables = new ArrayList<>();

    private final List<List<LogicalVariable>> correlatedNullableVariableLists = new ArrayList<>();

    public PropagatingTypeEnvironment(IExpressionTypeComputer expressionTypeComputer,
            IMissableTypeComputer missableTypeComputer, IMetadataProvider<?, ?> metadataProvider,
            TypePropagationPolicy policy, ITypeEnvPointer[] envPointers) {
        super(expressionTypeComputer, metadataProvider);
        this.missableTypeComputer = missableTypeComputer;
        this.policy = policy;
        this.envPointers = envPointers;
    }

    @Override
    public Object getVarType(LogicalVariable var) throws AlgebricksException {
        return getVarTypeFullList(var, null, null, null, null);
    }

    public List<LogicalVariable> getNonMissableVariables() {
        return nonMissableVariables;
    }

    public List<List<LogicalVariable>> getCorrelatedMissableVariableLists() {
        return correlatedMissableVariableLists;
    }

    public List<LogicalVariable> getNonNullableVariables() {
        return nonNullableVariables;
    }

    public List<List<LogicalVariable>> getCorrelatedNullableVariableLists() {
        return correlatedNullableVariableLists;
    }

    @Override
    public Object getVarType(LogicalVariable var, List<LogicalVariable> nonMissableVariables,
            List<List<LogicalVariable>> correlatedMissableVariableLists, List<LogicalVariable> nonNullableVariables,
            List<List<LogicalVariable>> correlatedNullableVariableLists) throws AlgebricksException {
        return getVarTypeFullList(var, nonMissableVariables, correlatedMissableVariableLists, nonNullableVariables,
                correlatedNullableVariableLists);
    }

    private Object getVarTypeFullList(LogicalVariable var, List<LogicalVariable> nonMissableVariableListExtra,
            List<List<LogicalVariable>> correlatedMissableVariableListsExtra,
            List<LogicalVariable> nonNullableVariableListExtra,
            List<List<LogicalVariable>> correlatedNullableVariableListsExtra) throws AlgebricksException {
        Object t = varTypeMap.get(var);
        if (t != null) {
            return t;
        }

        List<LogicalVariable> nonMissable =
                OperatorPropertiesUtil.unionAll(nonMissableVariables, nonMissableVariableListExtra);
        List<LogicalVariable> nonNullable =
                OperatorPropertiesUtil.unionAll(nonNullableVariables, nonNullableVariableListExtra);
        List<List<LogicalVariable>> correlatedMissable =
                OperatorPropertiesUtil.unionAll(correlatedMissableVariableLists, correlatedMissableVariableListsExtra);
        List<List<LogicalVariable>> correlatedNullable =
                OperatorPropertiesUtil.unionAll(correlatedNullableVariableLists, correlatedNullableVariableListsExtra);

        return policy.getVarType(var, missableTypeComputer, nonMissable, correlatedMissable, nonNullable,
                correlatedNullable, envPointers);
    }

    @Override
    public boolean substituteProducedVariable(LogicalVariable v1, LogicalVariable v2) throws AlgebricksException {
        boolean result = super.substituteProducedVariable(v1, v2);
        if (nonMissableVariables.remove(v1)) {
            nonMissableVariables.add(v2);
        }
        for (List<LogicalVariable> missableVarList : correlatedMissableVariableLists) {
            if (missableVarList.remove(v1)) {
                missableVarList.add(v2);
            }
        }
        if (nonNullableVariables.remove(v1)) {
            nonNullableVariables.add(v2);
        }
        for (List<LogicalVariable> nullableVarList : correlatedNullableVariableLists) {
            if (nullableVarList.remove(v1)) {
                nullableVarList.add(v2);
            }
        }
        return result;
    }
}
