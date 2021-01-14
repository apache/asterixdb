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

public class PropagatingTypeEnvironment extends AbstractTypeEnvironment {

    private final TypePropagationPolicy policy;

    private final IMissableTypeComputer missableTypeComputer;

    private final ITypeEnvPointer[] envPointers;

    private final List<LogicalVariable> nonMissableVariables = new ArrayList<>();

    private final List<List<LogicalVariable>> correlatedMissableVariableLists = new ArrayList<>();

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
        return getVarTypeFullList(var, nonMissableVariables, correlatedMissableVariableLists);
    }

    public List<LogicalVariable> getNonMissableVariables() {
        return nonMissableVariables;
    }

    public List<List<LogicalVariable>> getCorrelatedMissableVariableLists() {
        return correlatedMissableVariableLists;
    }

    @Override
    public Object getVarType(LogicalVariable var, List<LogicalVariable> nonMissableVariableList,
            List<List<LogicalVariable>> correlatedMissableVariableLists) throws AlgebricksException {
        for (LogicalVariable v : nonMissableVariables) {
            if (!nonMissableVariableList.contains(v)) {
                nonMissableVariableList.add(v);
            }
        }
        Object t = getVarTypeFullList(var, nonMissableVariableList, correlatedMissableVariableLists);
        for (List<LogicalVariable> list : correlatedMissableVariableLists) {
            if (!correlatedMissableVariableLists.contains(list)) {
                correlatedMissableVariableLists.add(list);
            }
        }
        return t;
    }

    private Object getVarTypeFullList(LogicalVariable var, List<LogicalVariable> nonMissableVariableList,
            List<List<LogicalVariable>> correlatedMissableVariableLists) throws AlgebricksException {
        Object t = varTypeMap.get(var);
        if (t != null) {
            return t;
        }
        return policy.getVarType(var, missableTypeComputer, nonMissableVariableList, correlatedMissableVariableLists,
                envPointers);
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
        return result;
    }
}
