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

    private final IMissableTypeComputer nullableTypeComputer;

    private final ITypeEnvPointer[] envPointers;

    private final List<LogicalVariable> nonNullVariables = new ArrayList<>();

    private final List<List<LogicalVariable>> correlatedNullableVariableLists = new ArrayList<>();

    public PropagatingTypeEnvironment(IExpressionTypeComputer expressionTypeComputer,
            IMissableTypeComputer nullableTypeComputer, IMetadataProvider<?, ?> metadataProvider,
            TypePropagationPolicy policy, ITypeEnvPointer[] envPointers) {
        super(expressionTypeComputer, metadataProvider);
        this.nullableTypeComputer = nullableTypeComputer;
        this.policy = policy;
        this.envPointers = envPointers;
    }

    @Override
    public Object getVarType(LogicalVariable var) throws AlgebricksException {
        return getVarTypeFullList(var, nonNullVariables, correlatedNullableVariableLists);
    }

    public List<LogicalVariable> getNonNullVariables() {
        return nonNullVariables;
    }

    public List<List<LogicalVariable>> getCorrelatedMissableVariableLists() {
        return correlatedNullableVariableLists;
    }

    @Override
    public Object getVarType(LogicalVariable var, List<LogicalVariable> nonNullVariableList,
            List<List<LogicalVariable>> correlatedNullableVariableLists) throws AlgebricksException {
        for (LogicalVariable v : nonNullVariables) {
            if (!nonNullVariableList.contains(v)) {
                nonNullVariableList.add(v);
            }
        }
        Object t = getVarTypeFullList(var, nonNullVariableList, correlatedNullableVariableLists);
        for (List<LogicalVariable> list : this.correlatedNullableVariableLists) {
            if (!correlatedNullableVariableLists.contains(list)) {
                correlatedNullableVariableLists.add(list);
            }
        }
        return t;
    }

    private Object getVarTypeFullList(LogicalVariable var, List<LogicalVariable> nonNullVariableList,
            List<List<LogicalVariable>> correlatedNullableVariableLists) throws AlgebricksException {
        Object t = varTypeMap.get(var);
        if (t != null) {
            return t;
        }
        return policy.getVarType(var, nullableTypeComputer, nonNullVariableList, correlatedNullableVariableLists,
                envPointers);
    }
}
