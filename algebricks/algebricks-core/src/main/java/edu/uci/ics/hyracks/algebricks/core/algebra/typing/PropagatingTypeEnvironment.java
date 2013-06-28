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
package edu.uci.ics.hyracks.algebricks.core.algebra.typing;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.INullableTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.TypePropagationPolicy;

public class PropagatingTypeEnvironment extends AbstractTypeEnvironment {

    private final TypePropagationPolicy policy;

    private final INullableTypeComputer nullableTypeComputer;

    private final ITypeEnvPointer[] envPointers;

    private final List<LogicalVariable> nonNullVariables = new ArrayList<LogicalVariable>();

    public PropagatingTypeEnvironment(IExpressionTypeComputer expressionTypeComputer,
            INullableTypeComputer nullableTypeComputer, IMetadataProvider<?, ?> metadataProvider,
            TypePropagationPolicy policy, ITypeEnvPointer[] envPointers) {
        super(expressionTypeComputer, metadataProvider);
        this.nullableTypeComputer = nullableTypeComputer;
        this.policy = policy;
        this.envPointers = envPointers;
    }

    @Override
    public Object getVarType(LogicalVariable var) throws AlgebricksException {
        return getVarTypeFullList(var, nonNullVariables);
    }

    public List<LogicalVariable> getNonNullVariables() {
        return nonNullVariables;
    }

    @Override
    public Object getVarType(LogicalVariable var, List<LogicalVariable> nonNullVariableList) throws AlgebricksException {
        nonNullVariableList.addAll(nonNullVariables);
        return getVarTypeFullList(var, nonNullVariableList);
    }

    private Object getVarTypeFullList(LogicalVariable var, List<LogicalVariable> nonNullVariableList)
            throws AlgebricksException {
        Object t = varTypeMap.get(var);
        if (t != null) {
            return t;
        }
        return policy.getVarType(var, nullableTypeComputer, nonNullVariableList, envPointers);
    }
}
