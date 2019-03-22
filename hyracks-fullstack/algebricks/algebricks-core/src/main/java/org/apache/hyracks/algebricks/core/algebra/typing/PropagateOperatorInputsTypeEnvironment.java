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

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionTypeComputer;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public class PropagateOperatorInputsTypeEnvironment extends AbstractTypeEnvironment {

    private final List<LogicalVariable> nonNullVariables = new ArrayList<LogicalVariable>();
    private final List<List<LogicalVariable>> correlatedNullableVariableLists = new ArrayList<List<LogicalVariable>>();
    private final ILogicalOperator op;
    private final ITypingContext ctx;

    public PropagateOperatorInputsTypeEnvironment(ILogicalOperator op, ITypingContext ctx,
            IExpressionTypeComputer expressionTypeComputer, IMetadataProvider<?, ?> metadataProvider) {
        super(expressionTypeComputer, metadataProvider);
        this.op = op;
        this.ctx = ctx;
    }

    public List<LogicalVariable> getNonNullVariables() {
        return nonNullVariables;
    }

    @Override
    public Object getVarType(LogicalVariable var, List<LogicalVariable> nonNullVariableList,
            List<List<LogicalVariable>> correlatedNullableVariableLists) throws AlgebricksException {
        nonNullVariableList.addAll(nonNullVariables);
        return getVarTypeFullList(var, nonNullVariableList, correlatedNullableVariableLists);
    }

    private Object getVarTypeFullList(LogicalVariable var, List<LogicalVariable> nonNullVariableList,
            List<List<LogicalVariable>> correlatedNullableVariableLists) throws AlgebricksException {
        Object t = varTypeMap.get(var);
        if (t != null) {
            return t;
        }
        for (Mutable<ILogicalOperator> r : op.getInputs()) {
            ILogicalOperator c = r.getValue();
            IVariableTypeEnvironment env = ctx.getOutputTypeEnvironment(c);
            Object t2 = env.getVarType(var, nonNullVariableList, correlatedNullableVariableLists);
            if (t2 != null) {
                return t2;
            }
        }
        return null;
    }

    @Override
    public Object getVarType(LogicalVariable var) throws AlgebricksException {
        return getVarTypeFullList(var, nonNullVariables, correlatedNullableVariableLists);
    }

}
