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
package org.apache.asterix.metadata.utils.filter;

import java.util.List;

import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;

public class FilterVariableTypeEnvironment implements IVariableTypeEnvironment {
    @Override
    public Object getVarType(LogicalVariable var) throws AlgebricksException {
        throw new IllegalAccessError("Should not be invoked");
    }

    @Override
    public Object getVarType(LogicalVariable var, List<LogicalVariable> nonMissableVariables,
            List<List<LogicalVariable>> correlatedMissableVariableLists, List<LogicalVariable> nonNullableVariables,
            List<List<LogicalVariable>> correlatedNullableVariableLists) throws AlgebricksException {
        throw new IllegalAccessError("Should not be invoked");
    }

    @Override
    public void setVarType(LogicalVariable var, Object type) {
        throw new IllegalAccessError("Should not be invoked");
    }

    @Override
    public Object getType(ILogicalExpression expr) throws AlgebricksException {
        return BuiltinType.ANY;
    }

    @Override
    public boolean substituteProducedVariable(LogicalVariable v1, LogicalVariable v2) throws AlgebricksException {
        throw new IllegalAccessError("Should not be invoked");
    }
}
