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
package org.apache.hyracks.algebricks.core.algebra.expressions;

import java.util.List;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;

public interface IVariableTypeEnvironment {
    public Object getVarType(LogicalVariable var) throws AlgebricksException;

    public Object getVarType(LogicalVariable var, List<LogicalVariable> nonNullVariables,
            List<List<LogicalVariable>> correlatedNullableVariableLists) throws AlgebricksException;

    public void setVarType(LogicalVariable var, Object type);

    public Object getType(ILogicalExpression expr) throws AlgebricksException;

    public boolean substituteProducedVariable(LogicalVariable v1, LogicalVariable v2) throws AlgebricksException;
}
