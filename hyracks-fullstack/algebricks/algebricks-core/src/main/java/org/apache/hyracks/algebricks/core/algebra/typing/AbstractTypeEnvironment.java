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

import java.util.HashMap;
import java.util.Map;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionTypeComputer;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public abstract class AbstractTypeEnvironment implements IVariableTypeEnvironment {

    protected final Map<LogicalVariable, Object> varTypeMap = new HashMap<LogicalVariable, Object>();
    protected final IExpressionTypeComputer expressionTypeComputer;
    protected final IMetadataProvider<?, ?> metadataProvider;

    public AbstractTypeEnvironment(IExpressionTypeComputer expressionTypeComputer,
            IMetadataProvider<?, ?> metadataProvider) {
        this.expressionTypeComputer = expressionTypeComputer;
        this.metadataProvider = metadataProvider;
    }

    @Override
    public Object getType(ILogicalExpression expr) throws AlgebricksException {
        return expressionTypeComputer.getType(expr, metadataProvider, this);
    }

    @Override
    public void setVarType(LogicalVariable var, Object type) {
        varTypeMap.put(var, type);
    }

    @Override
    public boolean substituteProducedVariable(LogicalVariable v1, LogicalVariable v2) throws AlgebricksException {
        Object t = varTypeMap.remove(v1);
        if (t == null) {
            return false;
        }
        varTypeMap.put(v2, t);
        return true;
    }
}
