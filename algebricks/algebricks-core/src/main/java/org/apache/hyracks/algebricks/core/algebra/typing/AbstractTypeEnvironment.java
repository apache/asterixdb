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

import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

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
        Object t = varTypeMap.get(v1);
        if (t == null) {
            return false;
        }
        varTypeMap.put(v1, null);
        varTypeMap.put(v2, t);
        return true;
    }
}
