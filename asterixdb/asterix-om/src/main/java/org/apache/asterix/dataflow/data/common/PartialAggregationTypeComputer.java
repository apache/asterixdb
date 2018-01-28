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
package org.apache.asterix.dataflow.data.common;

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IPartialAggregationTypeComputer;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public class PartialAggregationTypeComputer implements IPartialAggregationTypeComputer {

    @Override
    public Object getType(ILogicalExpression expr, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AggregateFunctionCallExpression agg = (AggregateFunctionCallExpression) expr;
        FunctionIdentifier partialFid = agg.getFunctionIdentifier();
        if (partialFid.equals(BuiltinFunctions.SERIAL_GLOBAL_AVG)) {
            partialFid = BuiltinFunctions.SERIAL_LOCAL_AVG;
        }
        AggregateFunctionCallExpression partialAgg =
                BuiltinFunctions.makeAggregateFunctionExpression(partialFid, agg.getArguments());
        return getTypeForFunction(partialAgg, env, metadataProvider);
    }

    private Object getTypeForFunction(AbstractFunctionCallExpression expr, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        return BuiltinFunctions.getResultTypeComputer(expr.getFunctionIdentifier()).computeType(expr, env,
                metadataProvider);
    }
}
