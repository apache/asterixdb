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

package org.apache.asterix.runtime.aggregates.std;

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class SqlSkewnessAggregateFunction extends AbstractSingleVarStatisticsAggregateFunction {

    public SqlSkewnessAggregateFunction(IScalarEvaluatorFactory[] args, IEvaluatorContext context,
            SourceLocation sourceLoc) throws HyracksDataException {
        super(args, context, sourceLoc);
    }

    @Override
    public void step(IFrameTupleReference tuple) throws HyracksDataException {
        processDataValues(tuple);
    }

    @Override
    public void finish(IPointable result) throws HyracksDataException {
        finishSkewFinalResults(result);
    }

    @Override
    public void finishPartial(IPointable result) throws HyracksDataException {
        finishPartialResults(result);
    }

    @Override
    protected void processNull() {
    }

    @Override
    protected boolean getM3Flag() {
        return true;
    }

    @Override
    protected boolean getM4Flag() {
        return false;
    }

    @Override
    protected FunctionIdentifier getFunctionIdentifier() {
        return BuiltinFunctions.SKEWNESS;
    }
}
