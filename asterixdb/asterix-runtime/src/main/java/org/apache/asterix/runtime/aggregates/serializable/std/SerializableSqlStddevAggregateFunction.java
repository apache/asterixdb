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
package org.apache.asterix.runtime.aggregates.serializable.std;

import java.io.DataOutput;

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class SerializableSqlStddevAggregateFunction
        extends AbstractSerializableSingleVariableStatisticsAggregateFunction {

    private final boolean isPop;
    private final int delta;

    public SerializableSqlStddevAggregateFunction(IScalarEvaluatorFactory[] args, IEvaluatorContext context,
            boolean isPop, SourceLocation sourceLoc) throws HyracksDataException {
        super(args, context, sourceLoc);
        this.isPop = isPop;
        if (isPop) {
            delta = 0;
        } else {
            delta = 1;
        }
    }

    @Override
    public void step(IFrameTupleReference tuple, byte[] state, int start, int len) throws HyracksDataException {
        processDataValues(tuple, state, start, len);
    }

    @Override
    public void finish(byte[] state, int start, int len, DataOutput result) throws HyracksDataException {
        finishStddevFinalResults(state, start, len, result, delta);
    }

    @Override
    public void finishPartial(byte[] state, int start, int len, DataOutput result) throws HyracksDataException {
        finish(state, start, len, result);
    }

    @Override
    protected void processNull(byte[] state, int start) {
    }

    @Override
    protected FunctionIdentifier getFunctionIdentifier() {
        if (isPop) {
            return BuiltinFunctions.STDDEV_POP;
        } else {
            return BuiltinFunctions.STDDEV_SAMP;
        }
    }

    @Override
    protected boolean getM3Flag() {
        return false;
    }

    @Override
    protected boolean getM4Flag() {
        return false;
    }
}
