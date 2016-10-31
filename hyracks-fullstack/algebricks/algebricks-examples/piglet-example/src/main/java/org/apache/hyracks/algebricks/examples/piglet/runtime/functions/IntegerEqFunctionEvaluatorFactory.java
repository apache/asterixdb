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
package org.apache.hyracks.algebricks.examples.piglet.runtime.functions;

import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class IntegerEqFunctionEvaluatorFactory implements IScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    private final IScalarEvaluatorFactory arg1Factory;

    private final IScalarEvaluatorFactory arg2Factory;

    public IntegerEqFunctionEvaluatorFactory(IScalarEvaluatorFactory arg1Factory, IScalarEvaluatorFactory arg2Factory) {
        this.arg1Factory = arg1Factory;
        this.arg2Factory = arg2Factory;
    }

    @Override
    public IScalarEvaluator createScalarEvaluator(final IHyracksTaskContext ctx) throws HyracksDataException {
        return new IScalarEvaluator() {
            private IPointable out1 = new VoidPointable();
            private IPointable out2 = new VoidPointable();
            private IScalarEvaluator eval1 = arg1Factory.createScalarEvaluator(ctx);
            private IScalarEvaluator eval2 = arg2Factory.createScalarEvaluator(ctx);
            private byte[] resultData = new byte[1];

            @Override
            public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                eval1.evaluate(tuple, out1);
                eval2.evaluate(tuple, out2);
                int v1 = IntegerPointable.getInteger(out1.getByteArray(), out1.getStartOffset());
                int v2 = IntegerPointable.getInteger(out2.getByteArray(), out2.getStartOffset());
                boolean r = v1 == v2;
                resultData[0] = r ? (byte) 1 : (byte) 0;
                result.set(resultData, 0, 1);
            }
        };
    }
}
