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
package org.apache.hyracks.algebricks.tests.pushruntime;

import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.BooleanPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class IntegerGreaterThanEvalFactory implements IScalarEvaluatorFactory {

    private static final long serialVersionUID = 1L;

    private IScalarEvaluatorFactory evalFact1, evalFact2;

    public IntegerGreaterThanEvalFactory(IScalarEvaluatorFactory evalFact1, IScalarEvaluatorFactory evalFact2) {
        this.evalFact1 = evalFact1;
        this.evalFact2 = evalFact2;
    }

    @Override
    public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
        return new IScalarEvaluator() {
            private IPointable p = VoidPointable.FACTORY.createPointable();
            private IScalarEvaluator eval1 = evalFact1.createScalarEvaluator(ctx);
            private IScalarEvaluator eval2 = evalFact2.createScalarEvaluator(ctx);
            private byte[] rBytes = new byte[1];

            @Override
            public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                eval1.evaluate(tuple, p);
                int v1 = IntegerPointable.getInteger(p.getByteArray(), p.getStartOffset());
                eval2.evaluate(tuple, p);
                int v2 = IntegerPointable.getInteger(p.getByteArray(), p.getStartOffset());
                BooleanPointable.setBoolean(rBytes, 0, v1 > v2);
                result.set(rBytes, 0, 1);
            }
        };
    }
}
