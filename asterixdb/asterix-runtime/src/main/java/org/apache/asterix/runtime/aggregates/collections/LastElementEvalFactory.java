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

package org.apache.asterix.runtime.aggregates.collections;

import org.apache.asterix.runtime.aggregates.std.AbstractAggregateFunction;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

class LastElementEvalFactory implements IAggregateEvaluatorFactory {

    private static final long serialVersionUID = 1L;
    private final IScalarEvaluatorFactory[] args;
    private final SourceLocation sourceLoc;

    LastElementEvalFactory(IScalarEvaluatorFactory[] args, SourceLocation sourceLoc) {
        this.args = args;
        this.sourceLoc = sourceLoc;
    }

    @Override
    public IAggregateEvaluator createAggregateEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
        return new AbstractAggregateFunction(sourceLoc) {

            // Needs to copy the bytes from inputVal to outputVal because the byte space of inputVal could be re-used
            // by consequent tuples.
            private ArrayBackedValueStorage outputVal = new ArrayBackedValueStorage();
            private IPointable inputVal = new VoidPointable();
            private IScalarEvaluator eval = args[0].createScalarEvaluator(ctx);
            private boolean empty;

            @Override
            public void init() {
                empty = true;
            }

            @Override
            public void step(IFrameTupleReference tuple) throws HyracksDataException {
                eval.evaluate(tuple, inputVal);
                outputVal.assign(inputVal);
                empty = false;
            }

            @Override
            public void finish(IPointable result) {
                if (empty) {
                    PointableHelper.setNull(result);
                } else {
                    result.set(outputVal);
                }
            }

            @Override
            public void finishPartial(IPointable result) {
                throw new UnsupportedOperationException();
            }
        };
    }

}
