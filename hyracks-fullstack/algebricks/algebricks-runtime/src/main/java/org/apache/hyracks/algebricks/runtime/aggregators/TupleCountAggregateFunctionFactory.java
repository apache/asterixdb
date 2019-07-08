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
package org.apache.hyracks.algebricks.runtime.aggregators;

import java.io.IOException;

import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class TupleCountAggregateFunctionFactory implements IAggregateEvaluatorFactory {

    private static final long serialVersionUID = 1L;

    @Override
    public IAggregateEvaluator createAggregateEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        return new IAggregateEvaluator() {

            int cnt;

            @Override
            public void step(IFrameTupleReference tuple) throws HyracksDataException {
                ++cnt;
            }

            @Override
            public void init() throws HyracksDataException {
                cnt = 0;
            }

            @Override
            public void finish(IPointable result) throws HyracksDataException {
                try {
                    abvs.reset();
                    abvs.getDataOutput().writeInt(cnt);
                    result.set(abvs);
                } catch (IOException e) {
                    throw HyracksDataException.create(e);
                }
            }

            @Override
            public void finishPartial(IPointable result) throws HyracksDataException {
                finish(result);
            }
        };
    }

}
