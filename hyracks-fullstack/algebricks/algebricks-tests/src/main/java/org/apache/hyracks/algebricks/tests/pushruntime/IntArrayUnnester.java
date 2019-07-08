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

import java.io.IOException;

import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IUnnestingEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IUnnestingEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class IntArrayUnnester implements IUnnestingEvaluatorFactory {

    private int[] x;

    public IntArrayUnnester(int[] x) {
        this.x = x;
    }

    private static final long serialVersionUID = 1L;

    @Override
    public IUnnestingEvaluator createUnnestingEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        return new IUnnestingEvaluator() {

            private int pos;

            @Override
            public void init(IFrameTupleReference tuple) throws HyracksDataException {
                pos = 0;
            }

            @Override
            public boolean step(IPointable result) throws HyracksDataException {
                try {
                    if (pos < x.length) {
                        // Writes one byte to distinguish between null
                        // values and end of sequence.
                        abvs.reset();
                        abvs.getDataOutput().writeInt(x[pos]);
                        result.set(abvs);
                        ++pos;
                        return true;
                    } else {
                        return false;
                    }

                } catch (IOException e) {
                    throw HyracksDataException.create(e);
                }
            }

        };

    }

}
