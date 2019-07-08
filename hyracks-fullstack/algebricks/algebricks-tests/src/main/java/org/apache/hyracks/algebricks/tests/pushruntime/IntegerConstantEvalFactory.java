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
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

public class IntegerConstantEvalFactory implements IScalarEvaluatorFactory {

    private static final long serialVersionUID = 1L;
    private final int value;

    public IntegerConstantEvalFactory(int value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "IntConstantEvalFactory " + value;
    }

    @Override
    public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
        return new IScalarEvaluator() {

            private ArrayBackedValueStorage buf = new ArrayBackedValueStorage();

            {
                IntegerSerializerDeserializer.INSTANCE.serialize(value, buf.getDataOutput());
            }

            @Override
            public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                result.set(buf);
            }
        };
    }

}
