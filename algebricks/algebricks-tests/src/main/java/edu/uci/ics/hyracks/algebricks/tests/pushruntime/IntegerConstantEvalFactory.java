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
package edu.uci.ics.hyracks.algebricks.tests.pushruntime;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

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
    public IScalarEvaluator createScalarEvaluator(IHyracksTaskContext ctx) throws AlgebricksException {
        return new IScalarEvaluator() {

            private ArrayBackedValueStorage buf = new ArrayBackedValueStorage();

            {
                try {
                    IntegerSerializerDeserializer.INSTANCE.serialize(value, buf.getDataOutput());
                } catch (HyracksDataException e) {
                    throw new AlgebricksException(e);
                }
            }

            @Override
            public void evaluate(IFrameTupleReference tuple, IPointable result) throws AlgebricksException {
                result.set(buf);
            }
        };
    }

}