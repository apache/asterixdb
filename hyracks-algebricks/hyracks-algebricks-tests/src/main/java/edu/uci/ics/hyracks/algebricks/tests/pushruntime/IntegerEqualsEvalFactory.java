/*
 * Copyright 2009-2010 by The Regents of the University of California
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

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IDataOutputProvider;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

public class IntegerEqualsEvalFactory implements IEvaluatorFactory {

    private static final long serialVersionUID = 1L;

    private IEvaluatorFactory evalFact1, evalFact2;

    public IntegerEqualsEvalFactory(IEvaluatorFactory evalFact1, IEvaluatorFactory evalFact2) {
        this.evalFact1 = evalFact1;
        this.evalFact2 = evalFact2;
    }

    @Override
    public IEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
        return new IEvaluator() {
            private DataOutput dataout = output.getDataOutput();
            private ArrayBackedValueStorage out1 = new ArrayBackedValueStorage();
            private ArrayBackedValueStorage out2 = new ArrayBackedValueStorage();
            private IEvaluator eval1 = evalFact1.createEvaluator(out1);
            private IEvaluator eval2 = evalFact2.createEvaluator(out2);

            @Override
            public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                out1.reset();
                eval1.evaluate(tuple);
                out2.reset();
                eval2.evaluate(tuple);
                int v1 = IntegerSerializerDeserializer.getInt(out1.getByteArray(), 0);
                int v2 = IntegerSerializerDeserializer.getInt(out2.getByteArray(), 0);
                boolean r = v1 == v2;
                try {
                    dataout.writeBoolean(r);
                } catch (IOException ioe) {
                    throw new AlgebricksException(ioe);
                }
            }
        };
    }
}
