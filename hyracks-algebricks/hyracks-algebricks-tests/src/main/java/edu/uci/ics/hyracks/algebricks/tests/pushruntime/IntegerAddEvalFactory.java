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

import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluator;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IDataOutputProvider;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

public class IntegerAddEvalFactory implements IEvaluatorFactory {

    private static final long serialVersionUID = 1L;

    private IEvaluatorFactory evalLeftFactory;
    private IEvaluatorFactory evalRightFactory;

    public IntegerAddEvalFactory(IEvaluatorFactory evalLeftFactory, IEvaluatorFactory evalRightFactory) {
        this.evalLeftFactory = evalLeftFactory;
        this.evalRightFactory = evalRightFactory;
    }

    @Override
    public IEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
        return new IEvaluator() {

            private DataOutput out = output.getDataOutput();
            private ArrayBackedValueStorage argOut = new ArrayBackedValueStorage();

            private IEvaluator evalLeft = evalLeftFactory.createEvaluator(argOut);
            private IEvaluator evalRight = evalRightFactory.createEvaluator(argOut);

            @SuppressWarnings("static-access")
            @Override
            public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                argOut.reset();
                evalLeft.evaluate(tuple);
                int v1 = IntegerSerializerDeserializer.INSTANCE.getInt(argOut.getBytes(), 0);
                argOut.reset();
                evalRight.evaluate(tuple);
                int v2 = IntegerSerializerDeserializer.INSTANCE.getInt(argOut.getBytes(), 0);
                try {
                    out.writeInt(v1 + v2);
                } catch (IOException e) {
                    throw new AlgebricksException(e);
                }
            }
        };
    }

}
