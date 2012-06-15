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
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IDataOutputProvider;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

public class IntegerAddEvalFactory implements ICopyEvaluatorFactory {

    private static final long serialVersionUID = 1L;

    private ICopyEvaluatorFactory evalLeftFactory;
    private ICopyEvaluatorFactory evalRightFactory;

    public IntegerAddEvalFactory(ICopyEvaluatorFactory evalLeftFactory, ICopyEvaluatorFactory evalRightFactory) {
        this.evalLeftFactory = evalLeftFactory;
        this.evalRightFactory = evalRightFactory;
    }

    @Override
    public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
        return new ICopyEvaluator() {

            private DataOutput out = output.getDataOutput();
            private ArrayBackedValueStorage argOut = new ArrayBackedValueStorage();

            private ICopyEvaluator evalLeft = evalLeftFactory.createEvaluator(argOut);
            private ICopyEvaluator evalRight = evalRightFactory.createEvaluator(argOut);

            @SuppressWarnings("static-access")
            @Override
            public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                argOut.reset();
                evalLeft.evaluate(tuple);
                int v1 = IntegerSerializerDeserializer.INSTANCE.getInt(argOut.getByteArray(), 0);
                argOut.reset();
                evalRight.evaluate(tuple);
                int v2 = IntegerSerializerDeserializer.INSTANCE.getInt(argOut.getByteArray(), 0);
                try {
                    out.writeInt(v1 + v2);
                } catch (IOException e) {
                    throw new AlgebricksException(e);
                }
            }
        };
    }

}
