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

import java.io.IOException;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.primitive.VoidPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

public class IntegerAddEvalFactory implements IScalarEvaluatorFactory {

    private static final long serialVersionUID = 1L;

    private IScalarEvaluatorFactory evalLeftFactory;
    private IScalarEvaluatorFactory evalRightFactory;

    public IntegerAddEvalFactory(IScalarEvaluatorFactory evalLeftFactory, IScalarEvaluatorFactory evalRightFactory) {
        this.evalLeftFactory = evalLeftFactory;
        this.evalRightFactory = evalRightFactory;
    }

    @Override
    public IScalarEvaluator createScalarEvaluator(final IHyracksTaskContext ctx) throws AlgebricksException {
        return new IScalarEvaluator() {
            private IPointable p = VoidPointable.FACTORY.createPointable();
            private ArrayBackedValueStorage argOut = new ArrayBackedValueStorage();

            private IScalarEvaluator evalLeft = evalLeftFactory.createScalarEvaluator(ctx);
            private IScalarEvaluator evalRight = evalRightFactory.createScalarEvaluator(ctx);

            @SuppressWarnings("static-access")
            @Override
            public void evaluate(IFrameTupleReference tuple, IPointable result) throws AlgebricksException {
                evalLeft.evaluate(tuple, p);
                int v1 = IntegerSerializerDeserializer.INSTANCE.getInt(p.getByteArray(), p.getStartOffset());
                evalRight.evaluate(tuple, p);
                int v2 = IntegerSerializerDeserializer.INSTANCE.getInt(p.getByteArray(), p.getStartOffset());
                try {
                    argOut.reset();
                    argOut.getDataOutput().writeInt(v1 + v2);
                    result.set(argOut);
                } catch (IOException e) {
                    throw new AlgebricksException(e);
                }
            }
        };
    }

}
