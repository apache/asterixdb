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
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.primitive.BooleanPointable;
import edu.uci.ics.hyracks.data.std.primitive.VoidPointable;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

public class IntegerGreaterThanEvalFactory implements IScalarEvaluatorFactory {

    private static final long serialVersionUID = 1L;

    private IScalarEvaluatorFactory evalFact1, evalFact2;

    public IntegerGreaterThanEvalFactory(IScalarEvaluatorFactory evalFact1, IScalarEvaluatorFactory evalFact2) {
        this.evalFact1 = evalFact1;
        this.evalFact2 = evalFact2;
    }

    @Override
    public IScalarEvaluator createScalarEvaluator(final IHyracksTaskContext ctx) throws AlgebricksException {
        return new IScalarEvaluator() {
            private IPointable p = VoidPointable.FACTORY.createPointable();
            private IScalarEvaluator eval1 = evalFact1.createScalarEvaluator(ctx);
            private IScalarEvaluator eval2 = evalFact2.createScalarEvaluator(ctx);
            private byte[] rBytes = new byte[1];

            @Override
            public void evaluate(IFrameTupleReference tuple, IPointable result) throws AlgebricksException {
                eval1.evaluate(tuple, p);
                int v1 = IntegerSerializerDeserializer.getInt(p.getByteArray(), p.getStartOffset());
                eval2.evaluate(tuple, p);
                int v2 = IntegerSerializerDeserializer.getInt(p.getByteArray(), p.getStartOffset());
                BooleanPointable.setBoolean(rBytes, 0, v1 > v2);
                result.set(rBytes, 0, 1);
            }
        };
    }
}