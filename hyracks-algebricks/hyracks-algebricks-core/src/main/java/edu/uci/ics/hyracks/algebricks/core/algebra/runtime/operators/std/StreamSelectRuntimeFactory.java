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
package edu.uci.ics.hyracks.algebricks.core.algebra.runtime.operators.std;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.algebricks.core.algebra.data.IBinaryBooleanInspector;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluator;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.context.RuntimeContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.operators.base.AbstractOneInputOneOutputRuntimeFactory;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;

public class StreamSelectRuntimeFactory extends AbstractOneInputOneOutputRuntimeFactory {

    private static final long serialVersionUID = 1L;

    private IEvaluatorFactory cond;

    private IBinaryBooleanInspector binaryBooleanInspector;

    /**
     * 
     * @param cond
     * @param projectionList
     *            if projectionList is null, then no projection is performed
     */
    public StreamSelectRuntimeFactory(IEvaluatorFactory cond, int[] projectionList,
            IBinaryBooleanInspector binaryBooleanInspector) {
        super(projectionList);
        this.cond = cond;
        this.binaryBooleanInspector = binaryBooleanInspector;
    }

    @Override
    public String toString() {
        return "stream-select " + cond.toString();
    }

    @Override
    public AbstractOneInputOneOutputOneFramePushRuntime createOneOutputPushRuntime(final RuntimeContext context) {
        return new AbstractOneInputOneOutputOneFramePushRuntime() {

            private IEvaluator eval;
            private ArrayBackedValueStorage evalOutput;

            @Override
            public void open() throws HyracksDataException {
                if (eval == null) {
                    initAccessAppendRef(context);
                    evalOutput = new ArrayBackedValueStorage();
                    try {
                        eval = cond.createEvaluator(evalOutput);
                    } catch (AlgebricksException ae) {
                        throw new HyracksDataException(ae);
                    }
                }
                writer.open();
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                tAccess.reset(buffer);
                int nTuple = tAccess.getTupleCount();
                for (int t = 0; t < nTuple; t++) {
                    tRef.reset(tAccess, t);
                    evalOutput.reset();
                    try {
                        eval.evaluate(tRef);
                    } catch (AlgebricksException ae) {
                        throw new HyracksDataException(ae);
                    }
                    if (binaryBooleanInspector.getBooleanValue(evalOutput.getBytes(), 0, evalOutput.getLength())) {
                        if (projectionList != null) {
                            appendProjectionToFrame(t, projectionList);
                        } else {
                            appendTupleToFrame(t);
                        }
                    }
                }
            }

        };
    }

}
