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
package edu.uci.ics.hyracks.algebricks.runtime.operators.std;

import java.nio.ByteBuffer;
import java.util.Arrays;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import edu.uci.ics.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputRuntimeFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class StreamProjectRuntimeFactory extends AbstractOneInputOneOutputRuntimeFactory {

    private static final long serialVersionUID = 1L;

    public StreamProjectRuntimeFactory(int[] projectionList) {
        super(projectionList);
    }

    @Override
    public String toString() {
        return "stream-project " + Arrays.toString(projectionList);
    }

    @Override
    public AbstractOneInputOneOutputOneFramePushRuntime createOneOutputPushRuntime(final IHyracksTaskContext ctx)
            throws AlgebricksException {

        return new AbstractOneInputOneOutputOneFramePushRuntime() {

            private boolean first = true;

            @Override
            public void open() throws HyracksDataException {
                if (first) {
                    first = false;
                    initAccessAppend(ctx);
                }
                writer.open();
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                tAccess.reset(buffer);
                int nTuple = tAccess.getTupleCount();
                for (int t = 0; t < nTuple; t++) {
                    appendProjectionToFrame(t, projectionList);
                }

            }

        };
    }

}
