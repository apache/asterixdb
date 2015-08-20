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
import edu.uci.ics.hyracks.algebricks.runtime.base.IUnnestingEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IUnnestingEvaluatorFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class IntArrayUnnester implements IUnnestingEvaluatorFactory {

    private int[] x;

    public IntArrayUnnester(int[] x) {
        this.x = x;
    }

    private static final long serialVersionUID = 1L;

    @Override
    public IUnnestingEvaluator createUnnestingEvaluator(IHyracksTaskContext ctx) throws AlgebricksException {
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        return new IUnnestingEvaluator() {

            private int pos;

            @Override
            public void init(IFrameTupleReference tuple) throws AlgebricksException {
                pos = 0;
            }

            @Override
            public boolean step(IPointable result) throws AlgebricksException {
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
                    throw new AlgebricksException(e);
                }
            }

        };

    }

}
