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
package edu.uci.ics.hyracks.algebricks.runtime.evaluators;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class ColumnAccessEvalFactory implements ICopyEvaluatorFactory {

    private static final long serialVersionUID = 1L;

    private final int fieldIndex;

    public ColumnAccessEvalFactory(int fieldIndex) {
        this.fieldIndex = fieldIndex;
    }

    @Override
    public String toString() {
        return "ColumnAccess(" + fieldIndex + ")";
    }

    @Override
    public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
        return new ICopyEvaluator() {

            private DataOutput out = output.getDataOutput();

            @Override
            public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                byte[] buffer = tuple.getFieldData(fieldIndex);
                int start = tuple.getFieldStart(fieldIndex);
                int length = tuple.getFieldLength(fieldIndex);
                try {
                    out.write(buffer, start, length);
                } catch (IOException ioe) {
                    throw new AlgebricksException(ioe);
                }
            }
        };
    }

}
