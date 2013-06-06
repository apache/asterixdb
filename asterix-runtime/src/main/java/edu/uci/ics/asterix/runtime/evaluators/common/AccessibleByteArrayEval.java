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
package edu.uci.ics.asterix.runtime.evaluators.common;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class AccessibleByteArrayEval implements ICopyEvaluator {

    private DataOutput out;
    private ByteArrayAccessibleOutputStream baaos;
    private DataOutput dataOutput;

    public AccessibleByteArrayEval(DataOutput out) {
        this.out = out;
        this.baaos = new ByteArrayAccessibleOutputStream();
        this.dataOutput = new DataOutputStream(baaos);
    }

    public AccessibleByteArrayEval(DataOutput out, ByteArrayAccessibleOutputStream baaos) {
        this.out = out;
        this.baaos = baaos;
        this.dataOutput = new DataOutputStream(baaos);
    }

    public DataOutput getDataOutput() {
        return dataOutput;
    }

    @Override
    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
        try {
            out.write(baaos.getByteArray(), 0, baaos.size());
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
    }

    public void setBaaos(ByteArrayAccessibleOutputStream baaos) {
        this.baaos = baaos;
    }

    public ByteArrayAccessibleOutputStream getBaaos() {
        return baaos;
    }

    public void reset() {
        baaos.reset();
    }
}
