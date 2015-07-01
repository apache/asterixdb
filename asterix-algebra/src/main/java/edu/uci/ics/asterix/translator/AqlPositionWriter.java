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
package edu.uci.ics.asterix.translator;

import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.hyracks.algebricks.runtime.base.IUnnestingPositionWriter;

public class AqlPositionWriter implements IUnnestingPositionWriter, Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public void write(DataOutput dataOutput, long position) throws IOException {
        dataOutput.writeByte(BuiltinType.AINT64.getTypeTag().serialize());
        dataOutput.writeLong(position);
    }

}
