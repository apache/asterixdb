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
package edu.uci.ics.asterix.dataflow.data.nontagged.serde;

import java.io.DataInput;
import java.io.DataOutput;

import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class ATypeSerializerDeserializer implements ISerializerDeserializer<IAType> {

    private static final long serialVersionUID = 1L;

    public static final ATypeSerializerDeserializer INSTANCE = new ATypeSerializerDeserializer();

    private ATypeSerializerDeserializer() {
    }

    @Override
    public IAType deserialize(DataInput in) throws HyracksDataException {
        throw new NotImplementedException();
    }

    @Override
    public void serialize(IAType instance, DataOutput out) throws HyracksDataException {
        throw new NotImplementedException();
    }
}
