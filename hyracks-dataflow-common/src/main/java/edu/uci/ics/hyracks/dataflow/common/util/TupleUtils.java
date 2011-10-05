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

package edu.uci.ics.hyracks.dataflow.common.util;

import java.io.DataOutput;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

@SuppressWarnings("rawtypes") 
public class TupleUtils {    
    @SuppressWarnings("unchecked")
    public static void createTuple(ArrayTupleBuilder tupleBuilder, ArrayTupleReference tuple, ISerializerDeserializer[] fieldSerdes, final Object... fields) throws HyracksDataException {
        DataOutput dos = tupleBuilder.getDataOutput();
        tupleBuilder.reset();
        for (int i = 0; i < fields.length; i++) {  
            fieldSerdes[i].serialize(fields[i], dos);
            tupleBuilder.addFieldEndOffset();
        }
        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
    }

    public static ITupleReference createTuple(ISerializerDeserializer[] fieldSerdes, final Object... fields) throws HyracksDataException {
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(fields.length);
        ArrayTupleReference tuple = new ArrayTupleReference();
        createTuple(tupleBuilder, tuple, fieldSerdes, fields);
        return tuple;
    }
    
    public static void createIntegerTuple(ArrayTupleBuilder tupleBuilder, ArrayTupleReference tuple,
            final int... fields) throws HyracksDataException {
        DataOutput dos = tupleBuilder.getDataOutput();
        tupleBuilder.reset();
        for (final int i : fields) {
            IntegerSerializerDeserializer.INSTANCE.serialize(i, dos);
            tupleBuilder.addFieldEndOffset();
        }
        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
    }

    public static ITupleReference createIntegerTuple(final int... fields) throws HyracksDataException {
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(fields.length);
        ArrayTupleReference tuple = new ArrayTupleReference();
        createIntegerTuple(tupleBuilder, tuple, fields);
        return tuple;
    }    
}
