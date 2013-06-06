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
package edu.uci.ics.hivesterix.runtime.factory.nullwriter;

import java.io.DataOutput;

import edu.uci.ics.hyracks.api.dataflow.value.INullWriter;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriterFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class HiveNullWriterFactory implements INullWriterFactory {

    private static final long serialVersionUID = 1L;

    public static HiveNullWriterFactory INSTANCE = new HiveNullWriterFactory();

    @Override
    public INullWriter createNullWriter() {
        return new HiveNullWriter();
    }
}

class HiveNullWriter implements INullWriter {

    @Override
    public void writeNull(DataOutput out) throws HyracksDataException {
        // do nothing
    }

}
