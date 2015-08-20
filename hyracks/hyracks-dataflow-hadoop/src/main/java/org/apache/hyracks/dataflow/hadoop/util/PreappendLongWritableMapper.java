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
package edu.uci.ics.hyracks.dataflow.hadoop.util;

import java.util.Properties;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import edu.uci.ics.hyracks.api.dataflow.IDataWriter;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.std.map.IDeserializedMapper;

public class PreappendLongWritableMapper implements IDeserializedMapper {

    @Override
    public void map(Object[] data, IDataWriter<Object[]> writer) throws HyracksDataException {
        writer.writeData(new Object[] { new LongWritable(0), new Text(String.valueOf(data[0])) });
    }
}
