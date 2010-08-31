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

package edu.uci.ics.hyracks.storage.am.btree.types;

import java.io.DataInputStream;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.util.StringUtils;
import edu.uci.ics.hyracks.storage.am.btree.api.IFieldAccessor;

public class StringAccessor implements IFieldAccessor {
		
	@Override
	public int getLength(byte[] data, int offset) {
		return StringUtils.getUTFLen(data, offset) + 2;
	}
	
	/*
	@Override
	public int getLength(byte[] data, int offset) {
		// TODO: this is very inefficient. We need a getInt() method that works
		ByteBuffer buf = ByteBuffer.wrap(data);
		return buf.getInt(offset) + 4; // assuming the first int indicates the length
	}
	*/	
			
	@Override
	public String print(byte[] data, int offset) {				
		ByteBuffer buf = ByteBuffer.wrap(data);
		ByteBufferInputStream bbis = new ByteBufferInputStream();
		bbis.setByteBuffer(buf, offset);		
		DataInputStream din = new DataInputStream(bbis);		
		String field = new String();
		try {
			field = UTF8StringSerializerDeserializer.INSTANCE.deserialize(din);
		} catch (HyracksDataException e) {
			e.printStackTrace();
		}		
		return String.format("%10s ", field);
	}   
}
