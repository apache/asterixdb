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

package edu.uci.ics.hyracks.storage.am.common.util;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.util.TupleUtils;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleReference;

@SuppressWarnings("rawtypes") 
public class TreeIndexUtils {
	public static String printFrameTuples(ITreeIndexFrame frame, ISerializerDeserializer[] fieldSerdes) throws HyracksDataException {		
		StringBuilder strBuilder = new StringBuilder();
		ITreeIndexTupleReference tuple = frame.createTupleReference();
		for (int i = 0; i < frame.getTupleCount(); i++) {
			tuple.resetByTupleIndex(frame, i);
			String tupleString = TupleUtils.printTuple(tuple, fieldSerdes);
			strBuilder.append(tupleString);
			if (i != frame.getTupleCount() - 1) {
				strBuilder.append(" | ");
			}
		}
		return strBuilder.toString();
    }
}
