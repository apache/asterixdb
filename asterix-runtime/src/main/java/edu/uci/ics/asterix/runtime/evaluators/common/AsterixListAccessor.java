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
import java.io.IOException;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AOrderedListSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AUnorderedListSerializerDeserializer;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;

/**
 * Utility class for accessing serialized unordered and ordered lists. 
 */
public class AsterixListAccessor {

	protected byte[] listBytes;
	protected int start;
	protected ATypeTag listType;
	protected ATypeTag itemType;
	protected int size;
	
	public ATypeTag getListType() {
		return listType;
	}

	public ATypeTag getItemType() {
		return itemType;
	}

	public boolean itemsAreSelfDescribing() {
		return itemType == ATypeTag.ANY;
	}

	public void reset(byte[] listBytes, int start) throws AsterixException {
		this.listBytes = listBytes;
		this.start = start;
		listType = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(listBytes[start]);
		if (listType != ATypeTag.UNORDEREDLIST && listType != ATypeTag.ORDEREDLIST) {
			throw new AsterixException("Unsupported type: " + listType);
		}
		itemType = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(listBytes[start + 1]);
		if (listType == ATypeTag.UNORDEREDLIST) {
			size = AUnorderedListSerializerDeserializer.getNumberOfItems(listBytes, start);
		} else {
			size = AOrderedListSerializerDeserializer.getNumberOfItems(listBytes, start);
		}
	}

	public int size() {
		return size;
	}
	
	public int getItemOffset(int itemIndex) throws AsterixException {
		if (listType == ATypeTag.UNORDEREDLIST) {
			return AUnorderedListSerializerDeserializer.getItemOffset(listBytes, start, itemIndex);
		} else {
			return AOrderedListSerializerDeserializer.getItemOffset(listBytes, start, itemIndex);
		}
	}
	
	public int getItemLength(int itemOffset) throws AsterixException {
		ATypeTag itemType = getItemType(itemOffset);
		return NonTaggedFormatUtil.getFieldValueLength(listBytes, itemOffset, itemType, itemsAreSelfDescribing());
	}
	
	public ATypeTag getItemType(int itemOffset) throws AsterixException {
		if (itemType == ATypeTag.ANY) {
			return EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(listBytes[itemOffset]);
		} else {
			return itemType;
		}
	}
	
	public void writeItem(int itemIndex, DataOutput dos) throws AsterixException, IOException {
		int itemOffset = getItemOffset(itemIndex);
		int itemLength = getItemLength(itemOffset);
		if (itemsAreSelfDescribing()) {
			++itemLength;
		} else {
			dos.writeByte(itemType.serialize());
		}
		dos.write(listBytes, itemOffset, itemLength);
	}
	
	public byte[] getByteArray() {
		return listBytes;
	}
	
	public int getStart() {
		return start;
	}
}
