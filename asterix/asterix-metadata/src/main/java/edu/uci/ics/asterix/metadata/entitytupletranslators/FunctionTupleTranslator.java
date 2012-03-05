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

package edu.uci.ics.asterix.metadata.entitytupletranslators;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.asterix.builders.IAOrderedListBuilder;
import edu.uci.ics.asterix.builders.OrderedListBuilder;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataRecordTypes;
import edu.uci.ics.asterix.metadata.entities.Function;
import edu.uci.ics.asterix.om.base.AOrderedList;
import edu.uci.ics.asterix.om.base.ARecord;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.base.IACursor;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Translates a Function metadata entity to an ITupleReference and vice versa.
 * 
 */
public class FunctionTupleTranslator extends AbstractTupleTranslator<Function> {
	// Field indexes of serialized Function in a tuple.
	// First key field.
	public static final int FUNCTION_DATAVERSENAME_TUPLE_FIELD_INDEX = 0;
	// Second key field.
	public static final int FUNCTION_FUNCTIONNAME_TUPLE_FIELD_INDEX = 1;
	// Thirdy key field.
	public static final int FUNCTION_FUNCTIONARITY_TUPLE_FIELD_INDEX = 2;

	// Payload field containing serialized Function.
	public static final int FUNCTION_PAYLOAD_TUPLE_FIELD_INDEX = 3;

	@SuppressWarnings("unchecked")
	private ISerializerDeserializer<ARecord> recordSerDes = AqlSerializerDeserializerProvider.INSTANCE
			.getSerializerDeserializer(MetadataRecordTypes.FUNCTION_RECORDTYPE);

	public FunctionTupleTranslator(boolean getTuple) {
		super(getTuple, MetadataPrimaryIndexes.FUNCTION_DATASET.getFieldCount());
	}

	@Override
	public Function getMetadataEntytiFromTuple(ITupleReference frameTuple)
			throws IOException {
		byte[] serRecord = frameTuple
				.getFieldData(FUNCTION_PAYLOAD_TUPLE_FIELD_INDEX);
		int recordStartOffset = frameTuple
				.getFieldStart(FUNCTION_PAYLOAD_TUPLE_FIELD_INDEX);
		int recordLength = frameTuple
				.getFieldLength(FUNCTION_PAYLOAD_TUPLE_FIELD_INDEX);
		ByteArrayInputStream stream = new ByteArrayInputStream(serRecord,
				recordStartOffset, recordLength);
		DataInput in = new DataInputStream(stream);
		ARecord functionRecord = (ARecord) recordSerDes.deserialize(in);
		return createFunctionFromARecord(functionRecord);
	}

	private Function createFunctionFromARecord(ARecord functionRecord) {
		String dataverseName = ((AString) functionRecord
				.getValueByPos(MetadataRecordTypes.FUNCTION_ARECORD_DATAVERSENAME_FIELD_INDEX))
				.getStringValue();
		String functionName = ((AString) functionRecord
				.getValueByPos(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTIONNAME_FIELD_INDEX))
				.getStringValue();
		String arity = ((AString) functionRecord
				.getValueByPos(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTIONARITY_FIELD_INDEX))
				.getStringValue();

		IACursor cursor = ((AOrderedList) functionRecord
				.getValueByPos(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_PARAM_LIST_FIELD_INDEX))
				.getCursor();
		List<String> params = new ArrayList<String>();
		while (cursor.next()) {
			params.add(((AString) cursor.get()).getStringValue());
		}

		String functionBody = ((AString) functionRecord
				.getValueByPos(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_BODY_FIELD_INDEX))
				.getStringValue();

		return new Function(dataverseName, functionName,
				Integer.parseInt(arity), params, functionBody);

	}

	@Override
	public ITupleReference getTupleFromMetadataEntity(Function function)
			throws IOException {
		// write the key in the first 3 fields of the tuple
		tupleBuilder.reset();
		aString.setValue(function.getDataverseName());
		stringSerde.serialize(aString, tupleBuilder.getDataOutput());
		tupleBuilder.addFieldEndOffset();
		aString.setValue(function.getFunctionName());
		stringSerde.serialize(aString, tupleBuilder.getDataOutput());
		tupleBuilder.addFieldEndOffset();
		aString.setValue("" + function.getFunctionArity());
		stringSerde.serialize(aString, tupleBuilder.getDataOutput());
		tupleBuilder.addFieldEndOffset();

		// write the pay-load in the fourth field of the tuple

		recordBuilder.reset(MetadataRecordTypes.FUNCTION_RECORDTYPE);

		// write field 0
		fieldValue.reset();
		aString.setValue(function.getDataverseName());
		stringSerde.serialize(aString, fieldValue.getDataOutput());
		recordBuilder.addField(
				MetadataRecordTypes.FUNCTION_ARECORD_DATAVERSENAME_FIELD_INDEX,
				fieldValue);

		// write field 1
		fieldValue.reset();
		aString.setValue(function.getFunctionName());
		stringSerde.serialize(aString, fieldValue.getDataOutput());
		recordBuilder.addField(
				MetadataRecordTypes.FUNCTION_ARECORD_FUNCTIONNAME_FIELD_INDEX,
				fieldValue);

		// write field 2
		fieldValue.reset();
		aString.setValue("" + function.getFunctionArity());
		stringSerde.serialize(aString, fieldValue.getDataOutput());
		recordBuilder.addField(
				MetadataRecordTypes.FUNCTION_ARECORD_FUNCTIONARITY_FIELD_INDEX,
				fieldValue);

		// write field 3
		IAOrderedListBuilder listBuilder = new OrderedListBuilder();
		ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();
		listBuilder
				.reset((AOrderedListType) MetadataRecordTypes.FUNCTION_RECORDTYPE
						.getFieldTypes()[MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_PARAM_LIST_FIELD_INDEX]);
		for (String param : function.getParams()) {
			itemValue.reset();
			aString.setValue(param);
			stringSerde.serialize(aString, itemValue.getDataOutput());
			listBuilder.addItem(itemValue);
		}
		fieldValue.reset();
		listBuilder.write(fieldValue.getDataOutput(), true);
		recordBuilder
				.addField(
						MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_PARAM_LIST_FIELD_INDEX,
						fieldValue);

		// write field 4
		fieldValue.reset();
		aString.setValue(function.getFunctionBody());
		stringSerde.serialize(aString, fieldValue.getDataOutput());
		recordBuilder.addField(
				MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_BODY_FIELD_INDEX,
				fieldValue);

		// write record
		recordBuilder.write(tupleBuilder.getDataOutput(), true);
		tupleBuilder.addFieldEndOffset();

		tuple.reset(tupleBuilder.getFieldEndOffsets(),
				tupleBuilder.getByteArray());
		return tuple;
	}

}