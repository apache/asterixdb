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

package edu.uci.ics.asterix.metadata.entitytupletranslators;

import edu.uci.ics.asterix.builders.IARecordBuilder;
import edu.uci.ics.asterix.builders.RecordBuilder;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.metadata.api.IMetadataEntityTupleTranslator;
import edu.uci.ics.asterix.om.base.ABoolean;
import edu.uci.ics.asterix.om.base.AMutableString;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;

/**
 * Contains common members shared across all concrete implementations of
 * IMetadataEntityTupleTranslator.
 */
public abstract class AbstractTupleTranslator<T> implements IMetadataEntityTupleTranslator<T> {
    protected AMutableString aString = new AMutableString("");
    @SuppressWarnings("unchecked")
    protected ISerializerDeserializer<AString> stringSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ASTRING);
    @SuppressWarnings("unchecked")
    protected ISerializerDeserializer<ABoolean> booleanSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ABOOLEAN);
    protected final IARecordBuilder recordBuilder;
    protected final ArrayBackedValueStorage fieldValue;
    protected final ArrayTupleBuilder tupleBuilder;
    protected final ArrayTupleReference tuple;

    public AbstractTupleTranslator(boolean getTuple, int fieldCount) {
        if (getTuple) {
            recordBuilder = new RecordBuilder();
            fieldValue = new ArrayBackedValueStorage();
            tupleBuilder = new ArrayTupleBuilder(fieldCount);
            tuple = new ArrayTupleReference();
        } else {
            recordBuilder = null;
            fieldValue = null;
            tupleBuilder = null;
            tuple = null;
        }
    }
}
