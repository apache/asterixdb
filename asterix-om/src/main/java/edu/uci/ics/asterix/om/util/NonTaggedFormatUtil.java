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
package edu.uci.ics.asterix.om.util;

import java.util.List;

import edu.uci.ics.asterix.common.config.DatasetConfig.IndexType;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AOrderedListSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ARecordSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AUnorderedListSerializerDeserializer;
import edu.uci.ics.asterix.formats.nontagged.AqlBinaryComparatorFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlBinaryTokenizerFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlTypeTraitProvider;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.AUnorderedListType;
import edu.uci.ics.asterix.om.types.AbstractCollectionType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;

public final class NonTaggedFormatUtil {

    public static final int OPTIONAL_TYPE_INDEX_IN_UNION_LIST = 1;

    public static final boolean isFixedSizedCollection(IAType type) {
        switch (type.getTypeTag()) {
            case STRING:
            case RECORD:
            case ORDEREDLIST:
            case UNORDEREDLIST:
            case ANY:
                return false;
            case UNION:
                if (!NonTaggedFormatUtil.isOptionalField((AUnionType) type))
                    return false;
                else
                    return isFixedSizedCollection(((AUnionType) type).getUnionList().get(
                            OPTIONAL_TYPE_INDEX_IN_UNION_LIST));
            default:
                return true;
        }
    }

    public static final boolean hasNullableField(ARecordType recType) {
        IAType type;
        List<IAType> unionList;
        for (int i = 0; i < recType.getFieldTypes().length; i++) {
            type = recType.getFieldTypes()[i];
            if (type != null) {
                if (type.getTypeTag() == ATypeTag.NULL)
                    return true;
                if (type.getTypeTag() == ATypeTag.UNION) { // union
                    unionList = ((AUnionType) type).getUnionList();
                    for (int j = 0; j < unionList.size(); j++)
                        if (unionList.get(j).getTypeTag() == ATypeTag.NULL)
                            return true;

                }
            }
        }
        return false;
    }

    public static boolean isOptionalField(AUnionType unionType) {
        if (unionType.getUnionList().size() == 2)
            if (unionType.getUnionList().get(0).getTypeTag() == ATypeTag.NULL)
                return true;
        return false;
    }

    public static int getFieldValueLength(byte[] serNonTaggedAObject, int offset, ATypeTag typeTag, boolean tagged)
            throws AsterixException {
        switch (typeTag) {
            case ANY:
                ATypeTag tag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serNonTaggedAObject[offset]);
                if (tag == ATypeTag.ANY) {
                    throw new AsterixException("Field value has type tag ANY, but it should have a concrete type.");
                }
                return getFieldValueLength(serNonTaggedAObject, offset, tag, true) + 1;
            case NULL:
                return 0;
            case BOOLEAN:
            case INT8:
                return 1;
            case INT16:
                return 2;
            case INT32:
            case FLOAT:
            case DATE:
            case YEARMONTHDURATION:
                return 4;
            case TIME:
                return 4;
            case INT64:
            case DOUBLE:
            case DATETIME:
            case DAYTIMEDURATION:
                return 8;
            case DURATION:
                return 12;
            case POINT:
                return 16;
            case INTERVAL:
                return 17;
            case POINT3D:
            case CIRCLE:
                return 24;
            case LINE:
            case RECTANGLE:
                return 32;
            case POLYGON:
                if (tagged)
                    return AInt16SerializerDeserializer.getShort(serNonTaggedAObject, offset + 1) * 16 + 2;
                else
                    return AInt16SerializerDeserializer.getShort(serNonTaggedAObject, offset) * 16 + 2;
            case STRING:
                if (tagged)
                    return AInt16SerializerDeserializer.getUnsignedShort(serNonTaggedAObject, offset + 1) + 2;
                else
                    return AInt16SerializerDeserializer.getUnsignedShort(serNonTaggedAObject, offset) + 2;
            case RECORD:
                if (tagged)
                    return ARecordSerializerDeserializer.getRecordLength(serNonTaggedAObject, offset + 1) - 1;
                else
                    return ARecordSerializerDeserializer.getRecordLength(serNonTaggedAObject, offset) - 1;
            case ORDEREDLIST:
                if (tagged)
                    return AOrderedListSerializerDeserializer.getOrderedListLength(serNonTaggedAObject, offset + 1) - 1;
                else
                    return AOrderedListSerializerDeserializer.getOrderedListLength(serNonTaggedAObject, offset) - 1;
            case UNORDEREDLIST:
                if (tagged)
                    return AUnorderedListSerializerDeserializer.getUnorderedListLength(serNonTaggedAObject, offset + 1) - 1;
                else
                    return AUnorderedListSerializerDeserializer.getUnorderedListLength(serNonTaggedAObject, offset) - 1;
            default:
                throw new NotImplementedException("No getLength implemented for a value of this type " + typeTag + " .");
        }
    }

    public static int getNumDimensions(ATypeTag typeTag) {
        switch (typeTag) {
            case POINT:
            case LINE:
            case POLYGON:
            case CIRCLE:
            case RECTANGLE:
                return 2;
            case POINT3D:
                return 3;
            default:
                throw new NotImplementedException("getNumDimensions is not implemented for this type " + typeTag + " .");
        }
    }

    public static IAType getNestedSpatialType(ATypeTag typeTag) {
        switch (typeTag) {
            case POINT:
            case LINE:
            case POLYGON:
            case CIRCLE:
            case RECTANGLE:
                return BuiltinType.ADOUBLE;
            default:
                throw new NotImplementedException(typeTag + " is not a supported spatial data type.");
        }
    }

    public static IBinaryTokenizerFactory getBinaryTokenizerFactory(ATypeTag keyType, IndexType indexType,
            int gramLength) throws AlgebricksException {
        switch (indexType) {
            case SINGLE_PARTITION_WORD_INVIX:
            case LENGTH_PARTITIONED_WORD_INVIX: {
                return AqlBinaryTokenizerFactoryProvider.INSTANCE.getWordTokenizerFactory(keyType, false);
            }
            case SINGLE_PARTITION_NGRAM_INVIX:
            case LENGTH_PARTITIONED_NGRAM_INVIX: {
                return AqlBinaryTokenizerFactoryProvider.INSTANCE.getNGramTokenizerFactory(keyType, gramLength, true,
                        false);
            }
            default: {
                throw new AlgebricksException("Tokenizer not applicable to index type '" + indexType + "'.");
            }
        }
    }

    public static IBinaryComparatorFactory getTokenBinaryComparatorFactory(IAType keyType) throws AlgebricksException {
        IAType type = keyType;
        ATypeTag typeTag = keyType.getTypeTag();
        // Extract item type from list.
        if (typeTag == ATypeTag.UNORDEREDLIST || typeTag == ATypeTag.ORDEREDLIST) {
            AbstractCollectionType listType = (AbstractCollectionType) keyType;
            if (!listType.isTyped()) {
                throw new AlgebricksException("Cannot build an inverted index on untyped lists.)");
            }
            type = listType.getItemType();
        }
        // Ignore case for string types.
        return AqlBinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(type, true, true);
    }

    public static ITypeTraits getTokenTypeTrait(IAType keyType) throws AlgebricksException {
        IAType type = keyType;
        ATypeTag typeTag = keyType.getTypeTag();
        // Extract item type from list.
        if (typeTag == ATypeTag.UNORDEREDLIST) {
            AUnorderedListType ulistType = (AUnorderedListType) keyType;
            if (!ulistType.isTyped()) {
                throw new AlgebricksException("Cannot build an inverted index on untyped lists.)");
            }
            type = ulistType.getItemType();
        }
        if (typeTag == ATypeTag.ORDEREDLIST) {
            AOrderedListType olistType = (AOrderedListType) keyType;
            if (!olistType.isTyped()) {
                throw new AlgebricksException("Cannot build an inverted index on untyped lists.)");
            }
            type = olistType.getItemType();
        }
        return AqlTypeTraitProvider.INSTANCE.getTypeTrait(type);
    }
}
