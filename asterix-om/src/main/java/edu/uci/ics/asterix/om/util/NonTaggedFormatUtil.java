package edu.uci.ics.asterix.om.util;

import java.util.List;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AOrderedListSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ARecordSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AUnorderedListSerializerDeserializer;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;

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
                return 4;
            case TIME:
                return 4;
            case INT64:
            case DOUBLE:
            case DATETIME:
                return 8;
            case DURATION:
                return 12;
            case POINT:
                return 16;
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
}
