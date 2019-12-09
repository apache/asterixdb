/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.runtime.evaluators.functions;

import static org.apache.asterix.om.types.EnumDeserializer.ATYPETAGDESERIALIZER;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;

import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.util.LogRedactionUtil;
import org.apache.hyracks.util.string.UTF8StringWriter;

/**
 * An utility class for some frequently used methods like checking the equality between two pointables (binary values)
 * (e.g., field names), string value of a fieldname pointable, getting the typetag of a pointable, etc.
 * Note: To get the typetag of a fieldvalue (i) in a record, it is recommended to use the getFieldTypeTags().get(i)
 * method rather than getting it from fhe field value itself.
 */

public class PointableHelper {

    // represents the possible value states for a pointable
    private enum PointableValueState {
        EMPTY_POINTABLE,
        MISSING,
        NULL,
        PRESENT
    }

    private static final byte[] NULL_BYTES = new byte[] { ATypeTag.SERIALIZED_NULL_TYPE_TAG };
    private static final byte[] MISSING_BYTES = new byte[] { ATypeTag.SERIALIZED_MISSING_TYPE_TAG };
    private final UTF8StringWriter utf8Writer;

    public static final IPointable NULL_REF = new VoidPointable();
    static {
        NULL_REF.set(NULL_BYTES, 0, NULL_BYTES.length);
    }

    public PointableHelper() {
        utf8Writer = new UTF8StringWriter();
    }

    public static int compareStringBinValues(IValueReference a, IValueReference b, IBinaryComparator comparator)
            throws HyracksDataException {
        // start+1 and len-1 due to type tag ignore (only interested in String value)
        return comparator.compare(a.getByteArray(), a.getStartOffset() + 1, a.getLength() - 1, b.getByteArray(),
                b.getStartOffset() + 1, b.getLength() - 1);
    }

    public static boolean isEqual(IValueReference a, IValueReference b, IBinaryComparator comparator)
            throws HyracksDataException {
        return compareStringBinValues(a, b, comparator) == 0;
    }

    public static boolean byteArrayEqual(IValueReference valueRef1, IValueReference valueRef2) {
        return byteArrayEqual(valueRef1, valueRef2, 3);
    }

    public static boolean byteArrayEqual(IValueReference valueRef1, IValueReference valueRef2, int dataOffset) {
        if (valueRef1 == null || valueRef2 == null) {
            return false;
        }
        if (valueRef1 == valueRef2) {
            return true;
        }

        int length1 = valueRef1.getLength();
        int length2 = valueRef2.getLength();

        if (length1 != length2) {
            return false;
        }

        byte[] bytes1 = valueRef1.getByteArray();
        byte[] bytes2 = valueRef2.getByteArray();
        int start1 = valueRef1.getStartOffset() + dataOffset;
        int start2 = valueRef2.getStartOffset() + dataOffset;

        int end = start1 + length1 - dataOffset;

        for (int i = start1, j = start2; i < end; i++, j++) {
            if (bytes1[i] != bytes2[j]) {
                return false;
            }
        }

        return true;
    }

    public static boolean sameType(ATypeTag typeTag, IVisitablePointable visitablePointable) {
        return (getTypeTag(visitablePointable) == typeTag);
    }

    public static ATypeTag getTypeTag(IValueReference visitablePointable) {
        byte[] bytes = visitablePointable.getByteArray();
        int s = visitablePointable.getStartOffset();
        return EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes[s]);
    }

    public static <T extends IValueReference> T findItem(IValueReference item, Collection<T> list,
            IBinaryComparator comparator) throws HyracksDataException {
        for (T listItem : list) {
            if (comparator.compare(item.getByteArray(), item.getStartOffset(), item.getLength(),
                    listItem.getByteArray(), listItem.getStartOffset(), listItem.getLength()) == 0) {
                return listItem;
            }
        }
        return null;
    }

    /**
     * @param str
     *            The input string
     * @param vs
     *            The storage buffer
     * @param writeTag
     *            Specifying whether a tag for the string should also be written
     */
    public void serializeString(String str, IMutableValueStorage vs, boolean writeTag) throws HyracksDataException {
        vs.reset();
        try {
            DataOutput output = vs.getDataOutput();
            if (writeTag) {
                output.write(ATypeTag.STRING.serialize());
            }
            utf8Writer.writeUTF8(str, output);
        } catch (IOException e) {
            throw new HyracksDataException("Could not serialize " + LogRedactionUtil.userData(str));
        }
    }

    public static void setNull(IPointable pointable) {
        pointable.set(NULL_BYTES, 0, NULL_BYTES.length);
    }

    public static void setMissing(IPointable pointable) {
        pointable.set(MISSING_BYTES, 0, MISSING_BYTES.length);
    }

    // 1 pointable check
    public static boolean checkAndSetMissingOrNull(IPointable result, IPointable pointable1)
            throws HyracksDataException {
        return checkAndSetMissingOrNull(result, null, pointable1, null, null, null);
    }

    // 2 pointables check
    public static boolean checkAndSetMissingOrNull(IPointable result, IPointable pointable1, IPointable pointable2)
            throws HyracksDataException {
        return checkAndSetMissingOrNull(result, null, pointable1, pointable2, null, null);
    }

    // 3 pointables check
    public static boolean checkAndSetMissingOrNull(IPointable result, IPointable pointable1, IPointable pointable2,
            IPointable pointable3) throws HyracksDataException {
        return checkAndSetMissingOrNull(result, null, pointable1, pointable2, pointable3, null);
    }

    // 4 pointables check
    public static boolean checkAndSetMissingOrNull(IPointable result, IPointable pointable1, IPointable pointable2,
            IPointable pointable3, IPointable pointable4) throws HyracksDataException {
        return checkAndSetMissingOrNull(result, null, pointable1, pointable2, pointable3, pointable4);
    }

    // 1 pointable check (check list members for missing values)
    public static boolean checkAndSetMissingOrNull(IPointable result, ListAccessor listAccessor, IPointable pointable1)
            throws HyracksDataException {
        return checkAndSetMissingOrNull(result, listAccessor, pointable1, null, null, null);
    }

    // 2 pointables check (check list members for missing values)
    public static boolean checkAndSetMissingOrNull(IPointable result, ListAccessor listAccessor, IPointable pointable1,
            IPointable pointable2) throws HyracksDataException {
        return checkAndSetMissingOrNull(result, listAccessor, pointable1, pointable2, null, null);
    }

    // 3 pointables check (check list members for missing values)
    public static boolean checkAndSetMissingOrNull(IPointable result, ListAccessor listAccessor, IPointable pointable1,
            IPointable pointable2, IPointable pointable3) throws HyracksDataException {
        return checkAndSetMissingOrNull(result, listAccessor, pointable1, pointable2, pointable3, null);
    }

    /**
     * This method takes multiple pointables, the first pointable being the pointable to write the result to, and
     * checks their ATypeTag value. If a missing or null ATypeTag is encountered, the method will set the result
     * pointable to missing or null accordingly, and will return {@code true}.
     *
     * As the missing encounter has a higher priority than the null, the method will keep checking if any missing has
     * been encountered first, if not, it will do a null check at the end.
     *
     * If the listAccessor is passed, this method will also go through any list pointable elements and search for
     * a missing value to give it a higher priority over null values. If {@code null} is passed for the listAccessor,
     * the list element check will be skipped.
     *
     * @param result the result pointable that will hold the data
     * @param listAccessor list accessor to use for check list elements.
     * @param pointable1 the first pointable to be checked
     * @param pointable2 the second pointable to be checked
     * @param pointable3 the third pointable to be checked
     * @param pointable4 the fourth pointable to be checked
     *
     * @return {@code true} if the pointable value is missing or null, {@code false} otherwise.
     */
    public static boolean checkAndSetMissingOrNull(IPointable result, ListAccessor listAccessor, IPointable pointable1,
            IPointable pointable2, IPointable pointable3, IPointable pointable4) throws HyracksDataException {

        // this flag will keep an eye on whether a null value is encountered or not
        boolean isMeetNull = false;

        switch (getPointableValueState(pointable1, listAccessor)) {
            case MISSING:
                setMissing(result);
                return true;
            case NULL:
                isMeetNull = true;
                break;
        }

        if (pointable2 != null) {
            switch (getPointableValueState(pointable2, listAccessor)) {
                case MISSING:
                    setMissing(result);
                    return true;
                case NULL:
                    isMeetNull = true;
                    break;
            }
        }

        if (pointable3 != null) {
            switch (getPointableValueState(pointable3, listAccessor)) {
                case MISSING:
                    setMissing(result);
                    return true;
                case NULL:
                    isMeetNull = true;
                    break;
            }
        }

        if (pointable4 != null) {
            switch (getPointableValueState(pointable4, listAccessor)) {
                case MISSING:
                    setMissing(result);
                    return true;
                case NULL:
                    isMeetNull = true;
                    break;
            }
        }

        // this is reached only if no missing is encountered in all the passed pointables
        if (isMeetNull) {
            setNull(result);
            return true;
        }

        // no missing or null encountered
        return false;
    }

    /**
     * This method checks and returns the pointable value state.
     *
     * If a ListAccessor is passed to this function, it will check if the passed pointable is a list, and if so, it
     * will search for a missing value inside the list before checking for null values. If the listAccessor value is
     * null, no list elements check will be performed.
     *
     * @param pointable the pointable to be checked
     * @param listAccessor list accessor used to check the list elements.
     *
     * @return the pointable value state for the passed pointable
     */
    private static PointableValueState getPointableValueState(IPointable pointable, ListAccessor listAccessor)
            throws HyracksDataException {
        if (pointable.getLength() == 0) {
            return PointableValueState.EMPTY_POINTABLE;
        }

        byte[] bytes = pointable.getByteArray();
        int offset = pointable.getStartOffset();
        ATypeTag typeTag = ATYPETAGDESERIALIZER.deserialize(bytes[offset]);

        if (typeTag == ATypeTag.MISSING) {
            return PointableValueState.MISSING;
        }

        if (typeTag == ATypeTag.NULL) {
            return PointableValueState.NULL;
        }

        boolean isNull = false;

        // Check the list elements first as it may have a missing that needs to be reported first
        if (listAccessor != null && typeTag.isListType()) {
            listAccessor.reset(bytes, offset);

            for (int i = 0; i < listAccessor.size(); i++) {
                if (listAccessor.getItemType(listAccessor.getItemOffset(i)) == ATypeTag.MISSING) {
                    return PointableValueState.MISSING;
                }

                if (listAccessor.getItemType(listAccessor.getItemOffset(i)) == ATypeTag.NULL) {
                    isNull = true;
                }
            }
        }

        if (isNull) {
            return PointableValueState.NULL;
        }

        return PointableValueState.PRESENT;
    }

    /**
     * Check if the provided bytes are of valid long type. In case floats and doubles are accepted, the accepted
     * values will be 1.0 and 2.0, but not 2.5. (only zero decimals)
     *
     * @param bytes data bytes
     * @param startOffset start offset
     * @param acceptFloatAndDouble flag to accept float and double values or not
     *
     * @return true if provided value is a valid long, false otherwise
     */
    public static boolean isValidLongValue(byte[] bytes, int startOffset, boolean acceptFloatAndDouble) {

        // Type tag
        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes[startOffset]);

        // If floats and doubles aren't allowed, we only check if it can be int64
        if (!acceptFloatAndDouble) {
            return ATypeHierarchy.canPromote(typeTag, ATypeTag.BIGINT);
        }

        // We accept floats and doubles, do all the checks
        if (!ATypeHierarchy.canPromote(typeTag, ATypeTag.DOUBLE)) {
            return false;
        }

        // Float check (1.0, 2.0 are fine, but 1.5 is not)
        if (typeTag == ATypeTag.FLOAT) {
            float value = AFloatSerializerDeserializer.getFloat(bytes, startOffset + 1);

            // Max and min checks, has a decimal value that is not 0
            if (value > Long.MAX_VALUE || value < Long.MIN_VALUE || value > Math.floor(value) || Float.isNaN(value)) {
                return false;
            }
        }

        // Double check (1.0, 2.0 are fine, but 1.5 is not)
        if (typeTag == ATypeTag.DOUBLE) {
            double value = ADoubleSerializerDeserializer.getDouble(bytes, startOffset + 1);

            // Max and min checks, has a decimal value that is not 0
            if (value > Long.MAX_VALUE || value < Long.MIN_VALUE || value > Math.floor(value) || Double.isNaN(value)) {
                return false;
            }
        }

        return true;
    }
}
