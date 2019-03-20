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
package org.apache.asterix.om.utils;

import java.util.List;

import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeTagUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class RecordUtil {
    /**
     * A fully open record type which has the name OpenRecord
     */
    public static final ARecordType FULLY_OPEN_RECORD_TYPE =
            new ARecordType("AnyObject", new String[0], new IAType[0], true);

    private RecordUtil() {
    }

    /**
     * @param field a field name represented by a list
     * @return string version of the field
     */
    public static String toFullyQualifiedName(List<String> field) {
        return StringUtils.join(field, ".");
    }

    /**
     * @param names a hierarchy of entity names
     * @return string version of the entity name to be qualified
     */
    public static String toFullyQualifiedName(String... names) {
        return StringUtils.join(names, ".");
    }

    /**
     * Computes the null Bitmap size when the schema has optional fields (nullable/missable)
     *
     * @param recordType the record type
     * @return the size of the bitmap in number of bytes
     */
    public static int computeNullBitmapSize(ARecordType recordType) {
        return computeNullBitmapSize(NonTaggedFormatUtil.hasOptionalField(recordType), recordType);
    }

    public static int computeNullBitmapSize(boolean hasOptionalField, ARecordType recordType) {
        // each field needs 2 bits for MISSING, NULL, and VALUE. for 4 fields, 4*2=8 bits (1 byte), thus divide by 4.
        return hasOptionalField ? (int) Math.ceil(recordType.getFieldTypes().length / 4.0) : 0;
    }

    public static boolean isNull(byte nullMissingBits, int fieldIndex) {
        int position = 1 << (7 - 2 * (fieldIndex % 4));
        return (nullMissingBits & position) == 0;
    }

    public static boolean isMissing(byte nullMissingBits, int fieldIndex) {
        int position = 1 << (7 - 2 * (fieldIndex % 4) - 1);
        return (nullMissingBits & position) == 0;
    }

    public static IAType getType(ARecordType recordType, int fieldIdx, ATypeTag fieldTag) throws HyracksDataException {
        IAType[] fieldTypes = recordType.getFieldTypes();
        if (fieldIdx >= fieldTypes.length) {
            return fieldTag.isDerivedType() ? DefaultOpenFieldType.getDefaultOpenFieldType(fieldTag)
                    : TypeTagUtil.getBuiltinTypeByTag(fieldTag);
        }
        return fieldTypes[fieldIdx];
    }
}
