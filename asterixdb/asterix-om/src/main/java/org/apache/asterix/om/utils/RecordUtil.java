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

import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.commons.lang3.StringUtils;

public class RecordUtil {
    /**
     * A fully open record type which has the name OpenRecord
     */
    public static final ARecordType FULLY_OPEN_RECORD_TYPE =
            new ARecordType("OpenRecord", new String[0], new IAType[0], true);

    private RecordUtil() {
    }

    /**
     * Create a fully open record type with the passed name
     *
     * @param name
     * @return
     */
    public static ARecordType createOpenRecordType(String name) {
        return new ARecordType(name, new String[0], new IAType[0], true);
    }

    /**
     * A util method that takes a field name and return a String representation for error messages
     *
     * @param field
     * @return
     */
    public static String toFullyQualifiedName(List<String> field) {
        return StringUtils.join(field, ".");
    }

    /**
     * A util method that takes String array and return a String representation for error messages
     *
     * @param field
     * @return
     */
    public static String toFullyQualifiedName(String... names) {
        return StringUtils.join(names, ".");
    }

    /**
     * compute the null Bitmap size for the open fields
     *
     * @param recordType
     *            the record type
     * @return the size of the bitmap
     */
    public static int computeNullBitmapSize(ARecordType recordType) {
        return NonTaggedFormatUtil.hasOptionalField(recordType)
                ? (int) Math.ceil(recordType.getFieldNames().length / 4.0) : 0;
    }
}
