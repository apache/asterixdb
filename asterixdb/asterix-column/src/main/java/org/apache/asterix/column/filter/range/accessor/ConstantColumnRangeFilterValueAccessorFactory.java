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
package org.apache.asterix.column.filter.range.accessor;

import java.util.Set;

import org.apache.asterix.column.filter.FilterAccessorProvider;
import org.apache.asterix.column.filter.range.IColumnRangeFilterValueAccessor;
import org.apache.asterix.column.filter.range.IColumnRangeFilterValueAccessorFactory;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ConstantColumnRangeFilterValueAccessorFactory implements IColumnRangeFilterValueAccessorFactory {
    private static final long serialVersionUID = -4835407779342615453L;
    public static final Set<ATypeTag> SUPPORTED_CONSTANT_TYPES;
    private final long normalizedValue;
    private final ATypeTag typeTag;
    private final String stringValue;

    static {
        SUPPORTED_CONSTANT_TYPES = Set.of(ATypeTag.BOOLEAN, ATypeTag.BIGINT, ATypeTag.DOUBLE, ATypeTag.STRING);
    }

    private ConstantColumnRangeFilterValueAccessorFactory(String stringValue, long normalizedValue, ATypeTag typeTag) {
        this.stringValue = stringValue;
        this.normalizedValue = normalizedValue;
        this.typeTag = typeTag;
    }

    /**
     * Create a constant accessor
     *
     * @param value constant value
     * @return constant accessor factory if supported, null otherwise
     */
    public static ConstantColumnRangeFilterValueAccessorFactory createFactory(IAObject value) {
        String stringValue;
        long normalizedValue;
        ATypeTag typeTag = value.getType().getTypeTag();
        switch (typeTag) {
            case BOOLEAN:
                boolean booleanVal = ((ABoolean) value).getBoolean();
                stringValue = Boolean.toString(booleanVal);
                normalizedValue = booleanVal ? 1 : 0;
                break;
            case BIGINT:
                long longVal = ((AInt64) value).getLongValue();
                stringValue = Long.toString(longVal);
                normalizedValue = longVal;
                break;
            case DOUBLE:
                double doubleVal = ((ADouble) value).getDoubleValue();
                stringValue = Double.toString(doubleVal);
                normalizedValue = Double.doubleToLongBits(doubleVal);
                break;
            case STRING:
                stringValue = ((AString) value).getStringValue();
                normalizedValue = normalize(stringValue);
                break;
            default:
                return null;
        }

        return new ConstantColumnRangeFilterValueAccessorFactory(stringValue, normalizedValue, typeTag);
    }

    @Override
    public IColumnRangeFilterValueAccessor create(FilterAccessorProvider filterAccessorProvider)
            throws HyracksDataException {
        return new ConstantColumnRangeFilterValueAccessor(normalizedValue, typeTag);
    }

    @Override
    public String toString() {
        if (typeTag == ATypeTag.STRING) {
            return "\"" + stringValue + "\"";
        }
        return stringValue;
    }

    private static long normalize(String value) {
        long nk = 0;
        for (int i = 0; i < 4; ++i) {
            nk <<= 16;
            if (i < value.length()) {
                nk += value.charAt(i) & 0xffff;
            }
        }
        //Make it always positive
        return nk >>> 1;
    }
}
