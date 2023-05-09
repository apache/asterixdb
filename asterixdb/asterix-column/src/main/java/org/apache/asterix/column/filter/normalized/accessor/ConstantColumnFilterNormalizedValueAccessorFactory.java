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
package org.apache.asterix.column.filter.normalized.accessor;

import org.apache.asterix.column.filter.FilterAccessorProvider;
import org.apache.asterix.column.filter.normalized.IColumnFilterNormalizedValueAccessor;
import org.apache.asterix.column.filter.normalized.IColumnFilterNormalizedValueAccessorFactory;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ConstantColumnFilterNormalizedValueAccessorFactory implements IColumnFilterNormalizedValueAccessorFactory {
    private static final long serialVersionUID = -4835407779342615453L;
    private final long normalizedValue;
    private final ATypeTag typeTag;
    private final String stringValue;

    private ConstantColumnFilterNormalizedValueAccessorFactory(String stringValue, long normalizedValue,
            ATypeTag typeTag) {
        this.stringValue = stringValue;
        this.normalizedValue = normalizedValue;
        this.typeTag = typeTag;
    }

    public static ConstantColumnFilterNormalizedValueAccessorFactory createFactory(IAObject value) {
        String stringValue;
        long normalizedValue;
        ATypeTag typeTag = value.getType().getTypeTag();
        switch (typeTag) {
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

        return new ConstantColumnFilterNormalizedValueAccessorFactory(stringValue, normalizedValue, typeTag);
    }

    @Override
    public IColumnFilterNormalizedValueAccessor create(FilterAccessorProvider filterAccessorProvider)
            throws HyracksDataException {
        return new ConstantColumnFilterNormalizedValueAccessor(normalizedValue, typeTag);
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
