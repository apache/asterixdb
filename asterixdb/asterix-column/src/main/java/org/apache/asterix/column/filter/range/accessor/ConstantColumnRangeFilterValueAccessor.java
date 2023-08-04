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

import org.apache.asterix.column.filter.range.IColumnRangeFilterValueAccessor;
import org.apache.asterix.om.types.ATypeTag;

public final class ConstantColumnRangeFilterValueAccessor implements IColumnRangeFilterValueAccessor {
    private final long normalizedValue;
    private final ATypeTag typeTag;

    //TODO add UUID

    public ConstantColumnRangeFilterValueAccessor(long normalizedValue, ATypeTag typeTag) {
        this.normalizedValue = normalizedValue;
        this.typeTag = typeTag;
    }

    @Override
    public long getNormalizedValue() {
        return normalizedValue;
    }

    @Override
    public ATypeTag getTypeTag() {
        return typeTag;
    }

    @Override
    public String toString() {
        return Long.toString(normalizedValue);
    }
}
