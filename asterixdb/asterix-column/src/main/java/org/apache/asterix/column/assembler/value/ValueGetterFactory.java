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
package org.apache.asterix.column.assembler.value;

import org.apache.asterix.om.types.ATypeTag;

public class ValueGetterFactory implements IValueGetterFactory {
    public static final IValueGetterFactory INSTANCE = new ValueGetterFactory();

    private ValueGetterFactory() {
    }

    @Override
    public IValueGetter createValueGetter(ATypeTag typeTag) {
        switch (typeTag) {
            case NULL:
                return NullValueGetter.INSTANCE;
            case MISSING:
                return MissingValueGetter.INSTANCE;
            case BOOLEAN:
                return new BooleanValueGetter();
            case TINYINT:
                return new Int8ValueGetter();
            case SMALLINT:
                return new Int16ValueGetter();
            case INTEGER:
                return new Int32ValueGetter();
            case BIGINT:
                return new Int64ValueGetter();
            case FLOAT:
                return new FloatValueGetter();
            case DOUBLE:
                return new DoubleValueGetter();
            case STRING:
                return new StringValueGetter();
            case UUID:
                return new UUIDValueGetter();
            default:
                throw new UnsupportedOperationException(typeTag + " is not supported");
        }
    }
}
