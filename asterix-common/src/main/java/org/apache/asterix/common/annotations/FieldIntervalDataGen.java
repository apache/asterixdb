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
package org.apache.asterix.common.annotations;

public class FieldIntervalDataGen implements IRecordFieldDataGen {

    public enum ValueType {
        INT,
        LONG,
        FLOAT,
        DOUBLE
    }

    private final String min;
    private final String max;
    private final ValueType valueType;

    public FieldIntervalDataGen(ValueType valueType, String min, String max) {
        this.valueType = valueType;
        this.min = min;
        this.max = max;
    }

    @Override
    public Kind getKind() {
        return Kind.INTERVAL;
    }

    public String getMin() {
        return min;
    }

    public String getMax() {
        return max;
    }

    public ValueType getValueType() {
        return valueType;
    }

}
