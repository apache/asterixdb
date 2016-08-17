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
package org.apache.asterix.common.config;

import java.util.logging.Level;

import org.apache.hyracks.util.StorageUtil;

public class PropertyInterpreters {

    public static IPropertyInterpreter<Integer> getIntegerPropertyInterpreter() {
        return Integer::parseInt;
    }

    public static IPropertyInterpreter<Boolean> getBooleanPropertyInterpreter() {
        return Boolean::parseBoolean;
    }

    public static IPropertyInterpreter<Long> getLongPropertyInterpreter() {
        return Long::parseLong;
    }

    public static IPropertyInterpreter<Level> getLevelPropertyInterpreter() {
        return Level::parse;
    }

    public static IPropertyInterpreter<String> getStringPropertyInterpreter() {
        return s -> s;
    }

    public static IPropertyInterpreter<Double> getDoublePropertyInterpreter() {
        return Double::parseDouble;
    }

    public static IPropertyInterpreter<Long> getLongBytePropertyInterpreter() {
        return StorageUtil::getByteValue;
    }

    public static IPropertyInterpreter<Integer> getIntegerBytePropertyInterpreter() {
        return s -> {
                long result = StorageUtil.getByteValue(s);
            if (result > Integer.MAX_VALUE || result < Integer.MIN_VALUE) {
                throw new IllegalArgumentException(
                        "The given value: " + result + " is not within the int range.");
            } else {
                return (int) result;
            }
        };
    }

}
