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
        return new IPropertyInterpreter<Integer>() {
            @Override
            public Integer interpret(String s) throws IllegalArgumentException {
                try {
                    return Integer.parseInt(s);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(e);
                }
            }
        };
    }

    public static IPropertyInterpreter<Boolean> getBooleanPropertyInterpreter() {
        return new IPropertyInterpreter<Boolean>() {
            @Override
            public Boolean interpret(String s) throws IllegalArgumentException {
                return Boolean.parseBoolean(s);
            }
        };
    }

    public static IPropertyInterpreter<Long> getLongPropertyInterpreter() {
        return new IPropertyInterpreter<Long>() {
            @Override
            public Long interpret(String s) throws IllegalArgumentException {
                try {
                    return Long.parseLong(s);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(e);
                }
            }
        };
    }

    public static IPropertyInterpreter<Level> getLevelPropertyInterpreter() {
        return new IPropertyInterpreter<Level>() {
            @Override
            public Level interpret(String s) throws IllegalArgumentException {
                return Level.parse(s);
            }
        };
    }

    public static IPropertyInterpreter<String> getStringPropertyInterpreter() {
        return new IPropertyInterpreter<String>() {
            @Override
            public String interpret(String s) throws IllegalArgumentException {
                return s;
            }
        };
    }

    public static IPropertyInterpreter<Double> getDoublePropertyInterpreter() {
        return new IPropertyInterpreter<Double>() {
            @Override
            public Double interpret(String s) throws IllegalArgumentException {
                try {
                    return Double.parseDouble(s);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(e);
                }
            }
        };
    }

    public static IPropertyInterpreter<Long> getLongBytePropertyInterpreter() {
        return new IPropertyInterpreter<Long>() {
            @Override
            public Long interpret(String s) throws IllegalArgumentException {
                try {
                    return StorageUtil.getByteValue(s);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(e);
                }
            }
        };
    }

    public static IPropertyInterpreter<Integer> getIntegerBytePropertyInterpreter() {
        return new IPropertyInterpreter<Integer>() {
            @Override
            public Integer interpret(String s) throws IllegalArgumentException {
                try {
                    return (int) StorageUtil.getByteValue(s);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(e);
                }
            }
        };
    }

}
