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
package org.apache.hyracks.api.config;

import java.util.function.Function;

import org.apache.hyracks.util.StringUtil;

public interface IOption {

    String name();

    Section section();

    String description();

    IOptionType type();

    /**
     * @return the unresolved default value of this option-
     */
    Object defaultValue();

    /**
     * @return a string to describe the default value, or null if the default should be used
     */
    default String usageDefaultOverride(IApplicationConfig accessor, Function<IOption, String> optionPrinter) {
        return null;
    }

    /**
     * Implementations should override this default implementation if this property value is non-static and should be
     * calculated on every call
     * @return the current value of this property
     */
    default Object get(IApplicationConfig appConfig) {
        return appConfig.getStatic(this);
    }

    /**
     * @return a true value indicates this option should not be advertised (e.g. command-line usage, documentation)
     */
    default boolean hidden() {
        return false;
    }

    default String cmdline() {
        return "-" + name().toLowerCase().replace("_", "-");
    }

    default String ini() {
        return toIni(name());
    }

    default String camelCase() {
        return StringUtil.toCamelCase(name());
    }

    default String toIniString() {
        return "[" + section().sectionName() + "] " + ini();
    }

    static String toIni(String name) {
        return name.toLowerCase().replace("_", ".");
    }

    default SerializedOption toSerializable() {
        return new SerializedOption(this);
    }
}
