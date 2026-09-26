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
package org.apache.asterix.common.api;

import java.util.function.Function;

import org.apache.asterix.common.config.ConfigConstraints;
import org.apache.hyracks.api.config.IOption;

public interface IConfigValidator {

    /**
     * Validates that {@code value} is a valid value for {@code option}
     *
     * @param option
     * @param value
     */
    void validate(IOption option, Object value);

    /**
     * Validates the constraints that span several options, which {@link #validate(IOption, Object)} cannot see,
     * for a change from {@code current} to {@code proposed}. Only violations the change introduces are rejected, so
     * a configuration that is already inconsistent can still be corrected one option at a time.
     *
     * @param current the value each option takes now
     * @param proposed the value each option would take after the change
     * @throws IllegalArgumentException naming every violated constraint that {@code current} does not violate
     */
    default void validateChange(Function<IOption, Object> current, Function<IOption, Object> proposed) {
        ConfigConstraints.validateChange(current, proposed);
    }
}
