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
package org.apache.asterix.app.config;

import org.apache.asterix.common.api.IConfigValidator;
import org.apache.asterix.common.config.StorageProperties;
import org.apache.asterix.runtime.compression.CompressionManager;
import org.apache.hyracks.api.config.IOption;

public class ConfigValidator implements IConfigValidator {

    @Override
    public void validate(IOption option, Object value) {
        boolean valid = true;
        if (option == StorageProperties.Option.STORAGE_COMPRESSION_BLOCK) {
            valid = CompressionManager.isRegisteredScheme((String) value);
        }
        if (!valid) {
            throw new IllegalArgumentException("Invalid value " + value + " for option " + option.name());
        }
    }
}
