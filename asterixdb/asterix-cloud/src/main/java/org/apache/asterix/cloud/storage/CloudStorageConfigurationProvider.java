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
package org.apache.asterix.cloud.storage;

import java.io.File;

import org.apache.hyracks.api.exceptions.HyracksDataException;

public class CloudStorageConfigurationProvider {

    public static CloudStorageConfigurationProvider INSTANCE = new CloudStorageConfigurationProvider();

    private CloudStorageConfigurationProvider() {
    }

    public ICloudStorageConfiguration getConfiguration(ICloudStorageConfiguration.ConfigurationType type)
            throws HyracksDataException {
        switch (type) {
            case FILE:
                File file = new File("/etc/storage");
                if (file.exists()) {
                    return new FileCloudStorageConfiguration(file);
                }
            default:
                return MockCloudStorageConfiguration.INSTANCE;
        }
    }
}
