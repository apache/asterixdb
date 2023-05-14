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
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class FileCloudStorageConfiguration implements ICloudStorageConfiguration {

    private final String containerName;

    public FileCloudStorageConfiguration(File file) throws HyracksDataException {
        if (!file.exists()) {
            throw new IllegalStateException("No cloud configuration file found");
        }

        try {
            List<String> lines = FileUtils.readLines(file, "UTF-8");
            this.containerName = lines.get(0);
        } catch (IOException ex) {
            throw HyracksDataException.create(ex);
        }
    }

    @Override
    public String getContainer() {
        return containerName;
    }
}
