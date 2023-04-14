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
package org.apache.asterix.cloud.clients.aws.s3.credentials;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class FileCredentials implements IS3Credentials {

    private final String accessKeyId;
    private final String secretAccessKey;
    private final String region;
    private String endpoint;

    // TODO(htowaileb): change the credential file to be json object instead of reading per line
    public FileCredentials(File file) throws HyracksDataException {
        if (!file.exists()) {
            throw new IllegalStateException("No cloud configuration file found");
        }

        try {
            List<String> lines = FileUtils.readLines(file, "UTF-8");
            this.accessKeyId = lines.get(1);
            this.secretAccessKey = lines.get(2);
            this.region = lines.get(3);

            if (lines.size() > 4) {
                this.endpoint = lines.get(4);
            }
        } catch (IOException ex) {
            throw HyracksDataException.create(ex);
        }
    }

    @Override
    public String getAccessKeyId() {
        return accessKeyId;
    }

    @Override
    public String getSecretAccessKey() {
        return secretAccessKey;
    }

    @Override
    public String getRegion() {
        return region;
    }

    @Override
    public String getEndpoint() {
        return endpoint;
    }
}
