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
package org.apache.asterix.cloud;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.asterix.cloud.clients.ICloudWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class CloudOutputStream extends OutputStream {
    private static final Logger LOGGER = LogManager.getLogger();

    private final ICloudWriter cloudWriter;
    private boolean aborted = false;

    public CloudOutputStream(ICloudWriter cloudWriter) {
        this.cloudWriter = cloudWriter;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        cloudWriter.write(b, off, len);
    }

    @Override
    public void write(int b) throws IOException {
        cloudWriter.write(b);
    }

    @Override
    public void close() throws IOException {
        if (aborted) {
            LOGGER.debug("Skipping call to finish() as operation was aborted");
            return;
        }
        cloudWriter.finish();
    }

    public void abort() throws IOException {
        if (aborted) {
            LOGGER.debug("Skipping call to abort() as we have aborted already");
            return;
        }
        aborted = true;
        cloudWriter.abort();
    }
}
