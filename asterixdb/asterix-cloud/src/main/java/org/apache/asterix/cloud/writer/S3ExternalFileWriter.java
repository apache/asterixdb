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
package org.apache.asterix.cloud.writer;

import org.apache.asterix.cloud.clients.ICloudClient;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.runtime.writer.IExternalFilePrinter;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.SourceLocation;

import software.amazon.awssdk.core.exception.SdkException;

final class S3ExternalFileWriter extends AbstractCloudExternalFileWriter {
    static int MAX_LENGTH_IN_BYTES = 1024;

    S3ExternalFileWriter(IExternalFilePrinter printer, ICloudClient cloudClient, String bucket, boolean partitionedPath,
            IWarningCollector warningCollector, SourceLocation pathSourceLocation) {
        super(printer, cloudClient, bucket, partitionedPath, warningCollector, pathSourceLocation);
    }

    @Override
    String getAdapterName() {
        return ExternalDataConstants.KEY_ADAPTER_NAME_AWS_S3;
    }

    @Override
    int getPathMaxLengthInBytes() {
        return MAX_LENGTH_IN_BYTES;
    }

    @Override
    boolean isSdkException(Exception e) {
        return e instanceof SdkException;
    }
}
