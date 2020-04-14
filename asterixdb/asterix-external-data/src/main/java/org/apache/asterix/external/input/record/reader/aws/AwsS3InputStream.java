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
package org.apache.asterix.external.input.record.reader.aws;

import static org.apache.asterix.external.util.ExternalDataConstants.AwsS3Constants;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.Map;

import org.apache.asterix.external.api.AsterixInputStream;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.CleanupUtils;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

public class AwsS3InputStream extends AsterixInputStream {

    // Configuration
    private final Map<String, String> configuration;

    private final S3Client s3Client;

    // File fields
    private final List<String> filePaths;
    private int nextFileIndex = 0;

    // File reading fields
    private InputStream inputStream;

    public AwsS3InputStream(Map<String, String> configuration, List<String> filePaths) {
        this.configuration = configuration;
        this.filePaths = filePaths;

        this.s3Client = buildAwsS3Client(configuration);
    }

    @Override
    public int read() throws IOException {
        throw new HyracksDataException(
                "read() is not supported with this stream. use read(byte[] b, int off, int len)");
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (inputStream == null) {
            if (!advance()) {
                return -1;
            }
        }

        int result = inputStream.read(b, off, len);

        // If file reading is done, go to the next file, or finish up if no files are left
        if (result < 0) {
            if (advance()) {
                result = inputStream.read(b, off, len);
            } else {
                return -1;
            }
        }

        return result;
    }

    private boolean advance() throws IOException {
        // No files to read for this partition
        if (filePaths == null || filePaths.isEmpty()) {
            return false;
        }

        // Finished reading all the files
        if (nextFileIndex == filePaths.size()) {
            if (inputStream != null) {
                inputStream.close();
            }
            return false;
        }

        // Close the current stream before going to the next one
        if (inputStream != null) {
            inputStream.close();
        }

        String bucket = configuration.get(AwsS3Constants.CONTAINER_NAME_FIELD_NAME);
        GetObjectRequest.Builder getObjectBuilder = GetObjectRequest.builder();
        GetObjectRequest getObjectRequest = getObjectBuilder.bucket(bucket).key(filePaths.get(nextFileIndex)).build();
        inputStream = s3Client.getObject(getObjectRequest);

        // Current file ready, point to the next file
        nextFileIndex++;
        return true;
    }

    @Override
    public boolean stop() {
        return false;
    }

    @Override
    public boolean handleException(Throwable th) {
        return false;
    }

    @Override
    public void close() throws IOException {
        if (inputStream != null) {
            CleanupUtils.close(inputStream, null);
        }
    }

    /**
     * Prepares and builds the Amazon S3 client with the provided configuration
     *
     * @param configuration S3 client configuration
     *
     * @return Amazon S3 client
     */
    private static S3Client buildAwsS3Client(Map<String, String> configuration) {
        S3ClientBuilder builder = S3Client.builder();

        // Credentials
        String accessKey = configuration.get(AwsS3Constants.ACCESS_KEY_FIELD_NAME);
        String secretKey = configuration.get(AwsS3Constants.SECRET_KEY_FIELD_NAME);
        AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKey, secretKey);
        builder.credentialsProvider(StaticCredentialsProvider.create(credentials));

        // Region
        String region = configuration.get(AwsS3Constants.REGION_FIELD_NAME);
        builder.region(Region.of(region));

        // Use user's endpoint if provided
        if (configuration.get(AwsS3Constants.SERVICE_END_POINT_FIELD_NAME) != null) {
            String endPoint = configuration.get(AwsS3Constants.SERVICE_END_POINT_FIELD_NAME);
            builder.endpointOverride(URI.create(endPoint));
        }

        return builder.build();
    }
}
