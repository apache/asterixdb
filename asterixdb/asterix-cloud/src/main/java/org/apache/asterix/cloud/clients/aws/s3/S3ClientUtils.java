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
package org.apache.asterix.cloud.clients.aws.s3;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

public class S3ClientUtils {

    private S3ClientUtils() {
        throw new AssertionError("do not instantiate");
    }

    public static List<S3Object> listS3Objects(S3Client s3Client, String bucket, String path) {
        String newMarker = null;
        ListObjectsV2Response listObjectsResponse;
        ListObjectsV2Request.Builder listObjectsBuilder = ListObjectsV2Request.builder().bucket(bucket);
        listObjectsBuilder.prefix(toCloudPrefix(path));
        List<S3Object> files = new ArrayList<>();
        while (true) {
            // List the objects from the start, or from the last marker in case of truncated result
            if (newMarker == null) {
                listObjectsResponse = s3Client.listObjectsV2(listObjectsBuilder.build());
            } else {
                listObjectsResponse = s3Client.listObjectsV2(listObjectsBuilder.continuationToken(newMarker).build());
            }

            files.addAll(listObjectsResponse.contents());

            // Mark the flag as done if done, otherwise, get the marker of the previous response for the next request
            if (Boolean.FALSE.equals(listObjectsResponse.isTruncated())) {
                break;
            } else {
                newMarker = listObjectsResponse.nextContinuationToken();
            }
        }
        return files;
    }

    public static boolean isEmptyPrefix(S3Client s3Client, String bucket, String path) {
        ListObjectsV2Request.Builder listObjectsBuilder = ListObjectsV2Request.builder().bucket(bucket);
        listObjectsBuilder.prefix(toCloudPrefix(path));
        List<S3Object> files = s3Client.listObjectsV2(listObjectsBuilder.build()).contents();

        return isEmptyFolder(files, path);
    }

    public static String encodeURI(String path) {
        if (path.isEmpty()) {
            return path;
        }
        try {
            return new URI("s3", "//", path).getRawFragment();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static String decodeURI(String path) {
        return URLDecoder.decode(path, Charset.defaultCharset());
    }

    private static String toCloudPrefix(String path) {
        return path.startsWith(File.separator) ? path.substring(1) : path;
    }

    private static boolean isEmptyFolder(List<S3Object> files, String path) {
        if (files.size() > 1) {
            return false;
        } else if (files.isEmpty()) {
            return true;
        }

        S3Object s3Object = files.get(0);
        String key = s3Object.key();
        return s3Object.size() == 0 && key.charAt(key.length() - 1) == '/' && key.startsWith(path);
    }
}
