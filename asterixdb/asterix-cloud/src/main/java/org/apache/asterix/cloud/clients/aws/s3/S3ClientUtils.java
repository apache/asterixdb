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

import static org.apache.hyracks.util.annotations.AiProvenance.Agent.CLAUDE_FABLE_5_1;
import static org.apache.hyracks.util.annotations.AiProvenance.ContributionKind.REFACTORED;
import static org.apache.hyracks.util.annotations.AiProvenance.Tool.CLAUDE_CODE_UI;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import org.apache.asterix.external.util.aws.s3.S3Utils;
import org.apache.hyracks.util.annotations.AiProvenance;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

public class S3ClientUtils {

    private S3ClientUtils() {
        throw new AssertionError("do not instantiate");
    }

    @AiProvenance(agent = CLAUDE_FABLE_5_1, tool = CLAUDE_CODE_UI, contributionKind = REFACTORED, notes = "Paginate with the SDK ListObjectsV2 iterable instead of a manual continuation-token loop")
    public static List<S3Object> listS3Objects(S3Client s3Client, String bucket, String path) {
        ListObjectsV2Request listObjectsRequest =
                ListObjectsV2Request.builder().bucket(bucket).prefix(toCloudPrefix(path)).build();
        List<S3Object> files = new ArrayList<>();

        // the iterable requests the next page with the previous page's continuation token until the result is complete
        ListObjectsV2Iterable listObjectsIterable = s3Client.listObjectsV2Paginator(listObjectsRequest);
        for (ListObjectsV2Response listObjectsResponse : listObjectsIterable) {
            // ignore objects ending with "/" since we don't create such objects.
            // S3 can return folders as objects with size 0 and key ending with "/"
            listObjectsResponse.contents().stream().filter(Predicate.not(S3Utils::isDirectory)).forEach(files::add);
        }
        return files;
    }

    public static boolean isEmptyPrefix(S3Client s3Client, String bucket, String path) {
        ListObjectsV2Request.Builder listObjectsBuilder = ListObjectsV2Request.builder().bucket(bucket);
        listObjectsBuilder.prefix(toCloudPrefix(path));
        List<S3Object> files = s3Client.listObjectsV2(listObjectsBuilder.build()).contents();

        return isEmptyFolder(files, path);
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
