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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.hyracks.util.annotations.AiProvenance;
import org.apache.hyracks.util.annotations.AiProvenance.Agent;
import org.apache.hyracks.util.annotations.AiProvenance.ContributionKind;
import org.apache.hyracks.util.annotations.AiProvenance.Tool;
import org.junit.Test;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Verifies that {@link S3ClientUtils#listS3Objects} walks every page of a truncated ListObjectsV2 result, chaining
 * each request off the previous page's continuation token. A hand-rolled {@link S3Client} serves the pages, because
 * the in-memory S3 mock used by the other tests returns every key in a single page and never truncates.
 */
@AiProvenance(agent = Agent.CLAUDE_FABLE_5_1, tool = Tool.CLAUDE_CODE_UI, contributionKind = ContributionKind.TEST_GENERATED)
public class S3ClientUtilsListObjectsTest {

    private static final String BUCKET = "bucket";
    private static final String PREFIX = "storage/partition_0";

    @Test
    public void walksAllPagesChainingContinuationTokens() {
        PagedS3Client client = new PagedS3Client(page(keys("a", 1000), "token-1"), page(keys("b", 1000), "token-2"),
                page(keys("c", 999), null));

        List<S3Object> listed = S3ClientUtils.listS3Objects(client, BUCKET, "/" + PREFIX);

        assertEquals(2999, listed.size());
        assertEquals(2999, listed.stream().map(S3Object::key).distinct().count());
        assertEquals(Arrays.asList(null, "token-1", "token-2"), client.receivedTokens);
        for (ListObjectsV2Request request : client.receivedRequests) {
            assertEquals(BUCKET, request.bucket());
            assertEquals(PREFIX, request.prefix());
        }
    }

    @Test
    public void singlePageIsRequestedOnce() {
        PagedS3Client client = new PagedS3Client(page(keys("a", 3), null));

        List<S3Object> listed = S3ClientUtils.listS3Objects(client, BUCKET, PREFIX);

        assertEquals(3, listed.size());
        assertEquals(1, client.receivedRequests.size());
        assertNull(client.receivedTokens.get(0));
    }

    @Test
    public void directoryMarkersAreDroppedOnEveryPage() {
        List<String> firstPage = new ArrayList<>(keys("a", 2));
        firstPage.add(PREFIX + "/dir-a/");
        List<String> secondPage = new ArrayList<>(keys("b", 2));
        secondPage.add(PREFIX + "/dir-b/");
        PagedS3Client client = new PagedS3Client(page(firstPage, "token-1"), page(secondPage, null));

        List<S3Object> listed = S3ClientUtils.listS3Objects(client, BUCKET, PREFIX);

        assertEquals(4, listed.size());
        assertEquals(0, listed.stream().filter(object -> object.key().endsWith("/")).count());
    }

    private static List<String> keys(String tag, int count) {
        return IntStream.range(0, count).mapToObj(i -> PREFIX + "/" + tag + "-" + i).collect(Collectors.toList());
    }

    private static ListObjectsV2Response page(List<String> keys, String nextContinuationToken) {
        List<S3Object> contents =
                keys.stream().map(key -> S3Object.builder().key(key).size(1L).build()).collect(Collectors.toList());
        return ListObjectsV2Response.builder().contents(contents).keyCount(contents.size())
                .isTruncated(nextContinuationToken != null).nextContinuationToken(nextContinuationToken).build();
    }

    /**
     * Serves a fixed sequence of pages; every operation other than ListObjectsV2 keeps the SDK's default behaviour
     * of throwing, so the test fails loudly if the code under test starts calling anything else.
     */
    private static final class PagedS3Client implements S3Client {
        private final List<ListObjectsV2Response> pages;
        private final List<ListObjectsV2Request> receivedRequests = new ArrayList<>();
        private final List<String> receivedTokens = new ArrayList<>();

        private PagedS3Client(ListObjectsV2Response... pages) {
            this.pages = Arrays.asList(pages);
        }

        @Override
        public ListObjectsV2Response listObjectsV2(ListObjectsV2Request request) {
            receivedRequests.add(request);
            receivedTokens.add(request.continuationToken());
            String token = request.continuationToken();
            if (token == null) {
                return pages.get(0);
            }
            for (int i = 0; i < pages.size() - 1; i++) {
                if (token.equals(pages.get(i).nextContinuationToken())) {
                    return pages.get(i + 1);
                }
            }
            throw new IllegalArgumentException("unknown continuation token " + token);
        }

        @Override
        public String serviceName() {
            return SERVICE_NAME;
        }

        @Override
        public void close() {
            // nothing to release
        }
    }
}
