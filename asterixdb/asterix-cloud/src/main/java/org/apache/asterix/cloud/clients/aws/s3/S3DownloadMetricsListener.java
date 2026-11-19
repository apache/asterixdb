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

import org.apache.asterix.cloud.clients.metrics.CloudRequestFailure;
import org.apache.asterix.cloud.clients.metrics.CloudRequestListeners;
import org.apache.asterix.cloud.clients.metrics.CloudRequestOperation;

import software.amazon.awssdk.transfer.s3.progress.TransferListener;

/**
 * Reports an object downloaded by the transfer manager over the CRT client to the {@link CloudRequestListeners}.
 * The CRT client takes no execution interceptor, and wrapping it would stop the transfer manager recognising it as a
 * CRT client, so its downloads are observed here instead. One report covers the whole object, including every ranged
 * part and retry the CRT client issued for it.
 * <p>
 * Not shareable: create one per download.
 */
final class S3DownloadMetricsListener implements TransferListener {
    private volatile long startNanos = System.nanoTime();

    @Override
    public void transferInitiated(Context.TransferInitiated context) {
        startNanos = System.nanoTime();
    }

    @Override
    public void transferComplete(Context.TransferComplete context) {
        report(null);
    }

    @Override
    public void transferFailed(Context.TransferFailed context) {
        report(S3RequestMetricsInterceptor.failure(context.exception()));
    }

    private void report(CloudRequestFailure failure) {
        CloudRequestListeners.requestCompleted(CloudRequestOperation.GET, System.nanoTime() - startNanos, failure);
    }
}
