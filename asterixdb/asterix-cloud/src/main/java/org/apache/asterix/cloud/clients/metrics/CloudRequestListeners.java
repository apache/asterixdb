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
package org.apache.asterix.cloud.clients.metrics;

/**
 * Holds the listener the cloud clients report their requests to. Process-wide because the clients are built, and
 * rebuilt on a configuration change, from their configuration alone.
 */
public final class CloudRequestListeners {
    private static volatile ICloudRequestListener listener = ICloudRequestListener.NO_OP;

    private CloudRequestListeners() {
    }

    public static void set(ICloudRequestListener newListener) {
        listener = newListener == null ? ICloudRequestListener.NO_OP : newListener;
    }

    public static void requestCompleted(CloudRequestOperation operation, long durationNanos,
            CloudRequestFailure failure) {
        listener.requestCompleted(operation, durationNanos, failure);
    }
}
