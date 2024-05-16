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
package org.apache.asterix.cloud.clients;

/**
 * Interface containing methods to perform IO operation on the Cloud Storage
 */
public interface ICloudGuardian {
    /**
     * Ensure we have authorization to perform isolated writes on the bucket. Isolated means that the writes do not
     * interfere with any other node, at least at present. In the event that the isolated writes can become interfering
     * in the future (i.e. committed), the {@link #checkWriteAccess(String, String)} method should be invoked just
     * prior.
     */
    void checkIsolatedWriteAccess(String bucket, String path);

    /**
     * Ensure we have authorization to perform writes on the bucket. These writes are not isolated in that they are
     * visibly side-effecting immediately.
     */
    void checkWriteAccess(String bucket, String path);

    /**
     * Ensure we have authorization to perform reads on the bucket.
     */
    void checkReadAccess(String bucket, String path);

    void setCloudClient(ICloudClient cloudClient);

    class NoOpCloudGuardian implements ICloudGuardian {

        public static final NoOpCloudGuardian INSTANCE = new NoOpCloudGuardian();

        private NoOpCloudGuardian() {
        }

        @Override
        public void checkIsolatedWriteAccess(String bucket, String path) {
            // no-op
        }

        @Override
        public void checkWriteAccess(String bucket, String path) {
            // no-op
        }

        @Override
        public void checkReadAccess(String bucket, String path) {
            // no-op
        }

        @Override
        public void setCloudClient(ICloudClient cloudClient) {
        }
    }
}
