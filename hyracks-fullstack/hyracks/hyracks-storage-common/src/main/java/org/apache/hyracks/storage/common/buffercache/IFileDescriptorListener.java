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
package org.apache.hyracks.storage.common.buffercache;

/**
 * Observes the {@link BufferCache}'s management of OS file descriptors, for instance to publish metrics. Called on the
 * I/O path and the release pass, so implementations must be cheap and must not block; anything thrown is logged and
 * otherwise ignored.
 */
public interface IFileDescriptorListener {

    IFileDescriptorListener NO_OP = new IFileDescriptorListener() {
    };

    /**
     * A file's descriptor was released to keep under the descriptor bound.
     *
     * @param idleNanos
     *            how long the file had gone without I/O when released
     */
    default void descriptorReleased(long idleNanos) {
    }

    /**
     * A released descriptor was reopened by an I/O on its file.
     *
     * @param releasedNanos
     *            how long the descriptor had been released
     */
    default void descriptorReopened(long releasedNanos) {
    }

    /**
     * A release pass finished.
     *
     * @param released
     *            the descriptors it released
     * @param durationNanos
     *            how long it took
     * @param underBound
     *            whether it left the open descriptors within the bound
     */
    default void releasePassCompleted(int released, long durationNanos, boolean underBound) {
    }
}
