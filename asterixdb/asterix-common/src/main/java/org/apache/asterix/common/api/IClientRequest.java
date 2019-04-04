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
package org.apache.asterix.common.api;

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface IClientRequest {

    enum State {

        RECEIVED("received"),
        RUNNING("running"),
        CANCELLED("cancelled"),
        COMPLETED("completed");

        private final String label;

        State(String label) {
            this.label = label;
        }

        public String getLabel() {
            return label;
        }
    }

    /**
     * A flag indicating if the request has been cancelled
     *
     * @return true if the request was cancelled, otherwise false
     */
    boolean isCancelled();

    /**
     * A system wide unique id representing this {@link IClientRequest}
     *
     * @return the system request id
     */
    String getId();

    /**
     * A user supplied id representing this {@link IClientRequest}
     *
     * @return the client supplied request id
     */
    String getClientContextId();

    /**
     * Mark the request as complete, non-cancellable anymore
     */
    void complete();

    /**
     * Mark the request as cancellable
     */
    void markCancellable();

    /**
     * @return true if the request can be cancelled. Otherwise false.
     */
    boolean isCancellable();

    /**
     * Cancel a request
     *
     * @param appCtx
     * @throws HyracksDataException
     */
    void cancel(ICcApplicationContext appCtx) throws HyracksDataException;

    /**
     * @return A json representation of this request
     */
    String toJson();
}
