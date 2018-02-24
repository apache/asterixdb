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
package org.apache.asterix.common.utils;

import io.netty.handler.codec.http.HttpResponseStatus;

public enum RequestStatus {
    SUCCESS,
    FAILED,
    NOT_FOUND;

    public HttpResponseStatus toHttpResponse() {
        switch (this) {
            case SUCCESS:
                return HttpResponseStatus.OK;
            case FAILED:
                return HttpResponseStatus.INTERNAL_SERVER_ERROR;
            case NOT_FOUND:
                return HttpResponseStatus.NOT_FOUND;
            default:
                throw new IllegalStateException("Unrecognized status: " + this);
        }
    }
}