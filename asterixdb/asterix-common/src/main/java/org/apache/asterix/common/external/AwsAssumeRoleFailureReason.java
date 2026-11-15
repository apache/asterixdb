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
package org.apache.asterix.common.external;

/**
 * Why an AWS STS AssumeRole call failed. A closed set, so that a failure can be counted without recording the
 * role ARN or the error message, both of which carry customer data.
 */
public enum AwsAssumeRoleFailureReason {
    /** The role's trust policy or the caller's permissions refused the request */
    ACCESS_DENIED("access_denied"),
    /** The credentials used to call STS are unknown or their signature is invalid */
    INVALID_CREDENTIALS("invalid_credentials"),
    /** The credentials used to call STS, or the request itself, have expired */
    EXPIRED("expired"),
    /** STS throttled the request */
    THROTTLED("throttled"),
    /** STS rejected a parameter, e.g. a malformed role ARN */
    INVALID_REQUEST("invalid_request"),
    /** STS is not activated in the requested region */
    REGION_DISABLED("region_disabled"),
    /** STS reported an internal failure or is unavailable */
    UNAVAILABLE("unavailable"),
    /** The request never got an answer from STS, e.g. a network failure or no credentials to call it with */
    CLIENT_ERROR("client_error"),
    OTHER("other");

    private final String label;

    AwsAssumeRoleFailureReason(String label) {
        this.label = label;
    }

    public String label() {
        return label;
    }
}
