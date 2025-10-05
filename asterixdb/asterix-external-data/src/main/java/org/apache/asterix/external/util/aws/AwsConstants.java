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
package org.apache.asterix.external.util.aws;

public class AwsConstants {
    private AwsConstants() {
        throw new AssertionError("do not instantiate");
    }

    // Authentication specific parameters
    public static final String REGION_FIELD_NAME = "region";
    public static final String CROSS_REGION_FIELD_NAME = "crossRegion";
    public static final String INSTANCE_PROFILE_FIELD_NAME = "instanceProfile";
    public static final String ACCESS_KEY_ID_FIELD_NAME = "accessKeyId";
    public static final String SECRET_ACCESS_KEY_FIELD_NAME = "secretAccessKey";
    public static final String SESSION_TOKEN_FIELD_NAME = "sessionToken";
    public static final String ROLE_ARN_FIELD_NAME = "roleArn";
    public static final String EXTERNAL_ID_FIELD_NAME = "externalId";
    public static final String SERVICE_END_POINT_FIELD_NAME = "serviceEndpoint";

    // AWS specific error codes
    public static final String ERROR_INTERNAL_ERROR = "InternalError";
    public static final String ERROR_SLOW_DOWN = "SlowDown";
    public static final String ERROR_METHOD_NOT_IMPLEMENTED = "NotImplemented";
    public static final String ERROR_EXPIRED_TOKEN = "ExpiredToken";
}
