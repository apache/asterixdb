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
package org.apache.asterix.external.util.aws.s3;

public class S3Constants {
    private S3Constants() {
        throw new AssertionError("do not instantiate");
    }

    public static final String REGION_FIELD_NAME = "region";
    public static final String INSTANCE_PROFILE_FIELD_NAME = "instanceProfile";
    public static final String ACCESS_KEY_ID_FIELD_NAME = "accessKeyId";
    public static final String SECRET_ACCESS_KEY_FIELD_NAME = "secretAccessKey";
    public static final String SESSION_TOKEN_FIELD_NAME = "sessionToken";
    public static final String SERVICE_END_POINT_FIELD_NAME = "serviceEndpoint";

    // AWS S3 specific error codes
    public static final String ERROR_INTERNAL_ERROR = "InternalError";
    public static final String ERROR_SLOW_DOWN = "SlowDown";
    public static final String ERROR_METHOD_NOT_IMPLEMENTED = "NotImplemented";

    /*
     * Hadoop-AWS
     * AWS connectors for s3 and s3n are deprecated.
     */
    public static final String HADOOP_ACCESS_KEY_ID = "fs.s3a.access.key";
    public static final String HADOOP_SECRET_ACCESS_KEY = "fs.s3a.secret.key";
    public static final String HADOOP_SESSION_TOKEN = "fs.s3a.session.token";
    public static final String HADOOP_REGION = "fs.s3a.region";
    public static final String HADOOP_SERVICE_END_POINT = "fs.s3a.endpoint";
    public static final String HADOOP_S3_FILESYSTEM_IMPLEMENTATION = "fs.s3a.impl";

    /*
     * Internal configurations
     */
    //Allows accessing directories as file system path
    public static final String HADOOP_PATH_STYLE_ACCESS = "fs.s3a.path.style.access";
    //The number of maximum HTTP connections in connection pool
    public static final String HADOOP_S3_CONNECTION_POOL_SIZE = "fs.s3a.connection.maximum";
    //S3 used protocol
    public static final String HADOOP_S3_PROTOCOL = "s3a";

    //Hadoop credentials provider key
    public static final String HADOOP_CREDENTIAL_PROVIDER_KEY = "fs.s3a.aws.credentials.provider";
    //Anonymous credential provider
    public static final String HADOOP_ANONYMOUS_ACCESS = "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider";
    //Temporary credential provider
    public static final String HADOOP_TEMP_ACCESS = "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider";
}
