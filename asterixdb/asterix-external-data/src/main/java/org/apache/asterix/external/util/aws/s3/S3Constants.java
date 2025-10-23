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

    // Key max length
    public static final int MAX_KEY_LENGTH_IN_BYTES = 1024;

    public static final String PATH_STYLE_ADDRESSING_FIELD_NAME = "pathStyleAddressing";

    /*
     * Hadoop-AWS
     */
    public static final String HADOOP_ASSUME_ROLE_ARN = "fs.s3a.assumed.role.arn";
    public static final String HADOOP_ASSUME_ROLE_EXTERNAL_ID = "fs.s3a.assumed.role.external.id";
    public static final String HADOOP_ASSUME_ROLE_SESSION_NAME = "fs.s3a.assumed.role.session.name";
    public static final String HADOOP_ASSUME_ROLE_SESSION_DURATION = "fs.s3a.assumed.role.session.duration";
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

    // hadoop credentials provider key
    public static final String HADOOP_CREDENTIAL_PROVIDER_KEY = "fs.s3a.aws.credentials.provider";
    public static final String HADOOP_CREDENTIALS_TO_ASSUME_ROLE_KEY = "fs.s3a.assumed.role.credentials.provider";

    // credential providers
    public static final String HADOOP_ANONYMOUS = "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider";
    public static final String HADOOP_ASSUMED_ROLE = "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider";
    public static final String HADOOP_INSTANCE_PROFILE = "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider";
    public static final String HADOOP_SIMPLE = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider";
    public static final String HADOOP_TEMPORARY = "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider";
}
