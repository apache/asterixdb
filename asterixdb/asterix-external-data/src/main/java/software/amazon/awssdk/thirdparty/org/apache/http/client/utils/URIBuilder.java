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
package software.amazon.awssdk.thirdparty.org.apache.http.client.utils;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * This class is introduced to avoid direct dependency on Apache HttpClient that is shaded in aws-java-sdk-bundle
 * which is huge in size and comes with few CVEs, considering this is all we need from that bundle. This can be
 * removed after upgrading hadoop to a version that includes the change that relies on Java's URI instead of the
 * shaded URI builder dependency.
 *
 * See the following for more details:
 * https://issues.apache.org/jira/browse/HADOOP-19282
 */
public class URIBuilder {
    private final org.apache.http.client.utils.URIBuilder uriBuilder = new org.apache.http.client.utils.URIBuilder();

    public URIBuilder setHost(String host) {
        uriBuilder.setHost(host);
        return this;
    }

    public URIBuilder setScheme(String scheme) {
        uriBuilder.setScheme(scheme);
        return this;
    }

    public URI build() throws URISyntaxException {
        return uriBuilder.build();
    }
}