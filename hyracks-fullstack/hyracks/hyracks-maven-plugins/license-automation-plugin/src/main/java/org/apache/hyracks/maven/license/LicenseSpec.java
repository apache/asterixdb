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
package org.apache.hyracks.maven.license;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class LicenseSpec extends ArtifactSpec {

    public static final int DEFAULT_METRIC = 100;
    public static final int UNDEFINED_LICENSE_METRIC = 999;

    private String displayName;
    private int metric = DEFAULT_METRIC;

    @SuppressWarnings("unused")
    public LicenseSpec() {
        // called by Maven configuration
    }

    @JsonCreator
    public LicenseSpec(@JsonProperty("aliasUrls") List<String> aliasUrls, @JsonProperty("content") String content,
            @JsonProperty("contentFile") String contentFile, @JsonProperty("displayName") String displayName,
            @JsonProperty("metric") int metric, @JsonProperty("url") String url) {
        this.aliasUrls = aliasUrls;
        this.content = content;
        this.contentFile = contentFile;
        this.displayName = displayName;
        this.metric = metric;
        this.url = url;
    }

    public LicenseSpec(String url, String displayName) {
        this.url = url;
        if (displayName != null) {
            this.displayName = displayName;
        }
    }

    public String getDisplayName() {
        return displayName;
    }

    public int getMetric() {
        return metric;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String toString() {
        return getDisplayName() != null ? getDisplayName() : getUrl();
    }
}
