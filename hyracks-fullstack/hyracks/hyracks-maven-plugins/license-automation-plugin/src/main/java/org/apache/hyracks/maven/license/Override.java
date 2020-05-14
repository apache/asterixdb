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

import java.util.ArrayList;
import java.util.List;

public class Override {

    @SuppressWarnings("unused") // set by Maven plugin configuration
    private String url;

    @SuppressWarnings("unused") // set by Maven plugin configuration
    private String gav;

    @SuppressWarnings("unused") // set by Maven plugin configuration
    private List<String> gavs = new ArrayList<>();

    @SuppressWarnings("unused") // set by Maven plugin configuration
    private String name;

    @SuppressWarnings("unused") // set by Maven plugin configuration
    private String noticeUrl;

    public String getGav() {
        return gav;
    }

    public List<String> getGavs() {
        return gavs;
    }

    public String getUrl() {
        return url;
    }

    public String getName() {
        return name;
    }

    public String getNoticeUrl() {
        return noticeUrl;
    }
}
