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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class ArtifactSpec {
    public static final String BAD_CHARS = "[ \"#$%&'()*+,/:;<=>\\[\\]^`\\{\\|\\}~]";
    protected String url;
    protected String contentFile;
    protected List<String> aliasUrls = new ArrayList<>();
    protected String content;

    public String getUrl() {
        return url;
    }

    public String getContentFile() {
        return getContentFile(true);
    }

    @SuppressWarnings("squid:S1166")
    public String getContentFile(boolean fixupExtension) {
        if (contentFile == null) {
            String file;
            try {
                URI uri = new URI(url);
                file = ((uri.getHost() != null ? uri.getHost() : "") + uri.getPath()).replaceAll(BAD_CHARS, "_");
            } catch (URISyntaxException e) {
                file = url.replaceAll(BAD_CHARS, "_");
            }
            return (!fixupExtension || file.endsWith(".txt")) ? file : file + ".txt";
        } else {
            return contentFile;
        }
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public List<String> getAliasUrls() {
        return aliasUrls;
    }
}
