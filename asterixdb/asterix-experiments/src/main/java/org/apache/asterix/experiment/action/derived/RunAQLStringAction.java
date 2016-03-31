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

package org.apache.asterix.experiment.action.derived;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;

import org.apache.asterix.experiment.action.base.AbstractAction;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;

public class RunAQLStringAction extends AbstractAction {
    private static final String REST_URI_TEMPLATE = "http://{0}:{1}/aql";

    private final HttpClient httpClient;

    private final String aql;

    private final String restHost;

    private final int restPort;

    private final OutputStream os;

    public RunAQLStringAction(HttpClient httpClient, String restHost, int restPort, String aql) {
        this.httpClient = httpClient;
        this.aql = aql;
        this.restHost = restHost;
        this.restPort = restPort;
        os = null;
    }

    public RunAQLStringAction(HttpClient httpClient, String restHost, int restPort, String aql, OutputStream os) {
        this.httpClient = httpClient;
        this.aql = aql;
        this.restHost = restHost;
        this.restPort = restPort;
        this.os = os;
    }

    @Override
    public void doPerform() throws Exception {
        String uri = MessageFormat.format(REST_URI_TEMPLATE, restHost, String.valueOf(restPort));
        HttpPost post = new HttpPost(uri);
        post.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
        post.setEntity(new StringEntity(aql, StandardCharsets.UTF_8));
        HttpEntity entity = httpClient.execute(post).getEntity();
        if (entity != null && entity.isStreaming()) {
            printStream(entity.getContent());
        }
    }

    private void printStream(InputStream content) throws IOException {
        if (os == null) {
            IOUtils.copy(content, System.out);
            System.out.flush();
        } else {
            IOUtils.copy(content, os);
            os.flush();
        }
    }
}
