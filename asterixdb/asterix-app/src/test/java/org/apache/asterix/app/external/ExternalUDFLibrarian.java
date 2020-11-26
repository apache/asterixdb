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
package org.apache.asterix.app.external;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.hyracks.algebricks.common.utils.Pair;

@SuppressWarnings("squid:S134")
public class ExternalUDFLibrarian implements IExternalUDFLibrarian {

    private HttpClient hc;

    public ExternalUDFLibrarian() {
        hc = new DefaultHttpClient();
    }

    @Override
    public void install(URI path, String libPath, Pair<String, String> credentials) throws Exception {
        HttpHost h = new HttpHost(path.getHost(), path.getPort(), "http");
        HttpPost post = new HttpPost(path);
        CredentialsProvider cp = new BasicCredentialsProvider();
        cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(credentials.first, credentials.second));
        HttpClientContext hcCtx = HttpClientContext.create();
        hcCtx.setCredentialsProvider(cp);
        AuthCache ac = new BasicAuthCache();
        ac.put(h, new BasicScheme());
        hcCtx.setAuthCache(ac);
        File lib = new File(libPath);
        HttpEntity file = MultipartEntityBuilder.create().setMode(HttpMultipartMode.STRICT)
                .addBinaryBody("lib", lib, ContentType.DEFAULT_BINARY, lib.getName()).build();
        post.setEntity(file);
        HttpResponse response = hc.execute(post, hcCtx);
        response.getEntity().consumeContent();
        if (response.getStatusLine().getStatusCode() != 200) {
            throw new AsterixException(response.getStatusLine().toString());
        }
    }

    @Override
    public void uninstall(URI path, Pair<String, String> credentials) throws IOException, AsterixException {
        CredentialsProvider cp = new BasicCredentialsProvider();
        cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(credentials.first, credentials.second));
        HttpClientContext hcCtx = HttpClientContext.create();
        hcCtx.setCredentialsProvider(cp);
        HttpHost h = new HttpHost(path.getHost(), path.getPort(), "http");
        AuthCache ac = new BasicAuthCache();
        ac.put(h, new BasicScheme());
        hcCtx.setAuthCache(ac);
        HttpDelete del = new HttpDelete(path);
        HttpResponse response = hc.execute(del, hcCtx);
        String resp = null;
        int respCode = response.getStatusLine().getStatusCode();
        if (respCode == 500) {
            resp = IOUtils.toString(response.getEntity().getContent());
        }
        response.getEntity().consumeContent();
        if (resp == null && respCode != 200) {
            resp = response.getStatusLine().toString();
        }
        if (resp != null) {
            throw new AsterixException(resp);
        }
    }

}
