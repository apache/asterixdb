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
package org.apache.hyracks.http.test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Callable;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.StandardHttpRequestRetryHandler;

import io.netty.handler.codec.http.HttpResponseStatus;

public class HttpRequestTask implements Callable<Void> {

    protected final HttpUriRequest request;

    protected HttpRequestTask() throws URISyntaxException {
        request = post(null);
    }

    @Override
    public Void call() throws Exception {
        try {
            HttpResponse response = executeHttpRequest(request);
            if (response.getStatusLine().getStatusCode() == HttpResponseStatus.OK.code()) {
                HttpServerTest.SUCCESS_COUNT.incrementAndGet();
            } else if (response.getStatusLine().getStatusCode() == HttpResponseStatus.SERVICE_UNAVAILABLE.code()) {
                HttpServerTest.UNAVAILABLE_COUNT.incrementAndGet();
            } else {
                HttpServerTest.OTHER_COUNT.incrementAndGet();
            }
            InputStream in = response.getEntity().getContent();
            if (HttpServerTest.PRINT_TO_CONSOLE) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                String line = null;
                while ((line = reader.readLine()) != null) {
                    System.out.println(line);
                }
            }
            IOUtils.closeQuietly(in);
        } catch (Throwable th) {
            th.printStackTrace();
            throw th;
        }
        return null;
    }

    protected HttpResponse executeHttpRequest(HttpUriRequest method) throws Exception {
        HttpClient client = HttpClients.custom().setRetryHandler(StandardHttpRequestRetryHandler.INSTANCE).build();
        try {
            return client.execute(method);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    protected HttpUriRequest get(String query) throws URISyntaxException {
        URI uri = new URI(HttpServerTest.PROTOCOL, null, HttpServerTest.HOST, HttpServerTest.PORT, HttpServerTest.PATH,
                query, null);
        RequestBuilder builder = RequestBuilder.get(uri);
        builder.setCharset(StandardCharsets.UTF_8);
        return builder.build();
    }

    protected HttpUriRequest post(String query) throws URISyntaxException {
        URI uri = new URI(HttpServerTest.PROTOCOL, null, HttpServerTest.HOST, HttpServerTest.PORT, HttpServerTest.PATH,
                query, null);
        RequestBuilder builder = RequestBuilder.post(uri);
        StringBuilder str = new StringBuilder();
        for (int i = 0; i < 32; i++) {
            str.append("This is a string statement that will be ignored");
            str.append('\n');
        }
        String statement = str.toString();
        builder.setHeader("Content-type", "application/x-www-form-urlencoded");
        builder.addParameter("statement", statement);
        builder.setEntity(new StringEntity(statement, StandardCharsets.UTF_8));
        builder.setCharset(StandardCharsets.UTF_8);
        return builder.build();
    }
}
