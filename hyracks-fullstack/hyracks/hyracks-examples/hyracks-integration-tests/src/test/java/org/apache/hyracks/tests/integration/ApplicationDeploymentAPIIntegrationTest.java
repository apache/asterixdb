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
package org.apache.hyracks.tests.integration;

import static org.apache.hyracks.tests.integration.TestUtil.uri;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.HttpClients;
import org.junit.Assert;
import org.junit.Test;

public class ApplicationDeploymentAPIIntegrationTest extends AbstractIntegrationTest {

    @Test
    public void testApplicationDeploymentSm() throws Exception {
        final int dataSize = 100;
        deployApplicationFile(dataSize, "small");
    }

    @Test
    public void testApplicationDeploymentMd() throws Exception {
        final int dataSize = 8 * 1024 * 1024;
        deployApplicationFile(dataSize, "medium");
    }

    @Test
    public void testApplicationDeploymentLg() throws Exception {
        final int dataSize = 50 * 1024 * 1024;
        deployApplicationFile(dataSize, "large");
    }

    protected void deployApplicationFile(int dataSize, String fileName) throws URISyntaxException, IOException {
        final String deployid = "testApp";

        String path = "/applications/" + deployid + "&" + fileName;
        URI uri = uri(path);

        byte[] data = new byte[dataSize];
        for (int i = 0; i < data.length; ++i) {
            data[i] = (byte) i;
        }

        HttpClient client = HttpClients.createMinimal();

        // Put the data

        HttpPut put = new HttpPut(uri);
        HttpEntity entity = new ByteArrayEntity(data, ContentType.APPLICATION_OCTET_STREAM);
        put.setEntity(entity);
        client.execute(put);

        // Get it back

        HttpGet get = new HttpGet(uri);
        HttpResponse response = client.execute(get);
        HttpEntity respEntity = response.getEntity();
        Header contentType = respEntity.getContentType();

        // compare results

        Assert.assertEquals(ContentType.APPLICATION_OCTET_STREAM.getMimeType(), contentType.getValue());
        InputStream is = respEntity.getContent();

        for (int i = 0; i < dataSize; ++i) {
            Assert.assertEquals(data[i], (byte) is.read());
        }
        Assert.assertEquals(-1, is.read());
        is.close();
    }
}
