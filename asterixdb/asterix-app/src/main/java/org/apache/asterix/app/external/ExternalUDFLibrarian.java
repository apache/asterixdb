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
import java.net.URL;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.FileEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.hyracks.api.exceptions.HyracksException;

@SuppressWarnings("squid:S134")
public class ExternalUDFLibrarian implements IExternalUDFLibrarian {

    private HttpClient hc;
    private String host;
    private int port;

    public ExternalUDFLibrarian(String host, int port) {
        hc = new DefaultHttpClient();
        this.host = host;
        this.port = port;
    }

    public ExternalUDFLibrarian() {
        this("localhost", 19002);
    }

    @Override
    public void install(String dataverse, String libName, String libPath) throws Exception {
        URL url = new URL("http", host, port, "/admin/udf/" + dataverse + "/" + libName);
        HttpPost post = new HttpPost(url.toString());
        post.setEntity(new FileEntity(new File(libPath), "application/octet-stream"));
        HttpResponse response = hc.execute(post);
        response.getEntity().consumeContent();
        if (response.getStatusLine().getStatusCode() != 200) {
            throw new HyracksException(response.getStatusLine().toString());
        }
    }

    @Override
    public void uninstall(String dataverse, String libName)
            throws IOException, ClientProtocolException, AsterixException {
        URL url = new URL("http", host, port, "/admin/udf/" + dataverse + "/" + libName);
        HttpDelete del = new HttpDelete(url.toString());
        HttpResponse response = hc.execute(del);
        response.getEntity().consumeContent();
        if (response.getStatusLine().getStatusCode() != 200) {
            throw new AsterixException(response.getStatusLine().toString());
        }
    }

}
