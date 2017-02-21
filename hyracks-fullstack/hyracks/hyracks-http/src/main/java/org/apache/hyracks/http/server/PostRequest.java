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
package org.apache.hyracks.http.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.server.utils.HttpUtil;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.codec.http.multipart.Attribute;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder;
import io.netty.handler.codec.http.multipart.InterfaceHttpData;
import io.netty.handler.codec.http.multipart.MixedAttribute;

public class PostRequest extends BaseRequest implements IServletRequest {

    private static final Logger LOGGER = Logger.getLogger(PostRequest.class.getName());

    private final List<String> names;
    private final List<String> values;

    public static IServletRequest create(FullHttpRequest request) throws IOException {
        List<String> names = new ArrayList<>();
        List<String> values = new ArrayList<>();
        HttpPostRequestDecoder decoder = null;
        try {
            decoder = new HttpPostRequestDecoder(request);
        } catch (Exception e) {
            //ignore. this means that the body of the POST request does not have key value pairs
            LOGGER.log(Level.WARNING, "Failed to decode a post message. Fix the API not to have queries as POST body",
                    e);
        }
        if (decoder != null) {
            try {
                List<InterfaceHttpData> bodyHttpDatas = decoder.getBodyHttpDatas();
                for (InterfaceHttpData data : bodyHttpDatas) {
                    if (data.getHttpDataType().equals(InterfaceHttpData.HttpDataType.Attribute)) {
                        Attribute attr = (MixedAttribute) data;
                        names.add(data.getName());
                        values.add(attr.getValue());
                    }
                }
            } finally {
                decoder.destroy();
            }
        }
        return new PostRequest(request, new QueryStringDecoder(request.uri()).parameters(), names, values);
    }

    protected PostRequest(FullHttpRequest request, Map<String, List<String>> parameters, List<String> names,
            List<String> values) {
        super(request, parameters);
        this.names = names;
        this.values = values;
    }

    @Override
    public String getParameter(CharSequence name) {
        for (int i = 0; i < names.size(); i++) {
            if (name.equals(names.get(i))) {
                return values.get(i);
            }
        }
        return HttpUtil.getParameter(parameters, name);
    }
}
