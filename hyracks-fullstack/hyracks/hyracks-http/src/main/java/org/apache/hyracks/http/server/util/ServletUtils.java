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
package org.apache.hyracks.http.server.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.GetRequest;
import org.apache.hyracks.http.server.PostRequest;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.codec.http.multipart.Attribute;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder;
import io.netty.handler.codec.http.multipart.InterfaceHttpData;
import io.netty.handler.codec.http.multipart.InterfaceHttpData.HttpDataType;
import io.netty.handler.codec.http.multipart.MixedAttribute;

public class ServletUtils {
    private static final Logger LOGGER = Logger.getLogger(ServletUtils.class.getName());

    private ServletUtils() {
    }

    public static String getParameter(Map<String, List<String>> parameters, CharSequence name) {
        List<String> parameter = parameters.get(name);
        if (parameter == null) {
            return null;
        } else if (parameter.size() == 1) {
            return parameter.get(0);
        } else {
            StringBuilder aString = new StringBuilder(parameter.get(0));
            for (int i = 1; i < parameter.size(); i++) {
                aString.append(",").append(parameter.get(i));
            }
            return aString.toString();
        }
    }

    public static IServletRequest post(FullHttpRequest request) throws IOException {
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
                    if (data.getHttpDataType().equals(HttpDataType.Attribute)) {
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

    public static IServletRequest get(FullHttpRequest request) throws IOException {
        return new GetRequest(request, new QueryStringDecoder(request.uri()).parameters());
    }

    public static IServletRequest toServletRequest(FullHttpRequest request) throws IOException {
        return request.method() == HttpMethod.GET ? ServletUtils.get(request) : ServletUtils.post(request);
    }

    public static void setContentType(IServletResponse response, String type, String charset) throws IOException {
        response.setHeader(HttpHeaderNames.CONTENT_TYPE, type + "; charset=" + charset);
    }

    public static void setContentType(IServletResponse response, String type) throws IOException {
        response.setHeader(HttpHeaderNames.CONTENT_TYPE, type);
    }
}
