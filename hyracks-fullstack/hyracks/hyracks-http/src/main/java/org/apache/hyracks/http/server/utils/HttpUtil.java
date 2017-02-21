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
package org.apache.hyracks.http.server.utils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.BaseRequest;
import org.apache.hyracks.http.server.PostRequest;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;

public class HttpUtil {

    private HttpUtil() {
    }

    public static class Encoding {
        public static final String UTF8 = "utf-8";

        private Encoding() {
        }
    }

    public static class ContentType {
        public static final String APPLICATION_ADM = "application/x-adm";
        public static final String APPLICATION_JSON = "application/json";
        public static final String CSV = "text/csv";
        public static final String IMG_PNG = "image/png";
        public static final String TEXT_HTML = "text/html";
        public static final String TEXT_PLAIN = "text/plain";

        private ContentType() {
        }
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

    public static IServletRequest toServletRequest(FullHttpRequest request) throws IOException {
        return request.method() == HttpMethod.POST ? PostRequest.create(request) : BaseRequest.create(request);
    }

    public static void setContentType(IServletResponse response, String type, String charset) throws IOException {
        response.setHeader(HttpHeaderNames.CONTENT_TYPE, type + "; charset=" + charset);
    }

    public static void setContentType(IServletResponse response, String type) throws IOException {
        response.setHeader(HttpHeaderNames.CONTENT_TYPE, type);
    }

    /**
     * Get the mime string representation from the extension
     *
     * @param extension
     * @return
     */
    public static String mime(String extension) {
        switch (extension) {
            case ".png":
                return "image/png";
            case ".eot":
                return "application/vnd.ms-fontobject";
            case ".svg":
                return "image/svg+xml\t";
            case ".ttf":
                return "application/x-font-ttf";
            case ".woff":
            case ".woff2":
                return "application/x-font-woff";
            case ".html":
                return "text/html";
            case ".css":
                return "text/css";
            case ".js":
                return "application/javascript";
            default:
                return null;
        }
    }
}
