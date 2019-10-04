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
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.BaseRequest;
import org.apache.hyracks.http.server.FormUrlEncodedRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpScheme;
import io.netty.util.AsciiString;

public class HttpUtil {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final Pattern PARENT_DIR = Pattern.compile("/[^./]+/\\.\\./");
    private static final Charset DEFAULT_RESPONSE_CHARSET = StandardCharsets.UTF_8;

    public static final AsciiString X_FORWARDED_PROTO = AsciiString.cached("x-forwarded-proto");

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
        public static final String JSON = "json";
        public static final String APPLICATION_X_WWW_FORM_URLENCODED = "application/x-www-form-urlencoded";
        public static final String CSV = "text/csv";
        public static final String IMG_PNG = "image/png";
        public static final String TEXT_HTML = "text/html";
        public static final String TEXT_PLAIN = "text/plain";

        private ContentType() {
        }
    }

    public static String getParameter(Map<String, List<String>> parameters, CharSequence name) {
        List<String> parameter = parameters.get(String.valueOf(name));
        return parameter == null ? null : String.join(",", parameter);
    }

    public static IServletRequest toServletRequest(ChannelHandlerContext ctx, FullHttpRequest request,
            HttpScheme scheme) {
        return ContentType.APPLICATION_X_WWW_FORM_URLENCODED.equals(getContentTypeOnly(request))
                ? FormUrlEncodedRequest.create(ctx, request, scheme) : BaseRequest.create(ctx, request, scheme);
    }

    public static String getContentTypeOnly(IServletRequest request) {
        return getContentTypeOnly(request.getHttpRequest());
    }

    public static String getContentTypeOnly(HttpRequest request) {
        String contentType = request.headers().get(HttpHeaderNames.CONTENT_TYPE);
        return contentType == null ? null : contentType.split(";")[0];
    }

    public static Charset getRequestCharset(HttpRequest request) {
        return io.netty.handler.codec.http.HttpUtil.getCharset(request, StandardCharsets.UTF_8);
    }

    public static String getRequestBody(IServletRequest request) {
        FullHttpRequest httpRequest = request.getHttpRequest();
        return httpRequest.content().toString(getRequestCharset(httpRequest));
    }

    public static Charset setContentType(IServletResponse response, String type, IServletRequest fromRequest)
            throws IOException {
        Charset preferredCharset = getPreferredCharset(fromRequest);
        response.setHeader(HttpHeaderNames.CONTENT_TYPE, type + "; charset=" + preferredCharset.name());
        return preferredCharset;
    }

    public static void setContentType(IServletResponse response, String type, String charset) throws IOException {
        response.setHeader(HttpHeaderNames.CONTENT_TYPE, type + "; charset=" + charset);
    }

    public static void setContentType(IServletResponse response, String type) throws IOException {
        response.setHeader(HttpHeaderNames.CONTENT_TYPE, type);
    }

    public static Map<String, String> getRequestHeaders(IServletRequest request) {
        Map<String, String> headers = new HashMap<>();
        request.getHttpRequest().headers().forEach(entry -> headers.put(entry.getKey(), entry.getValue()));
        return headers;
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
            case ".txt":
                return "text/plain";
            case ".ico":
                return "image/x-icon";
            case ".js":
                return "application/javascript";
            default:
                return null;
        }
    }

    public static String canonicalize(CharSequence requestURL) {
        String clusterURL = "";
        String newClusterURL = requestURL.toString();
        while (!clusterURL.equals(newClusterURL)) {
            clusterURL = newClusterURL;
            newClusterURL = PARENT_DIR.matcher(clusterURL).replaceAll("/");
        }
        return clusterURL;
    }

    public static void setConnectionHeader(HttpRequest request, DefaultHttpResponse response) {
        final boolean keepAlive = io.netty.handler.codec.http.HttpUtil.isKeepAlive(request);
        final AsciiString connectionHeaderValue = keepAlive ? HttpHeaderValues.KEEP_ALIVE : HttpHeaderValues.CLOSE;
        response.headers().set(HttpHeaderNames.CONNECTION, connectionHeaderValue);
    }

    public static Charset getPreferredCharset(IServletRequest request) {
        return getPreferredCharset(request, DEFAULT_RESPONSE_CHARSET);
    }

    public static Charset getPreferredCharset(IServletRequest request, Charset defaultCharset) {
        String acceptCharset = request.getHeader(HttpHeaderNames.ACCEPT_CHARSET);
        if (acceptCharset == null) {
            return defaultCharset;
        }
        // If no "q" parameter is present, the default weight is 1 [https://tools.ietf.org/html/rfc7231#section-5.3.1]
        Optional<Charset> preferredCharset = Stream.of(StringUtils.split(acceptCharset, ","))
                .map(WeightedHeaderValue::new).sorted().map(WeightedHeaderValue::getValueDefaultStar).filter(value -> {
                    if (!Charset.isSupported(value)) {
                        LOGGER.info("disregarding unsupported charset '{}'", value);
                        return false;
                    }
                    return true;
                }).map(Charset::forName).findFirst();
        return preferredCharset.orElse(defaultCharset);
    }

    private static class WeightedHeaderValue implements Comparable<WeightedHeaderValue> {

        final String value;
        final double weight;

        WeightedHeaderValue(String value) {
            // Accept-Charset = 1#( ( charset / "*" ) [ weight ] )
            // weight = OWS ";" OWS "q=" qvalue
            String[] splits = StringUtils.split(value, ";");
            this.value = splits[0].trim();
            if (splits.length == 1) {
                weight = 1.0d;
            } else {
                OptionalDouble specifiedWeight = Stream.of(splits).skip(1).map(String::trim).map(String::toLowerCase)
                        .filter(a -> a.startsWith("q="))
                        .mapToDouble(segment -> Double.parseDouble(StringUtils.splitByWholeSeparator(segment, "q=")[0]))
                        .findFirst();
                this.weight = specifiedWeight.orElse(1.0d);
            }
        }

        public String getValue() {
            return value;
        }

        public String getValueDefaultStar() {
            return "*".equals(value) ? DEFAULT_RESPONSE_CHARSET.name() : value;
        }

        public double getWeight() {
            return weight;
        }

        @Override
        public int compareTo(WeightedHeaderValue o) {
            return Double.compare(o.weight, weight);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            WeightedHeaderValue that = (WeightedHeaderValue) o;
            return Double.compare(that.weight, weight) == 0 && Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(value, weight);
        }
    }
}
