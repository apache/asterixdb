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
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.BaseRequest;
import org.apache.hyracks.http.server.FormUrlEncodedRequest;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.util.internal.PlatformDependent;

public class HttpUtil {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final Pattern PARENT_DIR = Pattern.compile("/[^./]+/\\.\\./");
    private static long maxMemUsage = 0L;

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
        return ContentType.APPLICATION_X_WWW_FORM_URLENCODED.equals(getContentTypeOnly(request))
                ? FormUrlEncodedRequest.create(request) : BaseRequest.create(request);
    }

    public static String getContentTypeOnly(IServletRequest request) {
        return getContentTypeOnly(request.getHttpRequest());
    }

    public static String getContentTypeOnly(HttpRequest request) {
        String contentType = request.headers().get(HttpHeaderNames.CONTENT_TYPE);
        return contentType == null ? null : contentType.split(";")[0];
    }

    public static String getRequestBody(IServletRequest request) {
        return request.getHttpRequest().content().toString(StandardCharsets.UTF_8);
    }

    public static void setContentType(IServletResponse response, String type, String charset) throws IOException {
        response.setHeader(HttpHeaderNames.CONTENT_TYPE, type + "; charset=" + charset);
    }

    public static void setContentType(IServletResponse response, String type) throws IOException {
        response.setHeader(HttpHeaderNames.CONTENT_TYPE, type);
    }

    public static Map<String, String> getRequestHeaders(IServletRequest request) {
        Map<String, String> headers = new HashMap<>();
        request.getHttpRequest().headers().forEach(entry -> {
            headers.put(entry.getKey(), entry.getValue());
        });
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

    @SuppressWarnings("restriction")
    public static synchronized void printMemUsage() {
        StringBuilder report = new StringBuilder();
        report.append("sun.misc.VM.maxDirectMemory: ");
        report.append(sun.misc.VM.maxDirectMemory());
        report.append('\n');
        report.append("sun.misc.SharedSecrets.getJavaNioAccess().getDirectBufferPool().getMemoryUsed(): ");
        report.append(sun.misc.SharedSecrets.getJavaNioAccess().getDirectBufferPool().getMemoryUsed());
        report.append('\n');
        report.append("sun.misc.SharedSecrets.getJavaNioAccess().getDirectBufferPool().getTotalCapacity(): ");
        report.append(sun.misc.SharedSecrets.getJavaNioAccess().getDirectBufferPool().getTotalCapacity());
        report.append('\n');
        report.append("ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage(): ");
        report.append(ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage());
        report.append('\n');
        report.append("---------------------------- Beans ----------------------------");
        report.append('\n');
        List<MemoryPoolMXBean> memPoolBeans = ManagementFactory.getMemoryPoolMXBeans();
        for (MemoryPoolMXBean bean : memPoolBeans) {
            if (bean.isValid() && bean.getType() == MemoryType.NON_HEAP) {
                report.append(bean.getName());
                report.append(": ");
                report.append(bean.getUsage());
                report.append('\n');
            }
        }
        report.append("---------------------------- Netty ----------------------------");
        report.append('\n');
        try {
            Field field = PlatformDependent.class.getDeclaredField("DIRECT_MEMORY_COUNTER");
            field.setAccessible(true);
            AtomicLong usedDirectMemory = (AtomicLong) field.get(null);
            long used = usedDirectMemory.get();
            report.append("Current PlatformDependent.DIRECT_MEMORY_COUNTER: ");
            report.append(used);
            report.append('\n');
            report.append("Maximum PlatformDependent.DIRECT_MEMORY_COUNTER: ");
            maxMemUsage = Math.max(maxMemUsage, used);
            report.append(maxMemUsage);
            report.append('\n');
            report.append('\n');
        } catch (Throwable th) { // NOSONAR
            LOGGER.log(Level.WARN, "Failed to access PlatformDependent.DIRECT_MEMORY_COUNTER", th);
            return;
        }
        report.append("--------------- PooledByteBufAllocator.DEFAULT ----------------");
        report.append(PooledByteBufAllocator.DEFAULT.dumpStats());
        LOGGER.log(Level.INFO, report.toString());
    }
}
