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
package org.apache.hyracks.test.http;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.server.utils.HttpUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import io.netty.handler.codec.http.HttpHeaderNames;

public class HttpAcceptCharsetTest {
    private static final Logger LOGGER = LogManager.getLogger();

    @Test
    public void testBogusAcceptCharset() {
        IServletRequest request = withCharset("lol");
        assertCharsetsEqual("default on bogus", StandardCharsets.UTF_8, request);
    }

    @Test
    public void testWeightedStarDefault() {
        IServletRequest request = withCharset("*;q=0.5,bogus;q=1.0");
        assertCharsetsEqual("defer on unsupported", StandardCharsets.UTF_8, request);
    }

    @Test
    public void testWeightedStar() {
        IServletRequest request = withCharset("*;q=0.5,utf-32;q=1.0");
        assertCharsetsEqual("defer on bogus", Charset.forName("utf-32"), request);
    }

    @Test
    public void testUnweightedIs1_0() {
        IServletRequest request = withCharset("utf-8;q=0.5,utf-16");
        assertCharsetsEqual("defer on bogus", Charset.forName("utf-16"), request);
    }

    @Test
    public void testAmbiguous() {
        IServletRequest request = withCharset("utf-8;q=.75,utf-16;q=.75,utf-32;q=.5");
        String preferredCharset = HttpUtil.getPreferredCharset(request).name();
        Assert.assertTrue("ambiguous by weight (got: " + preferredCharset + ")",
                preferredCharset.toLowerCase().matches("utf-(8|16)"));
    }

    private IServletRequest withCharset(String headerValue) {
        IServletRequest request = Mockito.mock(IServletRequest.class);
        Mockito.when(request.getHeader(HttpHeaderNames.ACCEPT_CHARSET)).thenReturn(headerValue);
        LOGGER.info("using {} of '{}'", HttpHeaderNames.ACCEPT_CHARSET, headerValue);
        return request;
    }

    private void assertCharsetsEqual(String message, Object charset, Object charset2) {
        Assert.assertEquals(message, normalize(charset), normalize(charset2));
    }

    private String normalize(Object charsetObject) {
        if (charsetObject instanceof Charset) {
            return ((Charset) charsetObject).name().toLowerCase();
        } else if (charsetObject instanceof String) {
            return ((String) charsetObject).toLowerCase();
        } else if (charsetObject instanceof IServletRequest) {
            return HttpUtil.getPreferredCharset((IServletRequest) charsetObject).name().toLowerCase();
        }
        throw new IllegalArgumentException("unknown type: " + charsetObject.getClass());
    }
}
