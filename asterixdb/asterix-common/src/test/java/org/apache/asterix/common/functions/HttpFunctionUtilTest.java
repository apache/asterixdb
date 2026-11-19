/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.asterix.common.functions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;

public class HttpFunctionUtilTest {

    @Test
    public void templateParametersInOrder() {
        assertEquals(List.of("a", "b", "a"), HttpFunctionUtil.getTemplateParameters("/x/{a}/{b}-{a}"));
        assertEquals(List.of(), HttpFunctionUtil.getTemplateParameters("/x/y"));
    }

    @Test
    public void malformedTemplatesAreRejected() {
        for (String template : List.of("/{a", "/{}", "/{a{b}}", "/a}")) {
            assertThrows(template, IllegalArgumentException.class,
                    () -> HttpFunctionUtil.getTemplateParameters(template));
        }
    }

    @Test
    public void validUrlsPassWithTheirParameters() throws CompilationException {
        assertEquals(List.of("id"), HttpFunctionUtil.validateUrl("https://api.example.com/items/{id}", null));
        assertEquals(List.of(), HttpFunctionUtil.validateUrl("http://localhost:8080", null));
        assertEquals(List.of("a", "b"), HttpFunctionUtil.validateUrl("HTTPS://localhost:8080/v1/{a}/x/{b}", null));
    }

    @Test
    public void httpsIsRecognisedInAnyCase() {
        assertTrue(HttpFunctionUtil.isHttps("https://api.example.com/x"));
        assertTrue(HttpFunctionUtil.isHttps("HTTPS://api.example.com/x"));
        assertFalse(HttpFunctionUtil.isHttps("http://api.example.com/x"));
        assertFalse(HttpFunctionUtil.isHttps("http://https.example.com/x"));
    }

    @Test
    public void invalidUrlsAreRejected() {
        for (String url : List.of("ftp://example.com/x", "/relative", "https://user:pw@example.com/x",
                "https://example.com/x?q=1", "https://example.com/x#frag", "https://example.com/{a",
                "https://example.com/{a}/{b", "not a url", "https://{host}/v1", "https://{host}.example.com/v1",
                "https://example.com:{port}/v1", "https://{host}")) {
            assertError(url, ErrorCode.EXTERNAL_HTTP_FUNCTION_INVALID_URL,
                    () -> HttpFunctionUtil.validateUrl(url, null));
        }
    }

    @Test
    public void unparseableHostIsNotReportedAsMissing() {
        assertUrlReason("http://my_service:8080/x", "invalid host in 'my_service:8080'");
        assertUrlReason("http:///x", "missing host");
    }

    @Test
    public void validPathsPassWithTheirParameters() throws CompilationException {
        assertEquals(List.of(), HttpFunctionUtil.validatePath("/", null));
        assertEquals(List.of("id"), HttpFunctionUtil.validatePath("/v1/items/{id}", null));
        assertEquals(List.of(), HttpFunctionUtil.validatePath("/v1/a.b/..c", null));
        assertEquals(List.of(), HttpFunctionUtil.validatePath("/v1/items/", null));
        assertEquals(List.of("id"), HttpFunctionUtil.validatePath("/v1/x-y_z~1/{id}.json", null));
    }

    @Test
    public void escapingPathsAreRejected() {
        for (String path : List.of("relative", "//host/x", "/a//b", "/a/../b", "/a/./b", "/..", "/a\\b", "/a?q", "/a#f",
                "/a/%2e%2e/b", "/a%2Fb", "/a%5cb", "/{a", "/{a}/{b", "/a/..;/admin", "/..;/..;/admin",
                "/a/%252e%252e/admin", "/a/%c0%ae%c0%ae/admin", "/a;x=1/b", "/a b", "/café")) {
            assertError(path, ErrorCode.EXTERNAL_HTTP_FUNCTION_INVALID_OPTION,
                    () -> HttpFunctionUtil.validatePath(path, null));
        }
    }

    @Test
    public void tokenHeaderNamesPass() {
        for (String name : List.of("X-Request-Id", "x_trace.id", "X-Api-Key!#$%&'*+^`|~")) {
            assertTrue(name, HttpFunctionUtil.isValidHeaderName(name));
        }
    }

    @Test
    public void nonTokenHeaderNamesAreRejected() {
        for (String name : List.of("", "X Id", "X:Id", "(x)", "a/b", "user@id", "{id}", "a=b", "a\"b", "X-Ten\u00e4nt",
                "x\u007f")) {
            assertFalse(name, HttpFunctionUtil.isValidHeaderName(name));
        }
    }

    @Test
    public void headerValuesWithoutControlCharactersPass() {
        for (String value : List.of("a b", "a\tb", "en-US, fr;q=0.8", "")) {
            assertTrue(value, HttpFunctionUtil.isValidHeaderValue(value));
        }
    }

    @Test
    public void headerValuesWithControlCharactersAreRejected() {
        for (String value : List.of("a\r\nX-Injected: 1", "a\rb", "a\nb", "acme\u0000evil", "x\u007f")) {
            assertFalse(value, HttpFunctionUtil.isValidHeaderValue(value));
        }
    }

    private static void assertError(String input, ErrorCode expected, ThrowingRunnable check) {
        CompilationException e = assertThrows(input, CompilationException.class, check);
        assertEquals(input, expected.intValue(), e.getErrorCode());
    }

    private static void assertUrlReason(String url, String reason) {
        CompilationException e =
                assertThrows(url, CompilationException.class, () -> HttpFunctionUtil.validateUrl(url, null));
        assertEquals(url, ErrorCode.EXTERNAL_HTTP_FUNCTION_INVALID_URL.intValue(), e.getErrorCode());
        assertTrue(e.getMessage(), e.getMessage().endsWith(reason));
    }
}
