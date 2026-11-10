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
package org.apache.asterix.external.library;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.junit.Test;

public class HttpFunctionUrlTemplateTest {

    private static final List<String> PARAMETERS = List.of("a", "b");
    private static final String TEMPLATE = "https://api.example.com:8443/v1/{a}/items/{a}.json";

    @Test
    public void fillsEachReferenceAsOneEncodedSegment() throws HyracksDataException {
        assertEquals("https://api.example.com:8443/v1/x%20y%3F%23%25%2B%C3%A9/items/x%20y%3F%23%25%2B%C3%A9.json",
                fill(TEMPLATE, "x y?#%+é"));
    }

    @Test
    public void leavesUnreservedCharactersAsTheyAre() throws HyracksDataException {
        assertEquals("https://h/v1/aZ09-._~", fill("https://h/v1/{a}", "aZ09-._~"));
    }

    @Test
    public void encodesAnAlreadyEncodedSegmentAgain() throws HyracksDataException {
        assertEquals("https://h/v1/a%2520b%252Fc", fill("https://h/v1/{a}", "a%20b%2Fc"));
    }

    @Test
    public void rejectsValuesThatChangeThePath() {
        for (String value : new String[] { "", ".", "..", "/", "\\", "../x", "x/y", "x\\y" }) {
            try {
                fill("https://h/{a}", value);
                fail("expected '" + value + "' to be rejected");
            } catch (HyracksDataException e) {
                assertEquals(ErrorCode.EXTERNAL_HTTP_FUNCTION_INVALID_ARGUMENT.intValue(), e.getErrorCode());
            }
        }
    }

    @Test
    public void rejectsNullOrMissing() {
        try {
            fill("https://h/items/{a}", null);
            fail("expected a NULL or MISSING argument to be rejected");
        } catch (HyracksDataException e) {
            assertEquals(ErrorCode.EXTERNAL_HTTP_FUNCTION_INVALID_ARGUMENT.intValue(), e.getErrorCode());
            assertTrue(e.getMessage(), e.getMessage().contains("must not be NULL or MISSING"));
        }
    }

    @Test
    public void tracksReferencedParameters() throws HyracksDataException {
        HttpFunctionUrlTemplate template = new HttpFunctionUrlTemplate(TEMPLATE, PARAMETERS, null);
        assertTrue(template.references(0));
        assertFalse(template.references(1));
    }

    @Test
    public void displaysOnlySchemeAuthorityAndPath() throws HyracksDataException {
        assertEquals("https://api.example.com:8443/v1/{a}/items/{a}.json", displayUrl(TEMPLATE));
        assertEquals("https://h/p/{a}", displayUrl("https://user:secret@h/p/{a}?key=value#fragment"));
        assertEquals("http://h", displayUrl("http://user@h"));
    }

    @Test
    public void rejectsUnknownParameter() {
        try {
            new HttpFunctionUrlTemplate("https://h/{c}", PARAMETERS, null);
            fail("expected an error for an unknown parameter");
        } catch (HyracksDataException e) {
            assertEquals(ErrorCode.EXTERNAL_HTTP_FUNCTION_INVALID_URL.intValue(), e.getErrorCode());
        }
    }

    private static String fill(String template, String value) throws HyracksDataException {
        StringBuilder url = new StringBuilder();
        new HttpFunctionUrlTemplate(template, PARAMETERS, null).fill(url, new String[] { value, null });
        return url.toString();
    }

    private static String displayUrl(String template) throws HyracksDataException {
        return new HttpFunctionUrlTemplate(template, PARAMETERS, null).getDisplayUrl();
    }
}
