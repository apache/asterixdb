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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.Map;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.http.Header;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.junit.Test;

public class ExternalScalarHttpFunctionEvaluatorTest {

    private static final String SECRET = "sk-do-not-show";

    @Test
    public void endpointHeaderWinsWhateverTheCase() throws HyracksDataException {
        Header[] headers = ExternalScalarHttpFunctionEvaluator.mergeHeaders(Map.of("Authorization", "Bearer a"),
                Map.of(), "{\"authorization\": \"Bearer b\", \"X-Trace\": \"1\"}", null);
        assertEquals(2, headers.length);
        assertEquals("Bearer a", headers[0].getValue());
        assertEquals("1", headers[1].getValue());
    }

    @Test
    public void requestHeaderWinsOverFunctionHeader() throws HyracksDataException {
        Header[] headers = ExternalScalarHttpFunctionEvaluator.mergeHeaders(Map.of(), Map.of("X-Trace", "query"),
                "{\"x-trace\": \"function\"}", null);
        assertEquals(1, headers.length);
        assertEquals("query", headers[0].getValue());
    }

    @Test
    public void unreadableHeadersAreAnInvalidOptionThatShowsNoValue() {
        try {
            ExternalScalarHttpFunctionEvaluator.mergeHeaders(Map.of(), Map.of(),
                    "{\"x-api-key\": [\"" + SECRET + "\"]}", null);
            fail("expected the headers to be refused");
        } catch (HyracksDataException e) {
            assertEquals(ErrorCode.EXTERNAL_HTTP_FUNCTION_INVALID_OPTION.intValue(), e.getErrorCode());
            assertFalse(e.getMessage(), e.getMessage().contains(SECRET));
            assertNull(e.getCause());
        }
    }
}
