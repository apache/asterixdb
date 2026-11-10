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
package org.apache.asterix.utils;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class RedactionUtilTest {

    private static final String FUNCTION = "CREATE EXTERNAL FUNCTION f(a) AT 'https://api.example.com/x' WITH { ";

    @Test
    public void headersAreRedacted() {
        assertEquals(FUNCTION + "\"headers...<redacted sensitive data>",
                RedactionUtil.LOG_REDACTOR.statement(FUNCTION + "\"headers\": { \"X-Api-Key\": \"s3cr3t\" } };"));
    }

    @Test
    public void redactionStartsAtFirstSensitiveOption() {
        assertEquals(FUNCTION + "\"bearerToken...<redacted sensitive data>", RedactionUtil.LOG_REDACTOR
                .statement(FUNCTION + "\"bearerToken\": \"t\",\n \"headers\": { \"X-Api-Key\": \"s3cr3t\" } };"));
    }

    @Test
    public void statementWithoutSensitiveOptionIsUnchanged() {
        String statement = FUNCTION + "\"method\": \"post\" };";
        assertEquals(statement, RedactionUtil.LOG_REDACTOR.statement(statement));
    }
}
