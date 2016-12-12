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
package org.apache.asterix.lang.sqlpp.parser;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.base.IParser;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.junit.Test;

public class ParserTest {

    protected void testLineEnding(String query) throws Exception {
        IParserFactory factory = new SqlppParserFactory();
        IParser parser = factory.createParser(query);
        try {
            parser.parse();
        } catch (AsterixException e) {
            if (!e.getMessage().contains("Syntax error: In line 3")) {
                throw new Exception("Unexpected error", e);
            }
        }
    }

    @Test
    public void testCR() throws Exception {
        testLineEnding("select\rvalue\r1");
    }

    @Test
    public void testCRLF() throws Exception {
        testLineEnding("select\r\nvalue\r\n1");
    }

    @Test
    public void testLF() throws Exception {
        testLineEnding("select\nvalue\n1");
    }
}
