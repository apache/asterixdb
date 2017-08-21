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

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.base.IParser;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.junit.Test;

public class ParserTest {

    protected void assertParseError(String query, int expectedErrorLine) throws Exception {
        IParserFactory factory = new SqlppParserFactory();
        IParser parser = factory.createParser(query);
        try {
            parser.parse();
            throw new Exception("Expected error at line " + expectedErrorLine + " was not thrown");
        } catch (CompilationException e) {
            if (!e.getMessage().contains("Syntax error: In line " + expectedErrorLine)) {
                throw new Exception("Unexpected error", e);
            }
        }
    }

    protected void testLineEnding(String query, int expectedErrorLine) throws Exception {
        assertParseError(query, expectedErrorLine);
    }

    @Test
    public void testCR() throws Exception {
        testLineEnding("select\rvalue\r1", 3);
    }

    @Test
    public void testCRLF() throws Exception {
        testLineEnding("select\r\nvalue\r\n1", 3);
    }

    @Test
    public void testLF() throws Exception {
        testLineEnding("select\nvalue\n1", 3);
    }

    @Test
    public void testMultipleStatements() throws Exception {
        String query = "use x;\nselect 1;";
        IParserFactory factory = new SqlppParserFactory();
        IParser parser = factory.createParser(query);
        parser.parse();
        query = "use x;\n use x;;;;\n use x;\n select 1;";
        parser = factory.createParser(query);
        parser.parse();
    }

    @Test
    public void testUnseparatedStatements() throws Exception {
        String query = "create dataverse dv select 1";
        assertParseError(query, 1);
        query = "use x\n select 1;";
        assertParseError(query, 2);
        query = "use x;\n use x;\n use x\n select 1;";
        assertParseError(query, 4);
    }

    @Test
    public void testEmptyStatement() throws Exception {
        String query = "";
        IParserFactory factory = new SqlppParserFactory();
        IParser parser = factory.createParser(query);
        parser.parse();

        query = ";";
        parser = factory.createParser(query);
        parser.parse();

        query = ";;;";
        parser = factory.createParser(query);
        parser.parse();

        query = "\n\n";
        parser = factory.createParser(query);
        parser.parse();

        query = "; select 1;";
        parser = factory.createParser(query);
        parser.parse();

        query = ";;;;; select 1;;;";
        parser = factory.createParser(query);
        parser.parse();
    }
}
