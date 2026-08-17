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

import java.util.List;

import org.apache.asterix.common.metadata.NamespaceResolver;
import org.apache.asterix.lang.common.base.IParser;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.junit.Assert;
import org.junit.Test;

/**
 * The text of each statement of a request, which is what lets a deployment record a statement on its own rather than
 * recording the whole request against every one of them.
 */
public class StatementTextTest {

    private static List<String> textsOf(String request) throws Exception {
        IParserFactory factory = new SqlppParserFactory(new NamespaceResolver(false));
        IParser parser = factory.createParser(request);
        parser.parse();
        return parser.getStatementTexts();
    }

    @Test
    public void eachStatementKeepsItsOwnText() throws Exception {
        Assert.assertEquals(List.of("select 1", "select 2"), textsOf("select 1; select 2;"));
    }

    /** The semicolon ends a statement rather than belonging to it, and the text of one statement stops there. */
    @Test
    public void theSemicolonIsNotPartOfTheText() throws Exception {
        Assert.assertEquals(List.of("select 1"), textsOf("select 1;"));
        Assert.assertEquals(List.of("select 1", "select 2"), textsOf("select 1;;;select 2;"));
    }

    /** A statement is taken from its first token to its last, so what surrounds it is not part of it. */
    @Test
    public void whatSurroundsAStatementIsNotPartOfIt() throws Exception {
        Assert.assertEquals(List.of("select 1", "select 2"),
                textsOf("  /* a comment */ select 1;  -- trailing\n  select 2;  \n"));
    }

    @Test
    public void aStatementSpanningSeveralLinesKeepsThem() throws Exception {
        List<String> texts = textsOf("select\n  1;\nselect value 2\n  from range(1, 2) r;");
        Assert.assertEquals(2, texts.size());
        Assert.assertEquals("select\n  1", texts.get(0));
        Assert.assertEquals("select value 2\n  from range(1, 2) r", texts.get(1));
    }

    /** A comment inside a statement is part of it, since it falls between the statement's first and last token. */
    @Test
    public void aCommentInsideAStatementIsPartOfIt() throws Exception {
        Assert.assertEquals(List.of("select /* here */ 1"), textsOf("select /* here */ 1;"));
    }

    @Test
    public void theTextsFollowTheStatementsInOrder() throws Exception {
        Assert.assertEquals(List.of("use dv", "select 1", "upsert into ds ([{\"id\": 1}])"),
                textsOf("use dv; select 1; upsert into ds ([{\"id\": 1}]);"));
    }
}
