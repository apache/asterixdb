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
package org.apache.asterix.spidersilk;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.asterix.spidersilk.api.INl2SqlTranslator;
import org.apache.asterix.spidersilk.api.Nl2SqlException;
import org.apache.asterix.spidersilk.api.SchemaContext;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the NL2SQL++ module skeleton.
 *
 * These tests verify the core API contracts without requiring a running AsterixDB
 * instance or a live LLM service. Full integration tests will be added in Phase 2
 * when LangChain4j translation is implemented.
 */
public class NL2SqlServletTest {

    @Test
    public void testSchemaContextToPromptString() {
        SchemaContext ctx =
                new SchemaContext("TinySocial", Arrays.asList("Dataset TweetMessages (tweetid: bigint, text: string)",
                        "Dataset FacebookUsers (id: bigint, name: string)"));

        String prompt = ctx.toPromptString();

        Assert.assertTrue("Prompt should contain dataverse name", prompt.contains("TinySocial"));
        Assert.assertTrue("Prompt should contain TweetMessages dataset", prompt.contains("TweetMessages"));
        Assert.assertTrue("Prompt should contain FacebookUsers dataset", prompt.contains("FacebookUsers"));
    }

    @Test
    public void testSchemaContextImmutable() {
        List<String> descriptions = new ArrayList<>();
        descriptions.add("Dataset Foo (id: bigint)");
        SchemaContext ctx = new SchemaContext("TestDV", descriptions);

        // Modifying the original list should not affect the SchemaContext
        descriptions.add("Dataset Bar (id: bigint)");

        Assert.assertEquals("SchemaContext should hold an immutable copy of the descriptions", 1,
                ctx.getDatasetDescriptions().size());
    }

    @Test
    public void testNl2SqlExceptionMessage() {
        Nl2SqlException ex = new Nl2SqlException("LLM service unavailable");
        Assert.assertEquals("LLM service unavailable", ex.getMessage());
    }

    @Test
    public void testNl2SqlExceptionWithCause() {
        RuntimeException cause = new RuntimeException("connection refused");
        Nl2SqlException ex = new Nl2SqlException("Translation failed", cause);

        Assert.assertEquals("Translation failed", ex.getMessage());
        Assert.assertSame(cause, ex.getCause());
    }

    /**
     * Verifies that a mock implementation of INl2SqlTranslator correctly
     * returns a SQL++ string. This ensures the interface contract is stable.
     */
    @Test
    public void testTranslatorInterfaceContract() throws Nl2SqlException {
        INl2SqlTranslator mockTranslator =
                (nl, schema) -> "SELECT VALUE t FROM TweetMessages t WHERE t.text LIKE '%" + nl + "%'";

        SchemaContext ctx =
                new SchemaContext("TinySocial", Arrays.asList("Dataset TweetMessages (tweetid: bigint, text: string)"));

        String result = mockTranslator.translate("AsterixDB", ctx);

        Assert.assertNotNull("Translator must return a non-null SQL++ string", result);
        Assert.assertTrue("Result should reference the dataset", result.contains("TweetMessages"));
        Assert.assertTrue("Result should be a SELECT statement", result.startsWith("SELECT"));
    }
}
