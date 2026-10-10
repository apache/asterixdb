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
package org.apache.asterix.spidersilk.api;

/**
 * Core interface for natural language to SQL++ translation.
 *
 * Implementations are model-agnostic: any LLM backend (OpenAI, Ollama, etc.)
 * can be used by providing a different implementation. The LangChain4j framework
 * is used internally to manage LLM communication, prompt templating, and retries.
 *
 * <p>Usage example:
 * <pre>
 *   INl2SqlTranslator translator = new LangChain4jTranslator(config);
 *   SchemaContext schema = schemaBuilder.buildContext("TinySocial");
 *   String sqlpp = translator.translate("Find all tweets mentioning AsterixDB", schema);
 *   // sqlpp => "SELECT VALUE t FROM TweetMessages t WHERE t.message_text LIKE '%AsterixDB%'"
 * </pre>
 */
public interface INl2SqlTranslator {

    /**
     * Translates a natural language query into an executable SQL++ statement.
     *
     * The implementation should:
     * <ol>
     *   <li>Build a schema-aware prompt from {@code schemaContext}</li>
     *   <li>Call the configured LLM to generate a SQL++ candidate</li>
     *   <li>Validate the candidate using the AsterixDB SQL++ parser</li>
     *   <li>Retry with error feedback if validation fails (up to a configured max)</li>
     * </ol>
     *
     * @param naturalLanguage the user's natural language query (non-null, non-empty)
     * @param schemaContext   schema information for the target dataverse; may be
     *                        {@code null} if no dataverse is specified
     * @return a syntactically valid SQL++ query string
     * @throws Nl2SqlException if translation fails after exhausting retries,
     *                         or if the LLM service is unavailable
     */
    String translate(String naturalLanguage, SchemaContext schemaContext) throws Nl2SqlException;
}
