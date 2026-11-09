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

import java.util.Collections;
import java.util.List;

/**
 * Encapsulates the database schema information extracted from AsterixDB metadata.
 * This context is injected into the LLM prompt to enable schema-aware SQL++ generation.
 *
 * The schema is extracted from the {@code asterix-metadata} module via MetadataManager,
 * including Dataset definitions, type information, and index metadata.
 */
public class SchemaContext {

    private final String dataverse;
    private final List<String> datasetDescriptions;

    public SchemaContext(String dataverse, List<String> datasetDescriptions) {
        this.dataverse = dataverse;
        this.datasetDescriptions = Collections.unmodifiableList(new java.util.ArrayList<>(datasetDescriptions));
    }

    /**
     * @return the target dataverse name
     */
    public String getDataverse() {
        return dataverse;
    }

    /**
     * @return human-readable schema descriptions for each dataset in the dataverse,
     *         formatted for inclusion in an LLM prompt
     */
    public List<String> getDatasetDescriptions() {
        return datasetDescriptions;
    }

    /**
     * Renders the schema context as a prompt-ready string.
     * Example output:
     * <pre>
     * Dataverse: TinySocial
     * Dataset TweetMessages (tweetid: bigint, sender-location: point, text: string, ...)
     * Dataset FacebookUsers (id: bigint, name: string, employment: [object], ...)
     * </pre>
     */
    public String toPromptString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Dataverse: ").append(dataverse).append('\n');
        for (String desc : datasetDescriptions) {
            sb.append(desc).append('\n');
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return "SchemaContext{dataverse='" + dataverse + "', datasets=" + datasetDescriptions.size() + "}";
    }
}
