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
package org.apache.hyracks.algebricks.core.algebra.base;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class IndexAdvisor {
    private final boolean isAdvise;
    private Object fakeIndexProvider;
    private final List<Advise> recommendedAdvise;
    private final List<Advise> presentAdvise;
    private static final JsonNode template;

    static {
        try {
            template = new ObjectMapper().readTree("""
                      [
                                      {
                                          "#operator": "Advise",
                                          "advice": {
                                              "#operator": "IndexAdvice",
                                              "adviseinfo": {
                                                  "current_indexes": [ ],
                                                  "recommended_indexes": {
                                                      "indexes": [ ]
                                                  }
                                              }
                                          }
                                      }
                     ]
                    """);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public IndexAdvisor(boolean isAdvise) {
        this.isAdvise = isAdvise;
        recommendedAdvise = new ArrayList<>();
        presentAdvise = new ArrayList<>();
    }

    public boolean getAdvise() {
        return isAdvise;
    }

    public void addRecommendedAdvise(String indexName, List<List<String>> keyFieldNames, String databaseName,
            String dataverseName, String datasetName) {
        recommendedAdvise.add(new Advise(indexName, keyFieldNames, databaseName, dataverseName, datasetName));
    }

    public void addPresentAdvise(String indexName, List<List<String>> keyFieldNames, String databaseName,
            String dataverseName, String datasetName) {
        presentAdvise.add(new Advise(indexName, keyFieldNames, databaseName, dataverseName, datasetName));
    }

    public void setFakeIndexProvider(Object fakeIndexProvider) {
        this.fakeIndexProvider = fakeIndexProvider;
    }

    public Object getFakeIndexProvider() {
        return fakeIndexProvider;
    }

    public JsonNode getResultJson() {

        JsonNode resultJson = template.deepCopy();
        ArrayNode currentIndexNode =
                (ArrayNode) resultJson.path(0).path("advice").path("adviseinfo").path("current_indexes");
        for (Advise advise : presentAdvise) {
            ObjectNode indexNode = JsonNodeFactory.instance.objectNode();
            indexNode.put("index_statement", advise.getCreateIndexClause());
            currentIndexNode.add(indexNode);
        }

        ArrayNode recommendedIndexNode = (ArrayNode) resultJson.path(0).path("advice").path("adviseinfo")
                .path("recommended_indexes").path("indexes");

        for (Advise advise : recommendedAdvise) {
            ObjectNode indexNode = JsonNodeFactory.instance.objectNode();
            indexNode.put("index_statement", advise.getCreateIndexClause());
            recommendedIndexNode.add(indexNode);
        }
        return resultJson; // Placeholder for actual implementation
    }

    static public class Advise {
        private final String indexName;
        private final List<List<String>> keyFieldNames;
        private final String databaseName;
        private final String dataverseName;
        private final String datasetName;

        public Advise(String indexName, List<List<String>> keyFieldNames, String databaseName, String dataverseName,
                String datasetName) {
            this.indexName = indexName;
            this.keyFieldNames = keyFieldNames;
            this.databaseName = databaseName;
            this.dataverseName = dataverseName;
            this.datasetName = datasetName;
        }

        public String getIndexName() {
            return indexName;
        }

        public String getCreateIndexClause() {
            return "CREATE INDEX " + indexName + " ON `" + databaseName + "`.`" + dataverseName + "`.`" + datasetName
                    + "`" + getKeyFieldNamesClause() + ";";
        }

        public String getKeyFieldNamesClause() {
            return keyFieldNames.stream()
                    .map(fields -> fields.stream().map(s -> "`" + s + "`").collect(Collectors.joining(".")))
                    .collect(Collectors.joining(",", "(", ")"));

        }

    }

}
