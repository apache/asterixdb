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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class IndexAdvisor {
    private final boolean isAdvise;
    private Object fakeIndexProvider;
    private final List<String> recommendedAdviseString;
    private final List<String> presentAdviseString;
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
        recommendedAdviseString = new ArrayList<>();
        presentAdviseString = new ArrayList<>();
    }

    public boolean getAdvise() {
        return isAdvise;
    }

    public void addRecommendedAdviseString(String advise) {
        recommendedAdviseString.add(advise);
    }

    public void addPresentAdviseString(String advise) {
        presentAdviseString.add(advise);
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
        for (String advise : presentAdviseString) {
            ObjectNode indexNode = JsonNodeFactory.instance.objectNode();
            indexNode.put("index_statement", advise);
            currentIndexNode.add(indexNode);
        }

        ArrayNode recommendedIndexNode = (ArrayNode) resultJson.path(0).path("advice").path("adviseinfo")
                .path("recommended_indexes").path("indexes");

        for (String advise : recommendedAdviseString) {
            ObjectNode indexNode = JsonNodeFactory.instance.objectNode();
            indexNode.put("index_statement", advise);
            recommendedIndexNode.add(indexNode);
        }
        return resultJson; // Placeholder for actual implementation
    }

}
