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
package org.apache.hyracks.util.trace;

import java.io.IOException;
import java.util.List;

import org.apache.hyracks.util.Log4j2Monitor;
import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TraceTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final String name = "test";

    public JsonNode validate(String line) throws IOException {
        final JsonNode traceRecord = mapper.readTree(line);

        Assert.assertTrue(traceRecord.has("ph"));

        Assert.assertTrue(traceRecord.has("pid"));
        Integer.parseInt(traceRecord.get("pid").asText());

        Assert.assertTrue(traceRecord.has("tid"));
        Long.parseLong(traceRecord.get("tid").asText());

        Assert.assertTrue(traceRecord.has("ts"));
        Long.parseLong(traceRecord.get("ts").asText());

        return traceRecord;
    }

    @Test
    public void testInstant() throws IOException {
        // test with initial categories

        TraceCategoryRegistry registry = new TraceCategoryRegistry();
        long cat1 = registry.get("CAT1");
        long cat2 = registry.get("CAT2");
        long cat3 = registry.get("CAT3");

        ITracer tracer = new Tracer(name, new String[] { "CAT1", "CAT2" }, registry);
        Log4j2Monitor.start();
        tracer.instant("test1", cat1, ITracer.Scope.p, null);
        tracer.instant("test2", cat2, ITracer.Scope.p, null);
        tracer.instant("test3", cat3, ITracer.Scope.p, null);

        List<String> lines = Log4j2Monitor.getLogs();
        for (String line : lines) {
            final JsonNode traceRecord = validate(line);
            Assert.assertEquals("i", traceRecord.get("ph").asText());
            Assert.assertNotEquals("CAT3", traceRecord.get("cat").asText());
        }

        // test with modified categories
        tracer.setCategories("CAT1", "CAT3");
        Log4j2Monitor.reset();

        tracer.instant("test1", cat1, ITracer.Scope.p, null);
        tracer.instant("test2", cat2, ITracer.Scope.p, null);
        tracer.instant("test3", cat3, ITracer.Scope.p, null);

        lines = Log4j2Monitor.getLogs();
        for (String line : lines) {
            final JsonNode traceRecord = validate(line);
            Assert.assertEquals("i", traceRecord.get("ph").asText());
            Assert.assertNotEquals("CAT2", validate(line).get("cat").asText());
        }
    }
}
