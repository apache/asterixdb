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
package org.apache.hyracks.tests.integration;

import static org.apache.hyracks.tests.integration.TestUtil.httpGetAsObject;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class NodesAPIIntegrationTest extends AbstractIntegrationTest {

    static final String[] NODE_SUMMARY_FIELDS = { "node-id", "heap-used", "system-load-average", "details" };

    static final String[] NODE_DETAILS_FIELDS = { "node-id", "os-name", "arch", "os-version", "num-processors",
            "vm-name", "vm-version", "vm-vendor", "classpath", "library-path", "boot-classpath", "input-arguments",
            "system-properties", "pid", "date", "rrd-ptr", "heartbeat-times", "heap-init-sizes", "heap-used-sizes",
            "heap-committed-sizes", "heap-max-sizes", "nonheap-init-sizes", "nonheap-used-sizes",
            "nonheap-committed-sizes", "nonheap-max-sizes", "application-memory-budget", "application-cpu-core-budget",
            "thread-counts", "peak-thread-counts", "system-load-averages", "gc-names", "gc-collection-counts",
            "gc-collection-times", "net-payload-bytes-read", "net-payload-bytes-written", "net-signaling-bytes-read",
            "net-signaling-bytes-written", "result-net-payload-bytes-read", "result-net-payload-bytes-written",
            "result-net-signaling-bytes-read", "result-net-signaling-bytes-written", "ipc-messages-sent",
            "ipc-message-bytes-sent", "ipc-messages-received", "ipc-message-bytes-received", "disk-reads",
            "disk-writes", "config" };

    public static final String ROOT_PATH = "/rest/nodes";

    @Test
    public void testNodeSummaries() throws Exception {
        ArrayNode nodes = getNodeSummaries();
        final int size = nodes.size();
        Assert.assertEquals(2, size);
        for (int i = 0; i < size; ++i) {
            checkNodeFields((ObjectNode) nodes.get(i), NODE_SUMMARY_FIELDS);
        }
    }

    protected ArrayNode getNodeSummaries() throws URISyntaxException, IOException {
        ObjectNode res = httpGetAsObject(ROOT_PATH);
        Assert.assertTrue(res.has("result"));
        return (ArrayNode) res.get("result");
    }

    @Test
    public void testNodeDetails() throws Exception {
        ArrayNode nodes = getNodeSummaries();
        for (JsonNode n : nodes) {
            ObjectNode o = (ObjectNode) n;
            URI uri = new URI(o.get("details").asText());
            ObjectNode res = httpGetAsObject(uri);
            checkNodeFields((ObjectNode) res.get("result"), NODE_DETAILS_FIELDS);
        }
    }

    @Test
    public void testStatedump() throws Exception {
        List<String> nodeIds = getNodeIds();
        ObjectNode res = httpGetAsObject("/rest/statedump");
        for (String nodeId : nodeIds) {
            Assert.assertTrue(res.has(nodeId));
            File dumpFile = new File(res.get(nodeId).asText());
            /* TODO(tillw) currently doesn't work in the integration tests ...
            Assert.assertTrue("File '" + dumpFile + "' does not exist", dumpFile.exists());
            Assert.assertTrue("File '" + dumpFile + "' is empty", dumpFile.length() > 0);
            Assert.assertTrue("File '" + dumpFile + "' could not be deleted", dumpFile.delete());
            */
        }
    }

    private void checkNodeFields(ObjectNode node, String[] fieldNames) {
        for (String field : fieldNames) {
            Assert.assertTrue("field '" + field + "' not found", node.has(field));
        }
    }

    private List<String> getNodeIds() throws IOException, URISyntaxException {
        ArrayNode nodes = getNodeSummaries();
        final int size = nodes.size();
        List<String> nodeIds = new ArrayList<>();
        for (int i = 0; i < size; ++i) {
            nodeIds.add(nodes.get(i).get("node-id").asText());
        }
        return nodeIds;
    }
}
