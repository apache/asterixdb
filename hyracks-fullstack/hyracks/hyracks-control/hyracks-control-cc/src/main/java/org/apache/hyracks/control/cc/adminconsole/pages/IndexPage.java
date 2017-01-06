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
package org.apache.hyracks.control.cc.adminconsole.pages;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.link.BookmarkablePageLink;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.markup.html.list.ListView;
import org.apache.wicket.request.mapper.parameter.PageParameters;

import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.work.GetJobSummariesJSONWork;
import org.apache.hyracks.control.cc.work.GetNodeSummariesJSONWork;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class IndexPage extends AbstractPage {
    private static final long serialVersionUID = 1L;

    public IndexPage() throws Exception {
        ClusterControllerService ccs = getAdminConsoleApplication().getClusterControllerService();

        GetNodeSummariesJSONWork gnse = new GetNodeSummariesJSONWork(ccs);
        ccs.getWorkQueue().scheduleAndSync(gnse);
        ArrayNode nodeSummaries = gnse.getSummaries();
        add(new Label("node-count", String.valueOf(nodeSummaries.size())));
        ListView<JsonNode> nodeList = new ListView<JsonNode>("node-list",
                Lists.newArrayList(nodeSummaries.iterator())) {
            private static final long serialVersionUID = 1L;

            @Override
            protected void populateItem(ListItem<JsonNode> item) {
                JsonNode o = item.getModelObject();
                item.add(new Label("node-id", o.get("node-id").asText()));
                item.add(new Label("heap-used", o.get("heap-used").asText()));
                item.add(new Label("system-load-average", o.get("system-load-average").asText()));
                PageParameters params = new PageParameters();
                params.add("node-id", o.get("node-id").asText());
                item.add(new BookmarkablePageLink<Object>("node-details", NodeDetailsPage.class, params));
            }
        };
        add(nodeList);

        GetJobSummariesJSONWork gjse = new GetJobSummariesJSONWork(ccs);
        ccs.getWorkQueue().scheduleAndSync(gjse);
        ArrayNode jobSummaries = gjse.getSummaries();
        ListView<JsonNode> jobList = new ListView<JsonNode>("jobs-list", Lists.newArrayList(jobSummaries.iterator())) {
            private static final long serialVersionUID = 1L;

            @Override
            protected void populateItem(ListItem<JsonNode> item) {
                JsonNode o = item.getModelObject();
                item.add(new Label("job-id", o.get("job-id").asText()));
                item.add(new Label("status", o.get("status").asText()));
                item.add(new Label("create-time", longToDateString(Long.parseLong(o.get("create-time").asText()))));
                item.add(new Label("start-time", longToDateString(Long.parseLong(o.get("start-time").asText()))));
                item.add(new Label("end-time", longToDateString(Long.parseLong(o.get("end-time").asText()))));
                PageParameters params = new PageParameters();
                params.add("job-id", o.get("job-id"));
                item.add(new BookmarkablePageLink<Object>("job-details", JobDetailsPage.class, params));
            }
        };
        add(jobList);
    }

    private String longToDateString(long milliseconds) {
        if (milliseconds == 0) {
            return "n/a";
        }
        SimpleDateFormat sdf = new SimpleDateFormat("MMM dd, yyyy HH:mm:ss");
        Date date = new Date(milliseconds);
        return sdf.format(date);
    }
}
