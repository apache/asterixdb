/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.control.cc.adminconsole.pages;

import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.link.BookmarkablePageLink;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.markup.html.list.ListView;
import org.apache.wicket.request.mapper.parameter.PageParameters;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.web.util.JSONUtils;
import edu.uci.ics.hyracks.control.cc.work.GetNodeSummariesJSONWork;

public class NodesSummaryPage extends AbstractPage {
    private static final long serialVersionUID = 1L;

    public NodesSummaryPage() throws Exception {
        ClusterControllerService ccs = getAdminConsoleApplication().getClusterControllerService();

        GetNodeSummariesJSONWork gnse = new GetNodeSummariesJSONWork(ccs);
        ccs.getWorkQueue().scheduleAndSync(gnse);
        JSONArray summaries = gnse.getSummaries();
        add(new Label("node-count", String.valueOf(summaries.length())));
        ListView<JSONObject> nodeList = new ListView<JSONObject>("node-list", JSONUtils.toList(summaries)) {
            private static final long serialVersionUID = 1L;

            @Override
            protected void populateItem(ListItem<JSONObject> item) {
                JSONObject o = item.getModelObject();
                try {
                    item.add(new Label("node-id", o.getString("node-id")));
                    item.add(new Label("heap-used", o.getString("heap-used")));
                    item.add(new Label("system-load-average", o.getString("system-load-average")));
                    PageParameters params = new PageParameters();
                    params.add("node-id", o.getString("node-id"));
                    item.add(new BookmarkablePageLink("node-details", NodeDetailsPage.class, params));
                } catch (JSONException e) {
                    throw new RuntimeException(e);
                }
            }
        };
        add(nodeList);
    }
}