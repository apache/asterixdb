package edu.uci.ics.hyracks.adminconsole.client.rest;

import com.google.gwt.core.client.JsArray;

import edu.uci.ics.hyracks.adminconsole.client.beans.NodeSummary;

public class GetNodeSummariesFunction extends AbstractRestFunction<JsArray<NodeSummary>> {
    public static final GetNodeSummariesFunction INSTANCE = new GetNodeSummariesFunction();

    private GetNodeSummariesFunction() {
    }

    @Override
    protected void appendURLPath(StringBuilder buffer) {
        buffer.append("/nodes");
    }
}