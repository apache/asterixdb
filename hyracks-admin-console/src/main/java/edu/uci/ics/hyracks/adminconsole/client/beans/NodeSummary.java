package edu.uci.ics.hyracks.adminconsole.client.beans;

import com.google.gwt.core.client.JavaScriptObject;
import com.google.gwt.core.client.JsArray;
import com.google.gwt.view.client.ProvidesKey;

public final class NodeSummary extends JavaScriptObject {
    public static final ProvidesKey<NodeSummary> KEY_PROVIDER = new ProvidesKey<NodeSummary>() {
        @Override
        public Object getKey(NodeSummary item) {
            return item.getNodeId();
        }
    };

    protected NodeSummary() {
    }

    public native String getNodeId()
    /*-{
         return this["node-id"];
    }-*/;

    public native Double getSystemLoadAverage()
    /*-{
         return this["system-load-average"];
    }-*/;

    public native Long getHeapUsed()
    /*-{
         return this["heap-used"];
    }-*/;

    public static native JsArray<NodeSummary> parseNodeSummariesResult(String json)
    /*-{
        return eval(json)[0].result;
     }-*/;
}