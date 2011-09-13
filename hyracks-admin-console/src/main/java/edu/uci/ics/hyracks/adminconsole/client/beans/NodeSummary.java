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