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
import com.google.gwt.core.client.JsArrayInteger;
import com.google.gwt.core.client.JsArrayNumber;

public final class NodeDetails extends JavaScriptObject {
    protected NodeDetails() {

    }

    public native String getNodeId()
    /*-{
         return this["node-id"];
    }-*/;

    public native String getOSName()
    /*-{
         return this["os-name"];
    }-*/;

    public native String getArch()
    /*-{
         return this["arch"];
    }-*/;

    public native String getOSVersion()
    /*-{
         return this["os-version"];
    }-*/;

    public native int getNProcessors()
    /*-{
         return this["num-processors"];
    }-*/;

    public native int getRRDPtr()
    /*-{
         return this["rrd-ptr"];
    }-*/;

    public native JsArrayNumber getHeartbeatTimes()
    /*-{
         return this["heartbeat-times"];
    }-*/;

    public native JsArrayNumber getHeapInitSizes()
    /*-{
         return this["heap-init-sizes"];
    }-*/;

    public native JsArrayNumber getHeapUsedSizes()
    /*-{
         return this["heap-used-sizes"];
    }-*/;

    public native JsArrayNumber getHeapCommittedSizes()
    /*-{
         return this["heap-committed-sizes"];
    }-*/;

    public native JsArrayNumber getHeapMaxSizes()
    /*-{
         return this["heap-max-sizes"];
    }-*/;

    public native JsArrayNumber getNonHeapInitSizes()
    /*-{
         return this["nonheap-init-sizes"];
    }-*/;

    public native JsArrayNumber getNonHeapUsedSizes()
    /*-{
         return this["nonheap-used-sizes"];
    }-*/;

    public native JsArrayNumber getNonHeapCommittedSizes()
    /*-{
         return this["nonheap-committed-sizes"];
    }-*/;

    public native JsArrayNumber getNonHeapMaxSizes()
    /*-{
         return this["nonheap-max-sizes"];
    }-*/;

    public native JsArrayInteger getThreadCounts()
    /*-{
         return this["thread-counts"];
    }-*/;

    public native JsArrayInteger getPeakThreadCounts()
    /*-{
         return this["peak-thread-counts"];
    }-*/;

    public native JsArrayNumber getSystemLoadAverages()
    /*-{
         return this["system-load-averages"];
    }-*/;
}