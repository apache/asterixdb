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

public final class JobActivityGraphDetails extends JavaScriptObject {
    protected JobActivityGraphDetails() {
    }

    public native JsArray<ActivityDetails> getActivities()
    /*-{
         return this["activities"];
    }-*/;

    public static final class ActivityDetails extends JavaScriptObject {
        protected ActivityDetails() {
        }

        public native String getId()
        /*-{
            return this["id"];
        }-*/;

        public native String getJavaClass()
        /*-{
            return this["java-class"];
        }-*/;

        public native String getOperatorId()
        /*-{
            return this["operator-id"];
        }-*/;

        public native JsArray<ActivityInputDetails> getInputs()
        /*-{
             return this["inputs"];
        }-*/;

        public native JsArray<ActivityOutputDetails> getOutputs()
        /*-{
             return this["outputs"];
        }-*/;
    }

    public static final class ActivityInputDetails extends JavaScriptObject {
        protected ActivityInputDetails() {
        }

        public native Integer getInputPort()
        /*-{
            return this["input-port"];
        }-*/;

        public native String getConnectorId()
        /*-{
            return this["connector-id"];
        }-*/;
    }

    public static final class ActivityOutputDetails extends JavaScriptObject {
        protected ActivityOutputDetails() {
        }

        public native Integer getOutputPort()
        /*-{
            return this["output-port"];
        }-*/;

        public native String getConnectorId()
        /*-{
            return this["connector-id"];
        }-*/;
    }
}