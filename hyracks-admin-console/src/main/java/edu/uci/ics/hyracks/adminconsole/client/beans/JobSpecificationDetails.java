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

public final class JobSpecificationDetails extends JavaScriptObject {
    protected JobSpecificationDetails() {
    }

    public native JsArray<OperatorDescriptor> getOperators()
    /*-{
         return this["operators"];
    }-*/;

    public native JsArray<OperatorDescriptor> getConnectors()
    /*-{
         return this["connectors"];
    }-*/;

    public static final class OperatorDescriptor extends JavaScriptObject {
        protected OperatorDescriptor() {
        }

        public native String getId()
        /*-{
            return this["id"];
        }-*/;

        public native String getJavaClass()
        /*-{
            return this["java-class"];
        }-*/;

        public native Integer getInArity()
        /*-{
            return this["in-arity"];
        }-*/;

        public native String getOutArity()
        /*-{
            return this["out-arity"];
        }-*/;
    }

    public static final class ConnectorDescriptor extends JavaScriptObject {
        protected ConnectorDescriptor() {
        }

        public native String getId()
        /*-{
            return this["id"];
        }-*/;

        public native String getJavaClass()
        /*-{
            return this["java-class"];
        }-*/;
    }
}