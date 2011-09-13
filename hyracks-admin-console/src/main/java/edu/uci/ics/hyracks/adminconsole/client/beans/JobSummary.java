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
import com.google.gwt.view.client.ProvidesKey;

public final class JobSummary extends JavaScriptObject {
    public static final ProvidesKey<JobSummary> KEY_PROVIDER = new ProvidesKey<JobSummary>() {
        @Override
        public Object getKey(JobSummary item) {
            return item.getJobId();
        }
    };

    protected JobSummary() {
    }

    public native String getJobId()
    /*-{
         return this["job-id"];
    }-*/;

    public native String getApplicationName()
    /*-{
         return this["application-name"];
    }-*/;

    public native String getStatus()
    /*-{
         return this.status;
    }-*/;

    public native Long getCreateTime()
    /*-{
         return this["create-time"];
    }-*/;

    public native Long getStartTime()
    /*-{
         return this["start-time"];
    }-*/;

    public native Long getEndTime()
    /*-{
         return this["end-time"];
    }-*/;
}