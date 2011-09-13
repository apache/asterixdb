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