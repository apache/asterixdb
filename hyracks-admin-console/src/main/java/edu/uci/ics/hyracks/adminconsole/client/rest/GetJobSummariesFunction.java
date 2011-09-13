package edu.uci.ics.hyracks.adminconsole.client.rest;

import com.google.gwt.core.client.JsArray;

import edu.uci.ics.hyracks.adminconsole.client.beans.JobSummary;

public class GetJobSummariesFunction extends AbstractRestFunction<JsArray<JobSummary>> {
    public static final GetJobSummariesFunction INSTANCE = new GetJobSummariesFunction();

    private GetJobSummariesFunction() {
    }

    @Override
    protected void appendURLPath(StringBuilder buffer) {
        buffer.append("/jobs");
    }
}